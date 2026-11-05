package coordinator

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

func TestProjectionRestorePreservesLeaseAndIdempotency(t *testing.T) {
	view := &projection{directory: t.TempDir()}
	defer view.close()
	at := time.Now().UTC()
	apply := func(op operation) applied {
		t.Helper()
		data, err := json.Marshal(op)
		if err != nil {
			t.Fatal(err)
		}
		return view.Apply(&raft.Log{Data: data}).(applied)
	}
	submit := operation{
		Kind: "submit", At: at, Key: "submit-once", MaxAttempts: 3,
		Submit: api.SubmitRequest{Engine: "echo", Prompt: "keep this job", MaxTokens: 16},
	}
	initial := apply(submit)
	if initial.err != nil || initial.job.ID == "" {
		t.Fatalf("submit: %+v", initial)
	}
	claim := operation{
		Kind: "claim", At: at, Key: "claim-once", Token: "first-token", LeaseTime: 15 * time.Second,
		Claim: api.ClaimRequest{Engine: "echo", WorkerID: "first-worker"},
	}
	started := apply(claim)
	if started.err != nil || started.lease == nil || started.lease.Attempt != 1 {
		t.Fatalf("claim: %+v", started)
	}
	snap, err := view.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	finish := operation{Kind: "finish", At: at.Add(time.Second), ID: initial.job.ID,
		Finish: api.FinishRequest{Token: "first-token", Output: "complete"}}
	if result := apply(finish); result.err != nil {
		t.Fatal(result.err)
	}

	// Restore must be able to replace the view even with an unreleased snapshot.
	source, err := os.Open(snap.(*projectionSnapshot).path)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- view.Restore(source) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		snap.Release()
		t.Fatal("restoring while a snapshot is outstanding blocked")
	}
	claim.Token = "fresh-proposal"
	replay := apply(claim)
	if replay.err != nil || replay.lease == nil || replay.lease.Token != started.lease.Token || replay.lease.Attempt != 1 {
		t.Fatalf("idempotent claim after snapshot: %+v", replay)
	}
	if result := apply(finish); result.err != nil {
		t.Fatal(result.err)
	}
	resubmitted := apply(submit)
	if resubmitted.err != nil || resubmitted.job.ID != initial.job.ID || resubmitted.job.State != api.Succeeded {
		t.Fatalf("idempotent submit after snapshot: %+v", resubmitted)
	}
	submit.Submit.Prompt = "different request"
	if result := apply(submit); !errors.Is(result.err, errKeyConflict) {
		t.Fatalf("expected idempotency conflict, got %+v", result)
	}
	claim.At = at.Add(time.Minute)
	if result := apply(claim); result.err != nil || result.lease != nil {
		t.Fatalf("replaying a completed claim returned a new lease: %+v", result)
	}
}

func TestProjectionMigratesLegacySnapshotAndReplicatesCredentials(t *testing.T) {
	at := time.Now().UTC()
	legacy, jobID := legacySnapshot(t, at, false)
	view := &projection{directory: t.TempDir()}
	defer view.close()
	if err := view.Restore(legacy); err != nil {
		t.Fatal(err)
	}
	apply := func(op operation) applied {
		t.Helper()
		data, err := json.Marshal(op)
		if err != nil {
			t.Fatal(err)
		}
		return view.Apply(&raft.Log{Data: data}).(applied)
	}
	issue := api.WorkerCredentialChange{WorkerID: "pc", Digest: strings.Repeat("a", 64)}
	if result := apply(operation{Kind: "worker_credential", At: at, Key: "issue", Credential: &issue}); result.err != nil {
		t.Fatal(result.err)
	}
	claim := operation{
		Kind: "claim", At: at, Key: "claim", Token: "lease-token", LeaseTime: time.Minute,
		Claim: api.ClaimRequest{WorkerID: "pc", Engine: "echo"}, WorkerDigest: issue.Digest,
	}
	if result := apply(claim); result.err != nil || result.lease == nil || result.lease.ID != jobID {
		t.Fatalf("claiming legacy job with managed credential: %+v", result)
	}
	snapshot, err := view.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	revoke := operation{
		Kind: "worker_credential", At: at.Add(time.Second), Key: "revoke",
		Credential: &api.WorkerCredentialChange{WorkerID: "pc"},
	}
	if result := apply(revoke); result.err != nil {
		t.Fatal(result.err)
	}
	file, err := os.Open(snapshot.(*projectionSnapshot).path)
	if err != nil {
		t.Fatal(err)
	}
	if err := view.Restore(file); err != nil {
		t.Fatal(err)
	}
	claim.At = at.Add(time.Second)
	if result := apply(claim); result.err != nil || result.lease == nil || result.lease.Token != "lease-token" {
		t.Fatalf("credential or claim receipt missing after snapshot restore: %+v", result)
	}
	if result := apply(revoke); result.err != nil {
		t.Fatal(result.err)
	}
	if result := apply(claim); !errors.Is(result.err, errWorkerUnauthorized) {
		t.Fatalf("revoked digest after replay: %+v", result)
	}
}

func TestProjectionRejectsPartialCredentialSnapshot(t *testing.T) {
	partial, _ := legacySnapshot(t, time.Now().UTC(), true)
	view := &projection{directory: t.TempDir()}
	defer view.close()
	if err := view.Restore(partial); err == nil || !strings.Contains(err.Error(), "incomplete worker credential state") {
		t.Fatalf("restore partial credential state: %v", err)
	}
}

func legacySnapshot(t *testing.T, at time.Time, partialCredentials bool) (*os.File, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range stateBuckets {
			if _, err := tx.CreateBucket(bucket); err != nil {
				return err
			}
		}
		if partialCredentials {
			_, err := tx.CreateBucket(workerCredentials)
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s := &store{db: db}
	job, err := s.submitAt(api.SubmitRequest{Engine: "echo", Prompt: "legacy job", MaxTokens: 4}, "", at, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	return file, job.ID
}
