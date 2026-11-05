package coordinator

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
	bolt "go.etcd.io/bbolt"
)

func TestManagedCredentialsFenceLeasesOnRotationAndRevocation(t *testing.T) {
	ctx := context.Background()
	s, err := openStore(filepath.Join(t.TempDir(), "jobs.db"), time.Minute, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	a, newA, b := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	job, err := s.Submit(ctx, api.SubmitRequest{Engine: "echo", Prompt: "test", MaxTokens: 4}, "")
	if err != nil {
		t.Fatal(err)
	}
	req := api.ClaimRequest{WorkerID: "pc-a", Engine: "echo"}
	if _, err := s.Claim(ctx, req, "a-claim", a); !errors.Is(err, errWorkerUnauthorized) {
		t.Fatalf("unknown credential: %v", err)
	}
	for _, change := range []api.WorkerCredentialChange{{WorkerID: "pc-a", Digest: a}, {WorkerID: "pc-b", Digest: b}} {
		if err := s.ChangeWorkerCredential(ctx, change, "issue-"+change.WorkerID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Claim(ctx, req, "a-claim", b); !errors.Is(err, errWorkerUnauthorized) {
		t.Fatalf("claim with another worker's credential: %v", err)
	}
	first, err := s.Claim(ctx, req, "a-claim", a)
	if err != nil || first == nil || first.Attempt != 1 {
		t.Fatalf("first claim: %+v, %v", first, err)
	}
	if _, err := s.Renew(ctx, job.ID, first.Token, b); !errors.Is(err, errWorkerUnauthorized) {
		t.Fatalf("renew with another worker's credential: %v", err)
	}
	finish := api.FinishRequest{Token: first.Token, Output: "stale"}
	if err := s.Finish(ctx, job.ID, finish, b); !errors.Is(err, errWorkerUnauthorized) {
		t.Fatalf("finish with another worker's credential: %v", err)
	}
	if err := s.ChangeWorkerCredential(ctx, api.WorkerCredentialChange{WorkerID: req.WorkerID, Digest: a}, "repeat-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Renew(ctx, job.ID, first.Token, a); err != nil {
		t.Fatalf("reissuing the current digest should leave the lease valid: %v", err)
	}
	if err := s.ChangeWorkerCredential(ctx, api.WorkerCredentialChange{WorkerID: req.WorkerID, Digest: newA}, "rotate-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, req, "a-claim", a); !errors.Is(err, errWorkerUnauthorized) {
		t.Fatalf("revoked credential can replay a claim: %v", err)
	}
	if _, err := s.Renew(ctx, job.ID, first.Token, a); !errors.Is(err, errWorkerUnauthorized) {
		t.Fatalf("revoked credential can renew: %v", err)
	}
	if err := s.Finish(ctx, job.ID, finish, a); !errors.Is(err, errWorkerUnauthorized) {
		t.Fatalf("revoked credential can finish: %v", err)
	}
	if err := s.Finish(ctx, job.ID, finish, newA); !errors.Is(err, errStale) {
		t.Fatalf("rotated credential can finish an old lease: %v", err)
	}
	queued, err := s.Status(ctx, job.ID)
	if err != nil || queued.State != api.Queued || queued.Attempt != 1 {
		t.Fatalf("requeued job after rotation: %+v, %v", queued, err)
	}
	second, err := s.Claim(ctx, api.ClaimRequest{WorkerID: "pc-b", Engine: "echo"}, "b-claim", b)
	if err != nil || second == nil || second.ID != job.ID || second.Attempt != 2 || second.Token == first.Token {
		t.Fatalf("replacement claim: %+v, %v", second, err)
	}
	if err := s.ChangeWorkerCredential(ctx, api.WorkerCredentialChange{WorkerID: "pc-b"}, "revoke-b"); err != nil {
		t.Fatal(err)
	}
	exhausted, err := s.Status(ctx, job.ID)
	if err != nil || exhausted.State != api.Failed || exhausted.Attempt != 2 || exhausted.Error == "" {
		t.Fatalf("exhausted job after revocation: %+v, %v", exhausted, err)
	}
}

func TestCredentialReceiptsAndRetiredDigestsSurviveRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	s, err := openStore(dbPath, time.Minute, 2)
	if err != nil {
		t.Fatal(err)
	}
	issue := api.WorkerCredentialChange{WorkerID: "pc", Digest: strings.Repeat("a", 64)}
	revoke := api.WorkerCredentialChange{WorkerID: "pc"}
	if err := s.ChangeWorkerCredential(ctx, issue, "issue"); err != nil {
		s.db.Close()
		t.Fatal(err)
	}
	if err := s.ChangeWorkerCredential(ctx, revoke, "revoke"); err != nil {
		s.db.Close()
		t.Fatal(err)
	}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openStore(dbPath, time.Minute, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	if err := s.ChangeWorkerCredential(ctx, issue, "issue"); err != nil {
		t.Fatalf("replay committed issue: %v", err)
	}
	req := api.ClaimRequest{WorkerID: "pc", Engine: "echo"}
	if _, err := s.Claim(ctx, req, "", issue.Digest); !errors.Is(err, errWorkerUnauthorized) {
		t.Fatalf("replay reactivated a retired credential: %v", err)
	}
	if err := s.ChangeWorkerCredential(ctx, revoke, "issue"); !errors.Is(err, errKeyConflict) {
		t.Fatalf("expected idempotency conflict: %v", err)
	}
	for _, id := range []string{"pc", "other"} {
		if err := s.ChangeWorkerCredential(ctx, api.WorkerCredentialChange{WorkerID: id, Digest: issue.Digest}, "reuse-"+id); !errors.Is(err, errWorkerDigestConflict) {
			t.Fatalf("expected retired digest conflict for %s: %v", id, err)
		}
	}
	fresh := api.WorkerCredentialChange{WorkerID: "pc", Digest: strings.Repeat("b", 64)}
	if err := s.ChangeWorkerCredential(ctx, fresh, "fresh"); err != nil {
		t.Fatal(err)
	}
	if err := s.ChangeWorkerCredential(ctx, revoke, "revoke"); err != nil {
		t.Fatalf("replay committed revoke: %v", err)
	}
	if lease, err := s.Claim(ctx, req, "", fresh.Digest); lease != nil || err != nil {
		t.Fatalf("replaying old revoke disabled the fresh credential: %+v, %v", lease, err)
	}
}

func TestReaperMarksFinalAttemptFailed(t *testing.T) {
	s, err := openStore(filepath.Join(t.TempDir(), "jobs.db"), time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	job, err := s.submit(api.SubmitRequest{Engine: "echo", Prompt: "test", MaxTokens: 4})
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := s.claim(api.ClaimRequest{WorkerID: "pc", Engine: "echo"}); lease == nil || err != nil {
		t.Fatalf("claim: %+v, %v", lease, err)
	}
	expireLease(t, s, job.ID)
	if err := s.reap(); err != nil {
		t.Fatal(err)
	}
	done, err := s.status(job.ID)
	if err != nil || done.State != api.Failed || done.Attempt != 1 || done.Error == "" {
		t.Fatalf("failed job: %+v, %v", done, err)
	}
}

func expireLease(t *testing.T, s *store, id string) {
	t.Helper()
	key, err := jobKey(id)
	if err != nil {
		t.Fatal(err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		r, err := load(tx, key)
		if err != nil {
			return err
		}
		r.DueAt = time.Now().Add(-time.Second)
		return put(tx, key, r)
	})
	if err != nil {
		t.Fatal(err)
	}
}
