package coordinator

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
	"github.com/hashicorp/raft"
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
