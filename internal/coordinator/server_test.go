package coordinator

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

func TestDurableFailover(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "jobs.db")
	cfg := Config{
		ClientToken: strings.Repeat("a", 32),
		WorkerToken: strings.Repeat("b", 32),
		Logger:      log.New(io.Discard, "", 0),
	}

	start := func() (*store, *httptest.Server, *api.Client, *api.Client) {
		t.Helper()
		s, err := openStore(path, 15*time.Second, 3)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(newHandler(s, cfg))
		t.Cleanup(func() {
			server.Close()
			s.db.Close()
		})
		user, err := api.NewClient(server.URL, cfg.ClientToken, false)
		if err != nil {
			t.Fatal(err)
		}
		worker, err := api.NewClient(server.URL, cfg.WorkerToken, false)
		if err != nil {
			t.Fatal(err)
		}
		return s, server, user, worker
	}

	s, server, user, worker := start()
	req := api.SubmitRequest{Engine: "echo", Prompt: "掉线恢复", MaxTokens: 32}
	if _, err := worker.Submit(ctx, req); !api.IsStatus(err, http.StatusUnauthorized) {
		t.Fatalf("worker should not submit jobs: %v", err)
	}
	if _, err := user.Claim(ctx, api.ClaimRequest{WorkerID: "desktop", Engine: "echo"}); !api.IsStatus(err, http.StatusUnauthorized) {
		t.Fatalf("client should not claim jobs: %v", err)
	}
	job, err := user.Submit(ctx, req)
	if err != nil || job.State != api.Queued {
		t.Fatalf("submit: %+v, %v", job, err)
	}
	if lease, err := worker.Claim(ctx, api.ClaimRequest{WorkerID: "unmatched", Engine: "ollama", Model: "small:1"}); lease != nil || err != nil {
		t.Fatalf("different engine must not claim echo work: %+v, %v", lease, err)
	}

	// Two workers may race for a job. Only one receives a lease.
	type outcome struct {
		lease *api.Lease
		err   error
	}
	results := make(chan outcome, 2)
	var ready sync.WaitGroup
	for _, id := range []string{"pc-a", "pc-b"} {
		ready.Go(func() {
			lease, err := worker.Claim(ctx, api.ClaimRequest{WorkerID: id, Engine: "echo"})
			results <- outcome{lease, err}
		})
	}
	ready.Wait()
	close(results)
	var first *api.Lease
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.lease != nil {
			if first != nil {
				t.Fatal("two workers received the same job")
			}
			first = result.lease
		}
	}
	if first == nil || first.Attempt != 1 {
		t.Fatalf("missing first lease: %+v", first)
	}

	server.Close()
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s, _, user, worker = start()
	// Force the persisted expiry so the test exercises failover without a timing race.
	// The stale token must be rejected before the periodic reaper changes the state.
	expireLease(t, s, job.ID)
	if err := worker.Finish(ctx, job.ID, api.FinishRequest{Token: first.Token, Output: "stale"}); !api.IsStatus(err, http.StatusConflict) {
		t.Fatalf("expired finish: %v", err)
	}
	if err := s.reap(); err != nil {
		t.Fatal(err)
	}
	queued, err := user.Status(ctx, job.ID)
	if err != nil || queued.State != api.Queued || queued.Attempt != 1 {
		t.Fatalf("requeued job: %+v, %v", queued, err)
	}
	second, err := worker.Claim(ctx, api.ClaimRequest{WorkerID: "spare", Engine: "echo"})
	if err != nil || second == nil || second.Attempt != 2 || second.Token == first.Token || second.Prompt != req.Prompt {
		t.Fatalf("reassigned lease: %+v, %v", second, err)
	}
	if _, err := worker.Renew(ctx, job.ID, api.LeaseRequest{Token: first.Token}); !api.IsStatus(err, http.StatusConflict) {
		t.Fatalf("old token must not renew a reassigned job: %v", err)
	}
	finish := api.FinishRequest{Token: second.Token, Output: "恢复完成"}
	for range 2 {
		if err := worker.Finish(ctx, job.ID, finish); err != nil {
			t.Fatalf("finish or retry finish: %v", err)
		}
	}
	done, err := user.Status(ctx, job.ID)
	if err != nil || done.State != api.Succeeded || done.Attempt != 2 || done.WorkerID != "spare" || done.Output != finish.Output {
		t.Fatalf("final job: %+v, %v", done, err)
	}
}
