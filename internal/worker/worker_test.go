package worker

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

func TestWorkerRenewsAndRetriesResult(t *testing.T) {
	renewal := make(chan struct{})
	finished := make(chan api.FinishRequest, 1)
	var claims, renewals, finishes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/worker/claim":
			if claims.Add(1) != 1 {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			json.NewEncoder(w).Encode(testLease())
		case "/v1/worker/jobs/job1/renew":
			var req api.LeaseRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Token != "lease1" {
				t.Errorf("invalid renewal request: %#v, %v", req, err)
			}
			json.NewEncoder(w).Encode(api.Lease{ID: "job1", TTLMillis: 1000})
			if renewals.Add(1) == 1 {
				close(renewal)
			}
		case "/v1/worker/jobs/job1/finish":
			var req api.FinishRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("invalid finish request: %v", err)
			}
			if finishes.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			select {
			case finished <- req:
			default:
			}
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := startTestWorker(t, ctx, server.URL, func(ctx context.Context, lease api.Lease) (string, error) {
		select {
		case <-renewal:
			return lease.Prompt, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	select {
	case req := <-finished:
		if req.Token != "lease1" || req.Output != "test prompt" || req.Error != "" {
			t.Fatalf("unexpected outcome: %#v", req)
		}
	case err := <-done:
		t.Fatalf("worker stopped before finishing: %v", err)
	case <-ctx.Done():
		t.Fatal("worker did not finish the lease")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if finishes.Load() != 2 {
		t.Fatalf("expected one retry, got %d submissions", finishes.Load())
	}
}

func TestWorkerCancelsStaleLeaseWithoutSubmitting(t *testing.T) {
	var claims, finishes atomic.Int32
	canceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/worker/claim":
			if claims.Add(1) == 1 {
				json.NewEncoder(w).Encode(testLease())
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/v1/worker/jobs/job1/renew":
			w.WriteHeader(http.StatusConflict)
		case "/v1/worker/jobs/job1/finish":
			finishes.Add(1)
			w.WriteHeader(http.StatusConflict)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := startTestWorker(t, ctx, server.URL, func(ctx context.Context, _ api.Lease) (string, error) {
		<-ctx.Done()
		close(canceled)
		return "", ctx.Err()
	})
	select {
	case <-canceled:
	case err := <-done:
		t.Fatalf("worker stopped before canceling inference: %v", err)
	case <-ctx.Done():
		t.Fatal("stale lease did not cancel inference")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if finishes.Load() != 0 {
		t.Fatal("worker submitted an outcome for a stale lease")
	}
}

func testLease() api.Lease {
	return api.Lease{ID: "job1", Token: "lease1", Prompt: "test prompt", Engine: "echo", MaxTokens: 16, Attempt: 1, TTLMillis: 1000}
}

func startTestWorker(t *testing.T, ctx context.Context, baseURL string, runner Runner) <-chan error {
	t.Helper()
	client, err := api.NewClient(baseURL, "token", false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, client, runner, Config{
			ID: "test", Engine: "echo", PollInterval: 10 * time.Millisecond,
			JobTimeout: 4 * time.Second, Logger: log.New(io.Discard, "", 0),
		})
	}()
	return done
}
