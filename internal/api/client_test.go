package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestClusterSubmitWaitsForElectionAndPrefersLeader(t *testing.T) {
	type received struct {
		origin string
		method string
		key    string
		auth   string
		body   string
	}
	requests := make(chan received, 64)
	var followerPosts, leaderPosts atomic.Int32
	server := func(origin string, posts *atomic.Int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			requests <- received{origin, r.Method, r.Header.Get(IdempotencyKeyHeader), r.Header.Get("Authorization"), string(body)}
			if r.Method == http.MethodPost && (posts.Add(1) == 1 || origin == "follower") {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintln(w, `{"error":"electing leader"}`)
				return
			}
			fmt.Fprintln(w, `{"id":"a","state":"queued"}`)
		}))
	}
	follower, leader := server("follower", &followerPosts), server("leader", &leaderPosts)
	defer follower.Close()
	defer leader.Close()
	client, err := NewClusterClient([]string{follower.URL, leader.URL}, "role-token", false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	job, err := client.Submit(ctx, SubmitRequest{Engine: "echo", Prompt: "hello", MaxTokens: 16})
	if err != nil || job.ID != "a" {
		t.Fatalf("job=%+v err=%v", job, err)
	}
	if len(requests) != 4 {
		t.Fatalf("want two rotations, got %d requests", len(requests))
	}
	first := <-requests
	if !ValidIdempotencyKey(first.key) || len(first.key) != 32 {
		t.Fatalf("missing generated key: %q", first.key)
	}
	for _, origin := range []string{"leader", "follower", "leader"} {
		r := <-requests
		if r.origin != origin || r.key != first.key || r.body != first.body || r.auth != "Bearer role-token" {
			t.Fatalf("unexpected attempt: %+v", r)
		}
	}
	if _, err := client.Status(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if r := <-requests; r.origin != "leader" || r.method != http.MethodGet || r.key != "" || len(requests) != 0 {
		t.Fatalf("status did not prefer successful leader: %+v", r)
	}
}

func TestClusterClaimRetriesInterruptedResponseAndStopsOnNoJob(t *testing.T) {
	firstKeys := make(chan string, 64)
	secondKeys := make(chan string, 64)
	firstRequestAt := make(chan time.Time, 64)
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstRequestAt <- time.Now()
		io.Copy(io.Discard, r.Body)
		firstKeys <- r.Header.Get(IdempotencyKeyHeader)
		fmt.Fprint(w, `{"id":`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer first.Close()
	var claims atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondKeys <- r.Header.Get(IdempotencyKeyHeader)
		if claims.Add(1) == 1 {
			fmt.Fprintln(w, `{"id":"b","token":"lease","ttl_ms":15000}`)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer second.Close()
	client, err := NewClusterClient([]string{first.URL, second.URL}, "", false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	claim := ClaimRequest{WorkerID: "test", Engine: "echo"}
	lease, err := client.Claim(ctx, claim)
	if err != nil || lease == nil || lease.ID != "b" {
		t.Fatalf("lease=%+v err=%v", lease, err)
	}
	if !lease.RequestStarted.After(<-firstRequestAt) {
		t.Fatalf("lease start did not use the successful attempt: %v", lease.RequestStarted)
	}
	key := <-firstKeys
	if !ValidIdempotencyKey(key) || key != <-secondKeys {
		t.Fatal("claim did not preserve its key across failover")
	}
	if lease, err := client.Claim(ctx, claim); lease != nil || err != nil {
		t.Fatalf("no-job response returned lease=%+v err=%v", lease, err)
	}
	if next := <-secondKeys; !ValidIdempotencyKey(next) || next == key || len(firstKeys) != 0 {
		t.Fatal("next logical claim did not use a fresh key or retried a successful no-job response")
	}
}

func TestClusterDoesNotFollowRedirectsOrRetryPermanentResponses(t *testing.T) {
	var unexpected atomic.Int32
	unexpectedHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		unexpected.Add(1)
	})
	alternate := httptest.NewServer(unexpectedHandler)
	redirect := httptest.NewServer(unexpectedHandler)
	defer alternate.Close()
	defer redirect.Close()
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusConflict, http.StatusTemporaryRedirect} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", redirect.URL)
				w.WriteHeader(status)
				fmt.Fprintln(w, `{"error":"terminal response"}`)
			}))
			defer first.Close()
			client, err := NewClusterClient([]string{first.URL, alternate.URL}, "role-token", false)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.SubmitWithKey(context.Background(), SubmitRequest{Prompt: "hello"}, "call-1")
			if !IsStatus(err, status) || unexpected.Load() != 0 {
				t.Fatalf("err=%v alternate requests=%d", err, unexpected.Load())
			}
		})
	}
}

func TestClusterRenewReservesTimeForAnotherOrigin(t *testing.T) {
	firstRequestAt := make(chan time.Time, 64)
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstRequestAt <- time.Now()
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request LeaseRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Token != "current-lease" || r.URL.Path != "/v1/worker/jobs/a/renew" {
			t.Errorf("request=%+v path=%s decode error=%v", request, r.URL.Path, err)
		}
		fmt.Fprintln(w, `{"id":"a","ttl_ms":15000}`)
	}))
	defer second.Close()
	client, err := NewClusterClient([]string{first.URL, second.URL}, "", false)
	if err != nil {
		t.Fatal(err)
	}
	// The worker supplies a lease deadline shorter than the normal per-origin timeout.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lease, err := client.Renew(ctx, "a", LeaseRequest{Token: "current-lease"})
	if err != nil || lease == nil || lease.ID != "a" || lease.TTLMillis != 15000 {
		t.Fatalf("lease=%+v err=%v", lease, err)
	}
	if !lease.RequestStarted.After(<-firstRequestAt) {
		t.Fatalf("renewal start did not use the successful attempt: %v", lease.RequestStarted)
	}
}

func TestClusterReturnsDeadlineAndLastTransientStatus(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, `{"error":"no quorum"}`)
	})
	first, second := httptest.NewServer(handler), httptest.NewServer(handler)
	defer first.Close()
	defer second.Close()
	client, err := NewClusterClient([]string{first.URL, second.URL}, "", false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err = client.Status(ctx, "a")
	if !errors.Is(err, context.DeadlineExceeded) || !IsStatus(err, http.StatusServiceUnavailable) || calls.Load() < 4 {
		t.Fatalf("expected a deadline after multiple unavailable rotations, got calls=%d err=%v", calls.Load(), err)
	}
}

func TestSingleOriginKeepsExplicitKeyAndDoesNotRetry(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var request SubmitRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Prompt != "hello" || r.Header.Get(IdempotencyKeyHeader) != "user.key_1" {
			t.Errorf("request=%+v decode error=%v key=%q", request, err, r.Header.Get(IdempotencyKeyHeader))
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SubmitWithKey(context.Background(), SubmitRequest{Prompt: "hello"}, "invalid key"); err == nil || calls.Load() != 0 {
		t.Fatalf("invalid key sent a request: %v", err)
	}
	if _, err := client.SubmitWithKey(context.Background(), SubmitRequest{Prompt: "hello"}, "user.key_1"); !IsStatus(err, http.StatusServiceUnavailable) || calls.Load() != 1 {
		t.Fatalf("single origin retried or lost status: calls=%d err=%v", calls.Load(), err)
	}
}
