package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWorkerCredentialFormatAndDigest(t *testing.T) {
	const expected = "2376d0fe0df5a01f3434292daca26b3860ced9f99a590ad3138ed3c81200c840"
	secret := strings.Repeat("ab", 32)
	if digest, ok := WorkerCredentialDigest("idm_" + secret); !ok || digest != expected {
		t.Fatal("credential digest did not match SHA-256 of the complete bearer token")
	}
	for _, token := range []string{secret, "idm_" + secret[1:], "idm_" + strings.ToUpper(secret), "idm_" + secret + "\n", "idm_" + secret[:63] + "g"} {
		if _, ok := WorkerCredentialDigest(token); ok {
			t.Fatal("accepted malformed worker credential")
		}
	}
	token, digest, err := NewWorkerCredential()
	if err != nil || !ValidWorkerCredentialDigest(digest) {
		t.Fatalf("credential generation failed: %v", err)
	}
	if derived, ok := WorkerCredentialDigest(token); !ok || derived != digest {
		t.Fatal("generated token failed validation or digest mismatch")
	}
	other, _, err := NewWorkerCredential()
	if err != nil || other == token {
		t.Fatal("repeated generation did not create a fresh token")
	}
}

func TestChangeWorkerCredentialRetriesOneLogicalChange(t *testing.T) {
	type observed struct {
		key, body string
	}
	requests := make(chan observed, 64)
	server := func(status int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/v1/admin/worker-credentials" || r.Header.Get("Authorization") != "Bearer admin-token" {
				t.Error("incorrect credential change method, endpoint or role")
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			requests <- observed{key: r.Header.Get(IdempotencyKeyHeader), body: string(body)}
			w.WriteHeader(status)
		}))
	}
	follower, leader := server(http.StatusServiceUnavailable), server(http.StatusNoContent)
	defer follower.Close()
	defer leader.Close()
	client, err := NewClusterClient([]string{follower.URL, leader.URL}, "admin-token", false)
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("ab", 32)
	if err := client.ChangeWorkerCredential(context.Background(), WorkerCredentialChange{WorkerID: "a", Digest: digest}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 {
		t.Fatalf("expected two attempts, got %d", len(requests))
	}
	first, second := <-requests, <-requests
	if first != second || !ValidIdempotencyKey(first.key) || first.body != fmt.Sprintf(`{"worker_id":"a","digest":"%s"}`, digest) {
		t.Fatal("credential change was not replayed with exactly the same body and key")
	}
	if err := client.ChangeWorkerCredential(context.Background(), WorkerCredentialChange{WorkerID: "a"}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("revocation did not go straight to leader, got %d attempts", len(requests))
	}
	if revoke := <-requests; revoke.key == first.key || revoke.body != `{"worker_id":"a"}` {
		t.Fatal("revocation reused a key or included a digest")
	}
}

func TestChangeWorkerCredentialRequiresNoContentConfirmation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "admin-token", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ChangeWorkerCredential(context.Background(), WorkerCredentialChange{WorkerID: "a"}); err == nil {
		t.Fatal("200 did not confirm a credential change")
	}
}
