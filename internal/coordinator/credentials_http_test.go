package coordinator

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

func TestManagedWorkerCredentialHTTP(t *testing.T) {
	ctx := t.Context()
	cfg := Config{
		ManagedWorkers: true,
		ClientToken:    strings.Repeat("c", 32),
		AdminToken:     strings.Repeat("a", 32),
		Logger:         log.New(io.Discard, "", 0),
	}
	s, err := openStore(filepath.Join(t.TempDir(), "managed.db"), 15*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	server := httptest.NewServer(newHandler(s, cfg))
	defer server.Close()

	client := func(token string) *api.Client {
		t.Helper()
		c, err := api.NewClient(server.URL, token, false)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	admin := client(cfg.AdminToken)
	user := client(cfg.ClientToken)
	issue := func(id string) *api.Client {
		t.Helper()
		token, digest, err := api.NewWorkerCredential()
		if err != nil {
			t.Fatal(err)
		}
		if err := admin.ChangeWorkerCredential(ctx, api.WorkerCredentialChange{WorkerID: id, Digest: digest}); err != nil {
			t.Fatal(err)
		}
		return client(token)
	}
	laptop := issue("laptop")
	spare := issue("spare")
	expectStatus := func(err error, status int) {
		t.Helper()
		if !api.IsStatus(err, status) {
			t.Fatalf("want HTTP %d, got %v", status, err)
		}
	}

	change := api.WorkerCredentialChange{WorkerID: "spare"}
	expectStatus(user.ChangeWorkerCredential(ctx, change), http.StatusUnauthorized)
	expectStatus(spare.ChangeWorkerCredential(ctx, change), http.StatusUnauthorized)
	request := api.SubmitRequest{Engine: "echo", Prompt: "credential failover", MaxTokens: 32}
	_, err = admin.Submit(ctx, request)
	expectStatus(err, http.StatusUnauthorized)
	_, err = laptop.Submit(ctx, request)
	expectStatus(err, http.StatusUnauthorized)
	job, err := user.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}

	_, err = laptop.Claim(ctx, api.ClaimRequest{WorkerID: "spare", Engine: "echo"})
	expectStatus(err, http.StatusUnauthorized)
	laptopClaim := api.ClaimRequest{WorkerID: "laptop", Engine: "echo"}
	initial, err := laptop.Claim(ctx, laptopClaim)
	if err != nil || initial == nil || initial.ID != job.ID || initial.Attempt != 1 {
		t.Fatalf("initial claim: %+v, %v", initial, err)
	}
	renew := api.LeaseRequest{Token: initial.Token}
	finish := api.FinishRequest{Token: initial.Token, Output: "stale"}
	_, err = spare.Renew(ctx, job.ID, renew)
	expectStatus(err, http.StatusUnauthorized)
	expectStatus(spare.Finish(ctx, job.ID, finish), http.StatusUnauthorized)

	newLaptop := issue("laptop")
	_, err = laptop.Claim(ctx, laptopClaim)
	expectStatus(err, http.StatusUnauthorized)
	_, err = laptop.Renew(ctx, job.ID, renew)
	expectStatus(err, http.StatusUnauthorized)
	expectStatus(laptop.Finish(ctx, job.ID, finish), http.StatusUnauthorized)
	current, err := user.Status(ctx, job.ID)
	if err != nil || current.State != api.Queued || current.Attempt != 1 {
		t.Fatalf("rotated job: %+v, %v", current, err)
	}
	lease, err := spare.Claim(ctx, api.ClaimRequest{WorkerID: "spare", Engine: "echo"})
	if err != nil || lease == nil || lease.ID != job.ID || lease.Token == initial.Token || lease.Attempt != 2 {
		t.Fatalf("reassigned job: %+v, %v", lease, err)
	}
	if err := spare.Finish(ctx, job.ID, api.FinishRequest{Token: lease.Token, Output: "replacement completed"}); err != nil {
		t.Fatal(err)
	}
	if next, err := newLaptop.Claim(ctx, laptopClaim); next != nil || err != nil {
		t.Fatalf("rotated credential was not accepted: %+v, %v", next, err)
	}
	if err := admin.ChangeWorkerCredential(ctx, api.WorkerCredentialChange{WorkerID: "laptop"}); err != nil {
		t.Fatal(err)
	}
	_, err = newLaptop.Claim(ctx, laptopClaim)
	expectStatus(err, http.StatusUnauthorized)
}

func TestManagedModeRequiresIndependentAdminAndClientTokens(t *testing.T) {
	validClient, validAdmin := strings.Repeat("c", 32), strings.Repeat("a", 32)
	cases := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"empty", Config{ManagedWorkers: true}, false},
		{"same token", Config{ManagedWorkers: true, ClientToken: validClient, AdminToken: validClient}, false},
		{"shared token still set", Config{ManagedWorkers: true, ClientToken: validClient, AdminToken: validAdmin, WorkerToken: strings.Repeat("w", 32)}, false},
		{"separate roles", Config{ManagedWorkers: true, ClientToken: validClient, AdminToken: validAdmin}, true},
		{"legacy local", Config{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAuthentication(tc.cfg, false)
			if (err == nil) != tc.ok {
				t.Fatalf("validateAuthentication: %v", err)
			}
		})
	}
}
