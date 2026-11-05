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

func TestBackupEndpointRequiresOptInAndAdmin(t *testing.T) {
	cfg := Config{
		ClientToken: strings.Repeat("c", 32),
		WorkerToken: strings.Repeat("w", 32),
		AdminToken:  strings.Repeat("a", 32),
		Logger:      log.New(io.Discard, "", 0),
	}
	store, err := openStore(filepath.Join(t.TempDir(), "source.db"), time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()

	client := func(server *httptest.Server, token string) *api.Client {
		t.Helper()
		c, err := api.NewClient(server.URL, token, false)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	disabled := httptest.NewServer(newHandler(store, cfg))
	defer disabled.Close()
	if _, _, err := client(disabled, cfg.AdminToken).OpenBackup(t.Context()); !api.IsStatus(err, http.StatusNotFound) {
		t.Fatalf("disabled backup endpoint: %v", err)
	}
	cfg.EnableBackup = true
	if err := validateAuthentication(cfg, false); err != nil {
		t.Fatal(err)
	}
	enabled := httptest.NewServer(newHandler(store, cfg))
	defer enabled.Close()
	for _, token := range []string{"", cfg.ClientToken, cfg.WorkerToken} {
		if _, _, err := client(enabled, token).OpenBackup(t.Context()); !api.IsStatus(err, http.StatusUnauthorized) {
			t.Fatalf("backup without admin role: %v", err)
		}
	}
	body, size, err := client(enabled, cfg.AdminToken).OpenBackup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	n, err := io.Copy(io.Discard, body)
	if err != nil || size <= 0 || n != size {
		t.Fatalf("admin backup stream: size=%d read=%d error=%v", size, n, err)
	}
}
