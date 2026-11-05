package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestOpenBackupUsesConfiguredReadyOrigin(t *testing.T) {
	const content = "example backup stream"
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != BackupPath || r.Header.Get("Authorization") != "Bearer admin" {
			t.Errorf("unexpected backup request: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer follower.Close()
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != BackupPath || r.Header.Get("Authorization") != "Bearer admin" {
			t.Errorf("unexpected backup request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", BackupContentType)
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		io.WriteString(w, content)
	}))
	defer leader.Close()
	c, err := NewClusterClient([]string{follower.URL, leader.URL}, "admin", false)
	if err != nil {
		t.Fatal(err)
	}
	body, size, err := c.OpenBackup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil || size != int64(len(content)) || string(data) != content {
		t.Fatalf("backup stream: size=%d data=%q err=%v", size, data, err)
	}
}

func TestOpenBackupRejectsNonArchiveResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "HTML from the wrong service")
	}))
	defer server.Close()
	c, err := NewClient(server.URL, "admin", false)
	if err != nil {
		t.Fatal(err)
	}
	body, _, err := c.OpenBackup(t.Context())
	if body != nil || err == nil || !strings.Contains(err.Error(), "invalid coordinator backup response") {
		t.Fatalf("unexpected result: body=%v err=%v", body, err)
	}
}
