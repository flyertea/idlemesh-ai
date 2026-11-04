package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

func TestOllamaUsesLocalCPUAndReleasesModel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			io.WriteString(w, `{"models":[{"name":"test:latest"}]}`)
		case "/api/generate":
			var req struct {
				Model     string         `json:"model"`
				Prompt    string         `json:"prompt"`
				Stream    *bool          `json:"stream"`
				KeepAlive *int           `json:"keep_alive"`
				Options   map[string]int `json:"options"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode generate request: %v", err)
			}
			numGPU, gpuSet := req.Options["num_gpu"]
			if req.Model != "test" || req.Prompt != "test prompt" ||
				req.Stream == nil || *req.Stream || req.KeepAlive == nil || *req.KeepAlive != 0 ||
				!gpuSet || numGPU != 0 || req.Options["num_thread"] != 2 ||
				req.Options["num_ctx"] != 1024 || req.Options["num_predict"] != 32 {
				t.Errorf("unexpected generate options: %#v", req)
			}
			io.WriteString(w, `{"response":"generated text","done":true}`)
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	runner, err := Ollama(server.URL, "test", 2, 1024)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner(context.Background(), api.Lease{Model: "test", Prompt: "test prompt", MaxTokens: 32})
	if err != nil || result != "generated text" {
		t.Fatalf("generate result = %q, error = %v", result, err)
	}
}

func TestOllamaRejectsRemoteModel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"models":[{"name":"test:latest","remote_host":"example.com"}]}`)
	}))
	defer server.Close()
	if _, err := Ollama(server.URL, "test", 2, 1024); err == nil || !strings.Contains(err.Error(), "remote inference") {
		t.Fatalf("expected a local model check error, got %v", err)
	}
}

func TestOllamaDoesNotFollowGenerationRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			io.WriteString(w, `{"models":[{"name":"test:latest"}]}`)
		case "/api/generate":
			http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
		default:
			t.Error("followed a redirect")
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	runner, err := Ollama(server.URL, "test", 2, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner(context.Background(), api.Lease{Model: "test", Prompt: "test prompt", MaxTokens: 32}); err == nil {
		t.Fatal("unexpected success")
	}
}
