package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

// Runner performs inference for a lease and stops when the context is canceled.
type Runner func(context.Context, api.Lease) (string, error)

// Echo exercises scheduling without a model or inference runtime.
func Echo(delay time.Duration) Runner {
	return func(ctx context.Context, lease api.Lease) (string, error) {
		if !wait(ctx, delay) {
			return "", ctx.Err()
		}
		return lease.Prompt, nil
	}
}

// Ollama verifies an installed, local model and returns a CPU inference runner.
func Ollama(baseURL, model string, threads, contextLength int) (Runner, error) {
	base, err := api.ValidateServerURL(baseURL, false)
	if err != nil || base.Scheme != "http" {
		return nil, errors.New("Ollama URL must be a loopback HTTP origin such as http://127.0.0.1:11434")
	}
	if !api.ValidCapability("ollama", model) || threads < 1 || contextLength < 1 {
		return nil, errors.New("Ollama requires a model, positive thread count, and positive context length")
	}
	if strings.HasSuffix(strings.ToLower(model), ":cloud") {
		return nil, fmt.Errorf("Ollama model %q uses remote inference. Choose a local model", model)
	}
	origin := base.String()
	transport := api.DirectTransport()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := checkOllamaModel(ctx, client, origin, model); err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	return func(ctx context.Context, lease api.Lease) (string, error) {
		if lease.Model != model {
			return "", errors.New("assigned model does not match the local Ollama model")
		}
		return generateOllama(ctx, client, origin, lease, threads, contextLength)
	}, nil
}

func checkOllamaModel(ctx context.Context, client *http.Client, base, model string) error {
	var response struct {
		Models []struct {
			Name        string `json:"name"`
			Model       string `json:"model"`
			RemoteHost  string `json:"remote_host"`
			RemoteModel string `json:"remote_model"`
		} `json:"models"`
	}
	if err := ollamaJSON(ctx, client, http.MethodGet, base+"/api/tags", nil, &response); err != nil {
		return fmt.Errorf("check Ollama models: %w", err)
	}
	tag := model
	if !strings.Contains(tag[strings.LastIndex(tag, "/")+1:], ":") {
		tag += ":latest"
	}
	for _, item := range response.Models {
		if item.Name == model || item.Model == model || item.Name == tag || item.Model == tag {
			if item.RemoteHost != "" || item.RemoteModel != "" {
				return fmt.Errorf("Ollama model %q uses remote inference. Choose a local model", model)
			}
			return nil
		}
	}
	return fmt.Errorf("Ollama model %q is unavailable locally. Run ollama pull %s on this machine", model, model)
}

func generateOllama(ctx context.Context, client *http.Client, base string, lease api.Lease, threads, contextLength int) (string, error) {
	request := struct {
		Model     string         `json:"model"`
		Prompt    string         `json:"prompt"`
		Stream    bool           `json:"stream"`
		KeepAlive int            `json:"keep_alive"`
		Options   map[string]int `json:"options"`
	}{
		Model:  lease.Model,
		Prompt: lease.Prompt,
		Options: map[string]int{
			"num_gpu":     0,
			"num_thread":  threads,
			"num_ctx":     contextLength,
			"num_predict": lease.MaxTokens,
		},
	}
	body, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	var response struct {
		Response *string `json:"response"`
		Done     bool    `json:"done"`
	}
	if err := ollamaJSON(ctx, client, http.MethodPost, base+"/api/generate", bytes.NewReader(body), &response); err != nil {
		return "", fmt.Errorf("Ollama inference: %w", err)
	}
	if !response.Done || response.Response == nil {
		return "", errors.New("Ollama returned an incomplete response")
	}
	return *response.Response, nil
}

func ollamaJSON(ctx context.Context, client *http.Client, method, endpoint string, body io.Reader, result any) error {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", res.StatusCode)
	}
	const limit = 1 << 20
	data, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return err
	}
	if len(data) > limit {
		return errors.New("Ollama response exceeds 1 MiB")
	}
	return json.Unmarshal(data, result)
}
