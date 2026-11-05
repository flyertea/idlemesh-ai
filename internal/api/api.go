package api

import (
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxPromptBytes = 32 << 10
	MaxOutputBytes = 256 << 10
	MaxErrorBytes  = 1024
	MaxBodyBytes   = 2 << 20

	IdempotencyKeyHeader = "Idempotency-Key"
)

const (
	Queued    = "queued"
	Running   = "running"
	Succeeded = "succeeded"
	Failed    = "failed"
)

type SubmitRequest struct {
	Engine    string `json:"engine"`
	Model     string `json:"model,omitempty"`
	Prompt    string `json:"prompt"`
	MaxTokens int    `json:"max_tokens"`
}

type Job struct {
	ID          string    `json:"id"`
	State       string    `json:"state"`
	Engine      string    `json:"engine"`
	Model       string    `json:"model,omitempty"`
	Attempt     int       `json:"attempt"`
	MaxAttempts int       `json:"max_attempts"`
	WorkerID    string    `json:"worker_id,omitempty"`
	Output      string    `json:"output,omitempty"`
	Error       string    `json:"error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type ClaimRequest struct {
	WorkerID string `json:"worker_id"`
	Engine   string `json:"engine"`
	Model    string `json:"model,omitempty"`
}

type Lease struct {
	ID        string `json:"id"`
	Token     string `json:"token,omitempty"`
	Prompt    string `json:"prompt,omitempty"`
	Engine    string `json:"engine,omitempty"`
	Model     string `json:"model,omitempty"`
	MaxTokens int    `json:"max_tokens,omitempty"`
	Attempt   int    `json:"attempt,omitempty"`
	TTLMillis int64  `json:"ttl_ms"`
	// RequestStarted is the local monotonic start of the successful HTTP attempt.
	RequestStarted time.Time `json:"-"`
}

type LeaseRequest struct {
	Token string `json:"token"`
}

type FinishRequest struct {
	Token  string `json:"token"`
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

var (
	modelPattern          = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_./:-]{0,127}$`)
	idPattern             = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)
	idempotencyKeyPattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)
)

func ValidCapability(engine, model string) bool {
	switch engine {
	case "echo":
		return model == ""
	case "ollama":
		return modelPattern.MatchString(model)
	default:
		return false
	}
}

func ValidWorkerID(id string) bool {
	return idPattern.MatchString(id)
}

func ValidIdempotencyKey(key string) bool {
	return idempotencyKeyPattern.MatchString(key)
}

func ValidPrompt(prompt string) bool {
	return len(prompt) <= MaxPromptBytes && utf8.ValidString(prompt) && strings.TrimSpace(prompt) != ""
}

func LoopbackHost(host string) bool {
	return strings.EqualFold(host, "localhost") || net.ParseIP(host).IsLoopback()
}

func ValidateServerURL(raw string, allowInsecure bool) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") || u.RawPath != "" ||
		(u.Scheme != "http" && u.Scheme != "https") {
		return nil, ErrServerURL
	}
	if u.Scheme == "http" && !LoopbackHost(u.Hostname()) && !allowInsecure {
		return nil, ErrInsecureServer
	}
	u.Path = ""
	return u, nil
}
