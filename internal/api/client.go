package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const (
	clientTimeout     = 10 * time.Second
	clusterTryTimeout = 1500 * time.Millisecond
	clusterRetryDelay = 150 * time.Millisecond
	maxClusterBackoff = 800 * time.Millisecond
)

var (
	ErrServerURL      = errors.New("server must be an HTTP(S) origin without credentials, a path, or a query")
	ErrInsecureServer = errors.New("non-loopback HTTP requires --allow-insecure-http; use HTTPS or an encrypted private network")
)

type Client struct {
	origins   []string
	preferred atomic.Uint32
	token     string
	http      *http.Client
}

func NewClient(baseURL, token string, allowInsecure bool) (*Client, error) {
	return NewClusterClient([]string{baseURL}, token, allowInsecure)
}

// NewClusterClient uses the configured origins as interchangeable members of one cluster.
func NewClusterClient(origins []string, token string, allowInsecure bool) (*Client, error) {
	if len(origins) == 0 {
		return nil, errors.New("configure at least one coordinator origin")
	}
	servers := make([]string, 0, len(origins))
	seen := make(map[string]bool, len(origins))
	for _, raw := range origins {
		u, err := ValidateServerURL(raw, allowInsecure)
		if err != nil {
			return nil, err
		}
		origin := u.String()
		if !seen[origin] {
			servers = append(servers, origin)
			seen[origin] = true
		}
	}
	return &Client{
		origins: servers,
		token:   token,
		http: &http.Client{
			Timeout:       clientTimeout,
			Transport:     DirectTransport(),
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

// DirectTransport keeps cluster and model traffic off environment proxies.
func DirectTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	dial := transport.DialContext
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if strings.EqualFold(host, "localhost") {
			address = net.JoinHostPort("127.0.0.1", port)
		}
		return dial(ctx, network, address)
	}
	return transport
}

type responseError struct {
	Status  int
	Message string
}

func (e *responseError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Status, e.Message)
}

func IsStatus(err error, code int) bool {
	return StatusCode(err) == code
}

func StatusCode(err error) int {
	var response *responseError
	if errors.As(err, &response) {
		return response.Status
	}
	return 0
}

func (c *Client) Submit(ctx context.Context, request SubmitRequest) (Job, error) {
	key, err := randomIdempotencyKey()
	if err != nil {
		return Job{}, err
	}
	return c.SubmitWithKey(ctx, request, key)
}

// SubmitWithKey lets a caller retry the same logical submission after an ambiguous result.
func (c *Client) SubmitWithKey(ctx context.Context, request SubmitRequest, key string) (Job, error) {
	if !ValidIdempotencyKey(key) {
		return Job{}, errors.New("idempotency key must have 1–128 ASCII letters, digits, periods, underscores or hyphens")
	}
	var job Job
	_, err := c.do(ctx, http.MethodPost, "/v1/jobs", key, request, &job)
	return job, err
}

func (c *Client) Status(ctx context.Context, id string) (Job, error) {
	var job Job
	_, err := c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), "", nil, &job)
	return job, err
}

func (c *Client) Claim(ctx context.Context, request ClaimRequest) (*Lease, error) {
	key, err := randomIdempotencyKey()
	if err != nil {
		return nil, err
	}
	var lease Lease
	status, err := c.do(ctx, http.MethodPost, "/v1/worker/claim", key, request, &lease)
	if status == http.StatusNoContent || err != nil {
		return nil, err
	}
	return &lease, nil
}

func (c *Client) Renew(ctx context.Context, id string, request LeaseRequest) (*Lease, error) {
	var lease Lease
	_, err := c.do(ctx, http.MethodPost, workerJobPath(id)+"/renew", "", request, &lease)
	if err != nil {
		return nil, err
	}
	return &lease, nil
}

func (c *Client) Finish(ctx context.Context, id string, request FinishRequest) error {
	_, err := c.do(ctx, http.MethodPost, workerJobPath(id)+"/finish", "", request, nil)
	return err
}

func (c *Client) ChangeWorkerCredential(ctx context.Context, request WorkerCredentialChange) error {
	key, err := randomIdempotencyKey()
	if err != nil {
		return err
	}
	status, err := c.do(ctx, http.MethodPost, "/v1/admin/worker-credentials", key, request, nil)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("worker credential change was not confirmed (HTTP %d)", status)
	}
	return nil
}

func workerJobPath(id string) string {
	return "/v1/worker/jobs/" + url.PathEscape(id)
}

func randomIdempotencyKey() (string, error) {
	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(key[:]), nil
}

func (c *Client) do(ctx context.Context, method, path, key string, request, response any) (int, error) {
	var data []byte
	if request != nil {
		var err error
		data, err = json.Marshal(request)
		if err != nil {
			return 0, err
		}
	}
	if len(c.origins) == 1 {
		return c.try(ctx, c.origins[0], method, path, key, data, response)
	}
	return c.tryCluster(ctx, method, path, key, data, response)
}

func (c *Client) tryCluster(ctx context.Context, method, path, key string, data []byte, response any) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, clientTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	var transient error
	for delay := clusterRetryDelay; ctx.Err() == nil; delay = min(delay*2, maxClusterBackoff) {
		start := int(c.preferred.Load())
		for attempt := 0; attempt < len(c.origins) && ctx.Err() == nil; attempt++ {
			index := (start + attempt) % len(c.origins)
			// Reserve time for the remaining origins when a lease deadline is tight.
			timeout := min(clusterTryTimeout, time.Until(deadline)/time.Duration(len(c.origins)-attempt))
			tryCtx, stop := context.WithTimeout(ctx, timeout)
			status, err := c.try(tryCtx, c.origins[index], method, path, key, data, response)
			stop()
			if err == nil {
				c.preferred.Store(uint32(index))
				return status, nil
			}
			if !retryable(status) {
				return status, err
			}
			if status != 0 || StatusCode(transient) == 0 {
				transient = err
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
	}
	return StatusCode(transient), errors.Join(transient, ctx.Err())
}

func retryable(status int) bool {
	return status == 0 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests ||
		status >= 500 && status < 600
}

func (c *Client) try(ctx context.Context, origin, method, path, key string, data []byte, response any) (int, error) {
	var body io.Reader
	if data != nil {
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, origin+path, body)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if key != "" {
		req.Header.Set(IdempotencyKeyHeader, key)
		// Avoid an implicit Transport replay when a v0.1 server ignores the key.
		req.GetBody = nil
	}
	started := time.Now()
	res, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	status := res.StatusCode
	if status < 200 || status >= 300 {
		var failure struct {
			Error string `json:"error"`
		}
		json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&failure)
		if failure.Error == "" {
			failure.Error = http.StatusText(status)
		}
		return status, &responseError{Status: status, Message: failure.Error}
	}
	if status == http.StatusNoContent || response == nil {
		return status, nil
	}
	payload, err := io.ReadAll(io.LimitReader(res.Body, MaxBodyBytes+1))
	if err != nil {
		return 0, err
	}
	if len(payload) > MaxBodyBytes {
		return status, errors.New("coordinator response exceeds the maximum body size")
	}
	if err := json.Unmarshal(payload, response); err != nil {
		return status, err
	}
	if lease, ok := response.(*Lease); ok {
		lease.RequestStarted = started
	}
	return status, nil
}
