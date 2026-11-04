package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	ErrServerURL      = errors.New("server must be an HTTP(S) origin without credentials, a path, or a query")
	ErrInsecureServer = errors.New("non-loopback HTTP requires --allow-insecure-http; use HTTPS or an encrypted private network")
)

type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

func NewClient(baseURL, token string, allowInsecure bool) (*Client, error) {
	u, err := ValidateServerURL(baseURL, allowInsecure)
	if err != nil {
		return nil, err
	}
	return &Client{
		baseURL: u.String(),
		token:   token,
		http: &http.Client{
			Timeout:       10 * time.Second,
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
	var job Job
	_, err := c.do(ctx, http.MethodPost, "/v1/jobs", request, &job)
	return job, err
}

func (c *Client) Status(ctx context.Context, id string) (Job, error) {
	var job Job
	_, err := c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, &job)
	return job, err
}

func (c *Client) Claim(ctx context.Context, request ClaimRequest) (*Lease, error) {
	var lease Lease
	status, err := c.do(ctx, http.MethodPost, "/v1/worker/claim", request, &lease)
	if status == http.StatusNoContent || err != nil {
		return nil, err
	}
	return &lease, nil
}

func (c *Client) Renew(ctx context.Context, id string, request LeaseRequest) (*Lease, error) {
	var lease Lease
	_, err := c.do(ctx, http.MethodPost, workerJobPath(id)+"/renew", request, &lease)
	if err != nil {
		return nil, err
	}
	return &lease, nil
}

func (c *Client) Finish(ctx context.Context, id string, request FinishRequest) error {
	_, err := c.do(ctx, http.MethodPost, workerJobPath(id)+"/finish", request, nil)
	return err
}

func workerJobPath(id string) string {
	return "/v1/worker/jobs/" + url.PathEscape(id)
}

func (c *Client) do(ctx context.Context, method, path string, request, response any) (int, error) {
	var body io.Reader
	if request != nil {
		data, err := json.Marshal(request)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
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
	return status, json.NewDecoder(io.LimitReader(res.Body, MaxBodyBytes)).Decode(response)
}
