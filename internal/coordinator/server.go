package coordinator

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

type Config struct {
	Listen            string
	Database          string
	LeaseTime         time.Duration
	MaxAttempts       int
	TLSCert           string
	TLSKey            string
	ClientToken       string
	WorkerToken       string
	AllowInsecureHTTP bool
	Logger            *log.Logger
}

func Run(ctx context.Context, cfg Config) error {
	host, _, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return fmt.Errorf("invalid --listen: %w", err)
	}
	if cfg.LeaseTime < time.Second || cfg.LeaseTime > 5*time.Minute {
		return errors.New("--lease must be between 1s and 5m")
	}
	if cfg.MaxAttempts < 1 || cfg.MaxAttempts > 20 {
		return errors.New("--max-attempts must be between 1 and 20")
	}
	tls := cfg.TLSCert != "" && cfg.TLSKey != ""
	if (cfg.TLSCert != "") != (cfg.TLSKey != "") {
		return errors.New("--tls-cert and --tls-key must be configured together")
	}
	remote := !api.LoopbackHost(host)
	if remote && !tls && !cfg.AllowInsecureHTTP {
		return errors.New("non-loopback listening requires TLS or --allow-insecure-http")
	}
	if remote || cfg.ClientToken != "" || cfg.WorkerToken != "" {
		if !validAuthToken(cfg.ClientToken) || !validAuthToken(cfg.WorkerToken) ||
			cfg.ClientToken == cfg.WorkerToken {
			return errors.New("set distinct IDLEMESH_CLIENT_TOKEN and IDLEMESH_WORKER_TOKEN values (32–256 printable ASCII characters each, without spaces)")
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}

	db, err := openStore(cfg.Database, cfg.LeaseTime, cfg.MaxAttempts)
	if err != nil {
		return fmt.Errorf("open coordinator database: %w", err)
	}
	defer db.db.Close()

	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	if !remote && !listener.Addr().(*net.TCPAddr).IP.IsLoopback() {
		return errors.New("listen address resolved outside the loopback interface")
	}

	server := &http.Server{
		Handler:           newHandler(db, cfg),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
		ErrorLog:          cfg.Logger,
	}
	serveResult := make(chan error, 1)
	go func() {
		scheme := "http"
		if tls {
			scheme = "https"
		}
		cfg.Logger.Printf("coordinator listening on %s://%s", scheme, listener.Addr())
		if tls {
			serveResult <- server.ServeTLS(listener, cfg.TLSCert, cfg.TLSKey)
			return
		}
		serveResult <- server.Serve(listener)
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := db.reap(); err != nil {
				cfg.Logger.Printf("reap leases: %v", err)
			}
		case err := <-serveResult:
			return err
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := server.Shutdown(shutdown)
			cancel()
			if err != nil {
				server.Close()
			}
			return err
		}
	}
}

func validAuthToken(token string) bool {
	if len(token) < 32 || len(token) > 256 {
		return false
	}
	for _, char := range token {
		if char < '!' || char > '~' || char == ',' {
			return false
		}
	}
	return true
}

type handler struct {
	store *store
	log   *log.Logger
}

func newHandler(s *store, cfg Config) http.Handler {
	h := &handler{store: s, log: cfg.Logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		reply(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/jobs", authorize(cfg.ClientToken, h.submit))
	mux.HandleFunc("GET /v1/jobs/{id}", authorize(cfg.ClientToken, h.status))
	mux.HandleFunc("POST /v1/worker/claim", authorize(cfg.WorkerToken, h.claim))
	mux.HandleFunc("POST /v1/worker/jobs/{id}/renew", authorize(cfg.WorkerToken, h.renew))
	mux.HandleFunc("POST /v1/worker/jobs/{id}/finish", authorize(cfg.WorkerToken, h.finish))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}

func authorize(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			host, _, err := net.SplitHostPort(r.Host)
			if err != nil {
				host = strings.Trim(r.Host, "[]")
			}
			peer, _, _ := net.SplitHostPort(r.RemoteAddr)
			if !api.LoopbackHost(host) || !net.ParseIP(peer).IsLoopback() {
				problem(w, http.StatusForbidden, "unauthenticated access is restricted to loopback")
				return
			}
		} else if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			problem(w, http.StatusUnauthorized, "a valid bearer token is required")
			return
		}
		next(w, r)
	}
}

func (h *handler) submit(w http.ResponseWriter, r *http.Request) {
	var req api.SubmitRequest
	if !decode(w, r, &req) {
		return
	}
	if !api.ValidCapability(req.Engine, req.Model) || !api.ValidPrompt(req.Prompt) ||
		req.MaxTokens < 1 || req.MaxTokens > 4096 {
		problem(w, http.StatusBadRequest, "invalid engine, model, prompt, or max_tokens (1–4096)")
		return
	}
	job, err := h.store.submit(req)
	if err != nil {
		h.storeError(w, err)
		return
	}
	reply(w, http.StatusAccepted, job)
}

func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	job, err := h.store.status(r.PathValue("id"))
	if err != nil {
		h.storeError(w, err)
		return
	}
	reply(w, http.StatusOK, job)
}

func (h *handler) claim(w http.ResponseWriter, r *http.Request) {
	var req api.ClaimRequest
	if !decode(w, r, &req) {
		return
	}
	if !api.ValidWorkerID(req.WorkerID) || !api.ValidCapability(req.Engine, req.Model) {
		problem(w, http.StatusBadRequest, "invalid worker_id, engine, or model")
		return
	}
	lease, err := h.store.claim(req)
	if err != nil {
		h.storeError(w, err)
		return
	}
	if lease == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	reply(w, http.StatusOK, lease)
}

func (h *handler) renew(w http.ResponseWriter, r *http.Request) {
	var req api.LeaseRequest
	if !decode(w, r, &req) {
		return
	}
	if len(req.Token) != 64 {
		problem(w, http.StatusBadRequest, "invalid lease token")
		return
	}
	lease, err := h.store.renew(r.PathValue("id"), req.Token)
	if err != nil {
		h.storeError(w, err)
		return
	}
	reply(w, http.StatusOK, lease)
}

func (h *handler) finish(w http.ResponseWriter, r *http.Request) {
	var req api.FinishRequest
	if !decode(w, r, &req) {
		return
	}
	if len(req.Token) != 64 || len(req.Error) > api.MaxErrorBytes || len(req.Output) > api.MaxOutputBytes ||
		!utf8.ValidString(req.Error) || !utf8.ValidString(req.Output) || (req.Error != "" && req.Output != "") {
		problem(w, http.StatusBadRequest, "invalid lease token, output, or error")
		return
	}
	if err := h.store.finish(r.PathValue("id"), req); err != nil {
		h.storeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaType != "application/json" {
		problem(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}
	body := http.MaxBytesReader(w, r.Body, api.MaxBodyBytes)
	defer body.Close()
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil && dec.Decode(new(any)) == io.EOF {
		return true
	}
	problem(w, http.StatusBadRequest, "invalid or oversized JSON")
	return false
}

func reply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func problem(w http.ResponseWriter, status int, message string) {
	reply(w, status, map[string]string{"error": message})
}

func (h *handler) storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNotFound):
		problem(w, http.StatusNotFound, errNotFound.Error())
	case errors.Is(err, errStale):
		problem(w, http.StatusConflict, errStale.Error())
	case errors.Is(err, errQueueFull):
		problem(w, http.StatusServiceUnavailable, errQueueFull.Error())
	default:
		h.log.Printf("database operation failed: %v", err)
		problem(w, http.StatusInternalServerError, "database operation failed")
	}
}
