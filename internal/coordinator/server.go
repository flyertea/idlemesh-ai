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
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/flyertea/idlemesh-ai/internal/api"
	"github.com/flyertea/idlemesh-ai/internal/ha"
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
	AdminToken        string
	ManagedWorkers    bool
	EnableBackup      bool
	RestoreBackup     string
	AllowInsecureHTTP bool
	Logger            *log.Logger
	Raft              *ha.Config
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
	if err := validateAuthentication(cfg, remote); err != nil {
		return err
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.RestoreBackup != "" {
		if cfg.Raft == nil || !cfg.Raft.Bootstrap || cfg.Raft.ReplaceID != "" {
			return errors.New("--restore-backup requires a fresh --raft-bootstrap voter; for standalone recovery use idlemesh restore")
		}
		files, err := os.ReadDir(cfg.Raft.DataDir)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if len(files) > 0 {
			return errors.New("--restore-backup requires a new or empty Raft data directory")
		}
	}

	var db backend
	var replicated *cluster
	if cfg.Raft == nil {
		local, err := openStore(cfg.Database, cfg.LeaseTime, cfg.MaxAttempts)
		if err != nil {
			return fmt.Errorf("open coordinator database: %w", err)
		}
		defer local.db.Close()
		db = local
	} else {
		raftConfig := *cfg.Raft
		if cfg.ManagedWorkers {
			raftConfig.Policy = "managed-workers-v1"
		}
		var prepared *PreparedBackup
		if cfg.RestoreBackup != "" {
			prepared, err = PrepareBackup(ctx, cfg.RestoreBackup)
			if err != nil {
				return fmt.Errorf("prepare recovery backup: %w", err)
			}
			raftConfig.Seed = prepared
		}
		replicated, err = openCluster(raftConfig, cfg.LeaseTime, cfg.MaxAttempts)
		if prepared != nil {
			if closeErr := prepared.Close(); closeErr != nil {
				cfg.Logger.Printf("remove staged recovery snapshot: %v", closeErr)
			}
		}
		if err != nil {
			return fmt.Errorf("open coordinator Raft cluster: %w", err)
		}
		defer replicated.close()
		db = replicated
	}

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
			if replicated != nil && cfg.Raft.ReplaceID != "" {
				if err := replicated.node.ReconcileReplacement(ctx); err != nil && !errors.Is(err, ha.ErrUnavailable) {
					cfg.Logger.Printf("reconcile Raft replacement: %v", err)
				}
			}
			if err := db.Reap(ctx); err != nil && !errors.Is(err, ha.ErrUnavailable) {
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

func validateAuthentication(cfg Config, remote bool) error {
	if cfg.ManagedWorkers {
		if cfg.WorkerToken != "" {
			return errors.New("unset IDLEMESH_WORKER_TOKEN on a coordinator using --managed-workers")
		}
		if !validAuthToken(cfg.ClientToken) || !validAuthToken(cfg.AdminToken) || cfg.ClientToken == cfg.AdminToken {
			return errors.New("set distinct IDLEMESH_CLIENT_TOKEN and IDLEMESH_ADMIN_TOKEN values (32–256 printable ASCII characters each, without spaces)")
		}
		return nil
	}
	if remote || cfg.EnableBackup || cfg.ClientToken != "" || cfg.WorkerToken != "" {
		if !validAuthToken(cfg.ClientToken) || !validAuthToken(cfg.WorkerToken) || cfg.ClientToken == cfg.WorkerToken {
			return errors.New("set distinct IDLEMESH_CLIENT_TOKEN and IDLEMESH_WORKER_TOKEN values (32–256 printable ASCII characters each, without spaces)")
		}
	}
	if cfg.EnableBackup && (!validAuthToken(cfg.AdminToken) || cfg.AdminToken == cfg.ClientToken || cfg.AdminToken == cfg.WorkerToken) {
		return errors.New("--enable-backup requires a valid IDLEMESH_ADMIN_TOKEN distinct from client and worker tokens")
	}
	return nil
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
	store      backend
	log        *log.Logger
	backupSlot chan struct{}
}

func newHandler(s backend, cfg Config) http.Handler {
	h := &handler{store: s, log: cfg.Logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		reply(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", authorize(cfg.ClientToken, func(w http.ResponseWriter, r *http.Request) {
		if err := h.store.Ready(r.Context()); err != nil {
			h.storeError(w, err)
			return
		}
		reply(w, http.StatusOK, map[string]string{"status": "ready"})
	}))
	mux.HandleFunc("POST /v1/jobs", authorize(cfg.ClientToken, h.submit))
	mux.HandleFunc("GET /v1/jobs/{id}", authorize(cfg.ClientToken, h.status))
	mux.HandleFunc("POST /v1/worker/claim", authorizeWorker(cfg, h.claim))
	mux.HandleFunc("POST /v1/worker/jobs/{id}/renew", authorizeWorker(cfg, h.renew))
	mux.HandleFunc("POST /v1/worker/jobs/{id}/finish", authorizeWorker(cfg, h.finish))
	if cfg.ManagedWorkers {
		mux.HandleFunc("POST /v1/admin/worker-credentials", authorize(cfg.AdminToken, h.changeWorkerCredential))
	}
	if cfg.EnableBackup {
		h.backupSlot = make(chan struct{}, 1)
		mux.HandleFunc("GET "+api.BackupPath, authorize(cfg.AdminToken, h.backup))
	}
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
			rejectBearer(w)
			return
		}
		next(w, r)
	}
}

func rejectBearer(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	problem(w, http.StatusUnauthorized, "a valid bearer token is required")
}

func authorizeWorker(cfg Config, next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	if !cfg.ManagedWorkers {
		return authorize(cfg.WorkerToken, func(w http.ResponseWriter, r *http.Request) {
			next(w, r, "")
		})
	}
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		if len(values) != 1 {
			rejectBearer(w)
			return
		}
		token, ok := strings.CutPrefix(values[0], "Bearer ")
		if !ok {
			rejectBearer(w)
			return
		}
		digest, ok := api.WorkerCredentialDigest(token)
		if !ok {
			rejectBearer(w)
			return
		}
		// The backend authenticates this digest together with the requested
		// operation, so a concurrent revocation cannot be bypassed.
		next(w, r, digest)
	}
}

func (h *handler) backup(w http.ResponseWriter, r *http.Request) {
	select {
	case h.backupSlot <- struct{}{}:
		defer func() { <-h.backupSlot }()
	default:
		problem(w, http.StatusTooManyRequests, "a backup is already being prepared")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if err := http.NewResponseController(w).SetWriteDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		h.storeError(w, err)
		return
	}
	archive, err := h.store.Backup(ctx)
	if err != nil {
		h.storeError(w, err)
		return
	}
	defer archive.Close()
	w.Header().Set("Content-Type", api.BackupContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(archive.Size, 10))
	w.Header().Set("Content-Disposition", `attachment; filename="idlemesh-backup.imbackup"`)
	if _, err := io.Copy(w, archive); err != nil {
		h.log.Printf("stream backup: %v", err)
	}
}

func (h *handler) changeWorkerCredential(w http.ResponseWriter, r *http.Request) {
	var req api.WorkerCredentialChange
	if !decode(w, r, &req) {
		return
	}
	if !api.ValidWorkerID(req.WorkerID) || (req.Digest != "" && !api.ValidWorkerCredentialDigest(req.Digest)) {
		problem(w, http.StatusBadRequest, "invalid worker ID or credential digest")
		return
	}
	key, valid := idempotencyKey(w, r)
	if !valid {
		return
	}
	if key == "" {
		problem(w, http.StatusBadRequest, "Idempotency-Key is required for credential changes")
		return
	}
	if err := h.store.ChangeWorkerCredential(r.Context(), req, key); err != nil {
		h.storeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	key, valid := idempotencyKey(w, r)
	if !valid {
		return
	}
	job, err := h.store.Submit(r.Context(), req, key)
	if err != nil {
		h.storeError(w, err)
		return
	}
	reply(w, http.StatusAccepted, job)
}

func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	job, err := h.store.Status(r.Context(), r.PathValue("id"))
	if err != nil {
		h.storeError(w, err)
		return
	}
	reply(w, http.StatusOK, job)
}

func (h *handler) claim(w http.ResponseWriter, r *http.Request, workerDigest string) {
	var req api.ClaimRequest
	if !decode(w, r, &req) {
		return
	}
	if !api.ValidWorkerID(req.WorkerID) || !api.ValidCapability(req.Engine, req.Model) {
		problem(w, http.StatusBadRequest, "invalid worker_id, engine, or model")
		return
	}
	key, valid := idempotencyKey(w, r)
	if !valid {
		return
	}
	lease, err := h.store.Claim(r.Context(), req, key, workerDigest)
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

func (h *handler) renew(w http.ResponseWriter, r *http.Request, workerDigest string) {
	var req api.LeaseRequest
	if !decode(w, r, &req) {
		return
	}
	if len(req.Token) != 64 {
		problem(w, http.StatusBadRequest, "invalid lease token")
		return
	}
	lease, err := h.store.Renew(r.Context(), r.PathValue("id"), req.Token, workerDigest)
	if err != nil {
		h.storeError(w, err)
		return
	}
	reply(w, http.StatusOK, lease)
}

func (h *handler) finish(w http.ResponseWriter, r *http.Request, workerDigest string) {
	var req api.FinishRequest
	if !decode(w, r, &req) {
		return
	}
	if len(req.Token) != 64 || len(req.Error) > api.MaxErrorBytes || len(req.Output) > api.MaxOutputBytes ||
		!utf8.ValidString(req.Error) || !utf8.ValidString(req.Output) || (req.Error != "" && req.Output != "") {
		problem(w, http.StatusBadRequest, "invalid lease token, output, or error")
		return
	}
	if err := h.store.Finish(r.Context(), r.PathValue("id"), req, workerDigest); err != nil {
		h.storeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	values := r.Header.Values(api.IdempotencyKeyHeader)
	if len(values) == 0 {
		return "", true
	}
	if len(values) != 1 || !api.ValidIdempotencyKey(values[0]) {
		problem(w, http.StatusBadRequest, "invalid Idempotency-Key")
		return "", false
	}
	return values[0], true
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
	case errors.Is(err, errKeyConflict):
		problem(w, http.StatusConflict, errKeyConflict.Error())
	case errors.Is(err, errWorkerUnauthorized):
		rejectBearer(w)
	case errors.Is(err, errWorkerDigestConflict):
		problem(w, http.StatusConflict, errWorkerDigestConflict.Error())
	case errors.Is(err, errInvalidWorkerCredential):
		problem(w, http.StatusBadRequest, errInvalidWorkerCredential.Error())
	case errors.Is(err, ha.ErrUnavailable):
		problem(w, http.StatusServiceUnavailable, "no ready coordinator leader at this endpoint")
	default:
		h.log.Printf("database operation failed: %v", err)
		problem(w, http.StatusInternalServerError, "database operation failed")
	}
}
