package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
	"github.com/flyertea/idlemesh-ai/internal/ha"
	bolt "go.etcd.io/bbolt"
)

type operation struct {
	Kind         string                      `json:"kind"`
	At           time.Time                   `json:"at"`
	ID           string                      `json:"id,omitempty"`
	Token        string                      `json:"token,omitempty"`
	Key          string                      `json:"key,omitempty"`
	LeaseTime    time.Duration               `json:"lease_time,omitempty"`
	MaxAttempts  int                         `json:"max_attempts,omitempty"`
	Submit       api.SubmitRequest           `json:"submit,omitempty"`
	Claim        api.ClaimRequest            `json:"claim,omitempty"`
	Finish       api.FinishRequest           `json:"finish,omitempty"`
	WorkerDigest string                      `json:"worker_digest,omitempty"`
	Credential   *api.WorkerCredentialChange `json:"credential,omitempty"`
}

type applied struct {
	job   api.Job
	lease *api.Lease
	err   error
}

type cluster struct {
	node            *ha.Node
	view            *projection
	leaseTime       time.Duration
	maxAttempts     int
	managedWorkers  bool
	readyWorkerTerm atomic.Uint64
}

func openCluster(config ha.Config, leaseTime time.Duration, maxAttempts int) (*cluster, error) {
	view := &projection{directory: filepath.Join(config.DataDir, "projection")}
	node, err := ha.Open(config, view)
	if err != nil {
		view.close()
		return nil, err
	}
	if err := view.initialize(); err != nil {
		node.Close()
		view.close()
		return nil, err
	}
	return &cluster{node: node, view: view, leaseTime: leaseTime, maxAttempts: maxAttempts, managedWorkers: config.Policy != ""}, nil
}

func (c *cluster) close() error {
	return errors.Join(c.node.Close(), c.view.close())
}

func (c *cluster) Submit(ctx context.Context, req api.SubmitRequest, key string) (api.Job, error) {
	result := c.apply(ctx, operation{Kind: "submit", Submit: req, Key: key, MaxAttempts: c.maxAttempts})
	return result.job, result.err
}

func (c *cluster) Status(ctx context.Context, id string) (api.Job, error) {
	if err := c.Ready(ctx); err != nil {
		return api.Job{}, err
	}
	c.view.mu.RLock()
	defer c.view.mu.RUnlock()
	if c.view.store == nil {
		return api.Job{}, ha.ErrUnavailable
	}
	return c.view.store.status(id)
}

func (c *cluster) Claim(ctx context.Context, req api.ClaimRequest, key, workerDigest string) (*api.Lease, error) {
	if err := c.checkWorkerCredential(workerDigest, req.WorkerID); err != nil {
		return nil, err
	}
	token, err := newLeaseToken()
	if err != nil {
		return nil, err
	}
	result := c.apply(ctx, operation{Kind: "claim", Claim: req, Key: key, Token: token, WorkerDigest: workerDigest, LeaseTime: c.leaseTime})
	return result.lease, result.err
}

func (c *cluster) Renew(ctx context.Context, id, token, workerDigest string) (*api.Lease, error) {
	if err := c.checkWorkerCredential(workerDigest, ""); err != nil {
		return nil, err
	}
	result := c.apply(ctx, operation{Kind: "renew", ID: id, Token: token, WorkerDigest: workerDigest, LeaseTime: c.leaseTime})
	if result.err == nil && result.lease == nil {
		return nil, errStale
	}
	return result.lease, result.err
}

func (c *cluster) Finish(ctx context.Context, id string, req api.FinishRequest, workerDigest string) error {
	if err := c.checkWorkerCredential(workerDigest, ""); err != nil {
		return err
	}
	return c.apply(ctx, operation{Kind: "finish", ID: id, Finish: req, WorkerDigest: workerDigest}).err
}

func (c *cluster) ChangeWorkerCredential(ctx context.Context, req api.WorkerCredentialChange, key string) error {
	if err := validateWorkerCredentialChange(req); err != nil {
		return err
	}
	term := c.node.LeaderTerm()
	err := c.apply(ctx, operation{Kind: "worker_credential", Key: key, Credential: &req}).err
	if err == nil && term != 0 && c.node.LeaderTerm() == term {
		c.markWorkerTermReady(term)
	}
	return err
}

func (c *cluster) Reap(ctx context.Context) error {
	term := c.node.LeaderTerm()
	if term == 0 {
		return nil
	}
	if c.managedWorkers && c.readyWorkerTerm.Load() != term {
		if err := c.Ready(ctx); err != nil {
			return err
		}
		if c.node.LeaderTerm() != term {
			return ha.ErrUnavailable
		}
		c.markWorkerTermReady(term)
	}
	c.view.mu.RLock()
	expired, err := c.view.store.expiredJobs(time.Now().UTC())
	c.view.mu.RUnlock()
	if err != nil || len(expired) == 0 {
		return err
	}
	return c.apply(ctx, operation{Kind: "reap"}).err
}

func (c *cluster) markWorkerTermReady(term uint64) {
	for previous := c.readyWorkerTerm.Load(); previous < term; previous = c.readyWorkerTerm.Load() {
		if c.readyWorkerTerm.CompareAndSwap(previous, term) {
			return
		}
	}
}

// Reject unknown credentials without appending a Raft entry. This check is
// advisory: the FSM checks the credential again in the mutation transaction.
func (c *cluster) checkWorkerCredential(digest, workerID string) error {
	if digest == "" {
		return nil
	}
	if !api.ValidWorkerCredentialDigest(digest) {
		return errWorkerUnauthorized
	}
	term := c.node.LeaderTerm()
	if term == 0 || c.readyWorkerTerm.Load() != term {
		return ha.ErrUnavailable
	}
	c.view.mu.RLock()
	defer c.view.mu.RUnlock()
	if c.view.store == nil {
		return ha.ErrUnavailable
	}
	err := c.view.store.db.View(func(tx *bolt.Tx) error {
		owner, err := credentialOwner(tx, digest)
		if err == nil && workerID != "" && workerID != owner {
			return errWorkerUnauthorized
		}
		return err
	})
	if c.node.LeaderTerm() != term {
		return ha.ErrUnavailable
	}
	return err
}

func (c *cluster) Ready(ctx context.Context) error {
	return c.node.Barrier(ctx)
}

func (c *cluster) Backup(ctx context.Context) (*Backup, error) {
	if err := c.Ready(ctx); err != nil {
		return nil, err
	}
	c.view.mu.RLock()
	defer c.view.mu.RUnlock()
	if c.view.store == nil {
		return nil, ha.ErrUnavailable
	}
	return c.view.store.Backup(ctx)
}

func (c *cluster) apply(ctx context.Context, op operation) applied {
	if !c.node.IsLeader() {
		return applied{err: ha.ErrUnavailable}
	}
	started := time.Now()
	op.At = started.UTC()
	data, err := json.Marshal(op)
	if err != nil {
		return applied{err: err}
	}
	raw, err := c.node.Apply(ctx, data)
	if err != nil {
		return applied{err: err}
	}
	result, ok := raw.(applied)
	if !ok {
		return applied{err: fmt.Errorf("unexpected Raft command result %T", raw)}
	}
	if result.lease != nil {
		result.lease.TTLMillis -= time.Since(started).Milliseconds()
		if result.lease.TTLMillis <= 0 {
			result.lease = nil
		}
	}
	return result
}
