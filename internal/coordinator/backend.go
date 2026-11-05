package coordinator

import (
	"context"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

type backend interface {
	Submit(context.Context, api.SubmitRequest, string) (api.Job, error)
	Status(context.Context, string) (api.Job, error)
	Claim(ctx context.Context, req api.ClaimRequest, key, workerDigest string) (*api.Lease, error)
	Renew(ctx context.Context, id, token, workerDigest string) (*api.Lease, error)
	Finish(ctx context.Context, id string, req api.FinishRequest, workerDigest string) error
	ChangeWorkerCredential(ctx context.Context, req api.WorkerCredentialChange, key string) error
	Backup(context.Context) (*Backup, error)
	Reap(context.Context) error
	Ready(context.Context) error
}

func (s *store) Submit(_ context.Context, req api.SubmitRequest, key string) (api.Job, error) {
	return s.submitAt(req, key, time.Now().UTC(), s.maxAttempts)
}

func (s *store) Status(_ context.Context, id string) (api.Job, error) {
	return s.status(id)
}

func (s *store) Claim(_ context.Context, req api.ClaimRequest, key, workerDigest string) (*api.Lease, error) {
	token, err := newLeaseToken()
	if err != nil {
		return nil, err
	}
	return s.claimAt(req, key, token, workerDigest, time.Now().UTC(), s.leaseTime)
}

func (s *store) Renew(_ context.Context, id, token, workerDigest string) (*api.Lease, error) {
	return s.renewAt(id, token, workerDigest, time.Now().UTC(), s.leaseTime)
}

func (s *store) Finish(_ context.Context, id string, req api.FinishRequest, workerDigest string) error {
	return s.finishAt(id, req, workerDigest, time.Now().UTC())
}

func (s *store) ChangeWorkerCredential(_ context.Context, req api.WorkerCredentialChange, key string) error {
	return s.changeWorkerCredentialAt(req, key, time.Now().UTC())
}

func (s *store) Reap(_ context.Context) error {
	return s.reap()
}

func (s *store) Ready(context.Context) error { return nil }
