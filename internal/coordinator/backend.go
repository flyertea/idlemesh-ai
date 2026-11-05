package coordinator

import (
	"context"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

type backend interface {
	Submit(context.Context, api.SubmitRequest, string) (api.Job, error)
	Status(context.Context, string) (api.Job, error)
	Claim(context.Context, api.ClaimRequest, string) (*api.Lease, error)
	Renew(context.Context, string, string) (*api.Lease, error)
	Finish(context.Context, string, api.FinishRequest) error
	Reap(context.Context) error
	Ready(context.Context) error
}

func (s *store) Submit(_ context.Context, req api.SubmitRequest, key string) (api.Job, error) {
	return s.submitAt(req, key, time.Now().UTC(), s.maxAttempts)
}

func (s *store) Status(_ context.Context, id string) (api.Job, error) {
	return s.status(id)
}

func (s *store) Claim(_ context.Context, req api.ClaimRequest, key string) (*api.Lease, error) {
	token, err := newLeaseToken()
	if err != nil {
		return nil, err
	}
	return s.claimAt(req, key, token, time.Now().UTC(), s.leaseTime)
}

func (s *store) Renew(_ context.Context, id, token string) (*api.Lease, error) {
	return s.renew(id, token)
}

func (s *store) Finish(_ context.Context, id string, req api.FinishRequest) error {
	return s.finish(id, req)
}

func (s *store) Reap(_ context.Context) error {
	return s.reap()
}

func (s *store) Ready(context.Context) error { return nil }
