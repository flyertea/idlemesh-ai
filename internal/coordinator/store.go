package coordinator

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
	bolt "go.etcd.io/bbolt"
)

const maxActiveJobs = 1024

var (
	jobsBucket     = []byte("jobs-v1")
	activeBucket   = []byte("active-v1")
	submitKeys     = []byte("submit-keys-v1")
	claimKeys      = []byte("claim-keys-v1")
	errNotFound    = errors.New("job not found")
	errStale       = errors.New("lease is expired or no longer current")
	errQueueFull   = errors.New("queue is full")
	errNoWork      = errors.New("no matching work")
	errKeyConflict = errors.New("idempotency key was used with different input")
)

var stateBuckets = [][]byte{jobsBucket, activeBucket, submitKeys, claimKeys}

type record struct {
	api.Job
	Prompt       string    `json:"prompt,omitempty"`
	MaxTokens    int       `json:"max_tokens"`
	Token        string    `json:"token,omitempty"`
	LastFinished string    `json:"last_finished,omitempty"`
	DueAt        time.Time `json:"due_at"`
}

type store struct {
	db          *bolt.DB
	leaseTime   time.Duration
	maxAttempts int
}

func openStore(path string, leaseTime time.Duration, maxAttempts int) (*store, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range stateBuckets {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &store{db: db, leaseTime: leaseTime, maxAttempts: maxAttempts}, nil
}

type submitted struct {
	Key  []byte   `json:"key"`
	Hash [32]byte `json:"hash"`
}

type claimed struct {
	Key     []byte           `json:"key"`
	Token   string           `json:"token"`
	Request api.ClaimRequest `json:"request"`
}

func (s *store) submit(req api.SubmitRequest) (api.Job, error) {
	return s.submitAt(req, "", time.Now().UTC(), s.maxAttempts)
}

func (s *store) submitAt(req api.SubmitRequest, requestKey string, now time.Time, maxAttempts int) (api.Job, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return api.Job{}, err
	}
	hash := sha256.Sum256(data)
	var job api.Job
	err = s.db.Update(func(tx *bolt.Tx) error {
		keys := tx.Bucket(submitKeys)
		if requestKey != "" {
			if value := keys.Get([]byte(requestKey)); value != nil {
				var saved submitted
				if err := json.Unmarshal(value, &saved); err != nil {
					return err
				}
				if saved.Hash != hash {
					return errKeyConflict
				}
				r, err := load(tx, saved.Key)
				job = r.Job
				return err
			}
		}
		active := tx.Bucket(activeBucket)
		if active.Stats().KeyN >= maxActiveJobs {
			return errQueueFull
		}

		seq, err := tx.Bucket(jobsBucket).NextSequence()
		if err != nil {
			return err
		}
		job = api.Job{
			ID: strconv.FormatUint(seq, 16), State: api.Queued,
			Engine: req.Engine, Model: req.Model, MaxAttempts: maxAttempts,
			CreatedAt: now, UpdatedAt: now,
		}
		key := binary.BigEndian.AppendUint64(nil, seq)
		if err := put(tx, key, record{Job: job, Prompt: req.Prompt, MaxTokens: req.MaxTokens}); err != nil {
			return err
		}
		if requestKey != "" {
			if err := putJSON(keys, []byte(requestKey), submitted{Key: key, Hash: hash}); err != nil {
				return err
			}
		}
		return active.Put(key, nil)
	})
	return job, err
}

func (s *store) status(id string) (api.Job, error) {
	key, err := jobKey(id)
	if err != nil {
		return api.Job{}, err
	}
	var job api.Job
	err = s.db.View(func(tx *bolt.Tx) error {
		r, err := load(tx, key)
		job = r.Job
		return err
	})
	return job, err
}

func newLeaseToken() (string, error) {
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(random[:]), nil
}

func (s *store) claim(req api.ClaimRequest) (*api.Lease, error) {
	token, err := newLeaseToken()
	if err != nil {
		return nil, err
	}
	return s.claimAt(req, "", token, time.Now().UTC(), s.leaseTime)
}

func (s *store) claimAt(req api.ClaimRequest, requestKey, token string, now time.Time, leaseTime time.Duration) (*api.Lease, error) {
	var lease *api.Lease
	err := s.db.Update(func(tx *bolt.Tx) error {
		keys := tx.Bucket(claimKeys)
		if requestKey != "" {
			if value := keys.Get([]byte(requestKey)); value != nil {
				var saved claimed
				if err := json.Unmarshal(value, &saved); err != nil {
					return err
				}
				if saved.Request != req {
					return errKeyConflict
				}
				r, err := load(tx, saved.Key)
				if err != nil {
					return err
				}
				if r.validLease(saved.Token, now) {
					lease = r.lease(now)
				}
				return errNoWork // An idempotent replay does not need a write.
			}
		}

		cursor := tx.Bucket(activeBucket).Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			r, err := load(tx, key)
			if err != nil {
				return err
			}
			if r.Engine != req.Engine || r.Model != req.Model ||
				now.Before(r.DueAt) || r.Attempt >= r.MaxAttempts {
				continue
			}

			r.Token = token
			r.State = api.Running
			r.Attempt++
			r.WorkerID = req.WorkerID
			r.Error = ""
			r.UpdatedAt = maxTime(r.UpdatedAt, now)
			r.DueAt = now.Add(leaseTime)
			lease = r.lease(now)
			if requestKey != "" {
				if err := putJSON(keys, []byte(requestKey), claimed{Key: key, Token: token, Request: req}); err != nil {
					return err
				}
			}
			return put(tx, key, r)
		}
		return errNoWork
	})
	if errors.Is(err, errNoWork) {
		return lease, nil
	}
	return lease, err
}

func (r record) lease(now time.Time) *api.Lease {
	return &api.Lease{
		ID: r.ID, Token: r.Token, Prompt: r.Prompt, Engine: r.Engine, Model: r.Model,
		MaxTokens: r.MaxTokens, Attempt: r.Attempt, TTLMillis: r.DueAt.Sub(now).Milliseconds(),
	}
}

func (s *store) renew(id, token string) (*api.Lease, error) {
	return s.renewAt(id, token, time.Now().UTC(), s.leaseTime)
}

func (s *store) renewAt(id, token string, now time.Time, leaseTime time.Duration) (*api.Lease, error) {
	key, err := jobKey(id)
	if err != nil {
		return nil, err
	}
	var ttl time.Duration
	err = s.db.Update(func(tx *bolt.Tx) error {
		r, err := load(tx, key)
		if err != nil {
			return err
		}
		if !r.validLease(token, now) {
			return errStale
		}
		r.DueAt = maxTime(r.DueAt, now.Add(leaseTime))
		ttl = min(leaseTime, r.DueAt.Sub(now))
		return put(tx, key, r)
	})
	if err != nil {
		return nil, err
	}
	return &api.Lease{ID: id, TTLMillis: ttl.Milliseconds()}, nil
}

func (s *store) finish(id string, req api.FinishRequest) error {
	return s.finishAt(id, req, time.Now().UTC())
}

func (s *store) finishAt(id string, req api.FinishRequest, now time.Time) error {
	key, err := jobKey(id)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		r, err := load(tx, key)
		if err != nil {
			return err
		}
		if equalToken(r.LastFinished, req.Token) {
			return nil
		}
		if !r.validLease(req.Token, now) {
			return errStale
		}
		r.LastFinished = r.Token
		r.Token = ""
		r.UpdatedAt = maxTime(r.UpdatedAt, now)
		if req.Error == "" {
			r.State = api.Succeeded
			r.Output = req.Output
			return terminal(tx, key, r)
		}
		r.Error = req.Error
		if r.Attempt >= r.MaxAttempts {
			r.State = api.Failed
			return terminal(tx, key, r)
		}
		r.State = api.Queued
		r.DueAt = now.Add(time.Second)
		return put(tx, key, r)
	})
}

// Reflect expired leases in job status even when no worker is available.
func (s *store) reap() error {
	return s.reapAt(time.Now().UTC())
}

func (s *store) expiredJobs(now time.Time) ([][]byte, error) {
	var expired [][]byte
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(activeBucket).ForEach(func(key, _ []byte) error {
			r, err := load(tx, key)
			if err == nil && r.expired(now) {
				expired = append(expired, append([]byte(nil), key...))
			}
			return err
		})
	})
	return expired, err
}

func (s *store) reapAt(now time.Time) error {
	expired, err := s.expiredJobs(now)
	if err != nil || len(expired) == 0 {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, key := range expired {
			r, err := load(tx, key)
			if err != nil {
				return err
			}
			if r.expired(now) {
				r.Error = "worker lease expired before a result was received"
				r.UpdatedAt = maxTime(r.UpdatedAt, now)
				if r.Attempt >= r.MaxAttempts {
					r.State = api.Failed
					if err := terminal(tx, key, r); err != nil {
						return err
					}
					continue
				}
				r.State = api.Queued
				r.Token = ""
				r.DueAt = time.Time{}
				if err := put(tx, key, r); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (r record) expired(now time.Time) bool {
	return r.State == api.Running && !now.Before(r.DueAt)
}

func (r record) validLease(token string, now time.Time) bool {
	return r.State == api.Running && now.Before(r.DueAt) && equalToken(r.Token, token)
}

func equalToken(expected, token string) bool {
	return expected != "" && subtle.ConstantTimeCompare([]byte(expected), []byte(token)) == 1
}

func jobKey(id string) ([]byte, error) {
	seq, err := strconv.ParseUint(id, 16, 64)
	if err != nil || seq == 0 || strconv.FormatUint(seq, 16) != id {
		return nil, errNotFound
	}
	return binary.BigEndian.AppendUint64(nil, seq), nil
}

func load(tx *bolt.Tx, key []byte) (record, error) {
	var r record
	data := tx.Bucket(jobsBucket).Get(key)
	if data == nil {
		return r, errNotFound
	}
	return r, json.Unmarshal(data, &r)
}

func put(tx *bolt.Tx, key []byte, r record) error {
	return putJSON(tx.Bucket(jobsBucket), key, r)
}

func putJSON(bucket *bolt.Bucket, key []byte, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put(key, data)
}

func maxTime(first, second time.Time) time.Time {
	if first.After(second) {
		return first
	}
	return second
}

func terminal(tx *bolt.Tx, key []byte, r record) error {
	r.Prompt = ""
	r.Token = ""
	r.DueAt = time.Time{}
	if err := put(tx, key, r); err != nil {
		return err
	}
	return tx.Bucket(activeBucket).Delete(key)
}
