package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

// projection holds a disposable bbolt view of the replicated log. Raft opens
// its locked durable store before invoking any FSM method; the view is rebuilt
// from snapshots and log entries on every process start.
type projection struct {
	mu        sync.RWMutex
	directory string
	path      string
	store     *store
}

func (p *projection) initialize() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store != nil {
		return nil
	}
	if err := os.MkdirAll(p.directory, 0700); err != nil {
		return err
	}
	for _, pattern := range []string{"view-*.db", "snapshot-*.db"} {
		stale, err := filepath.Glob(filepath.Join(p.directory, pattern))
		if err != nil {
			return err
		}
		for _, path := range stale {
			// A previous instance may still be closing its view on Windows.
			os.Remove(path)
		}
	}
	s, path, err := p.build(nil)
	if err == nil {
		p.store, p.path = s, path
	}
	return err
}

func (p *projection) build(snapshot io.Reader) (*store, string, error) {
	file, err := os.CreateTemp(p.directory, "view-*.db")
	if err != nil {
		return nil, "", err
	}
	path := file.Name()
	var copyErr error
	if snapshot != nil {
		var size int64
		size, copyErr = io.Copy(file, snapshot)
		if copyErr == nil && size == 0 {
			copyErr = errors.New("empty coordinator snapshot")
		}
	}
	if err := errors.Join(copyErr, file.Close()); err != nil {
		os.Remove(path)
		return nil, "", err
	}
	var s *store
	if snapshot == nil {
		s, err = openStore(path, 0, 0)
	} else {
		var db *bolt.DB
		db, err = bolt.Open(path, 0600, nil)
		if err == nil {
			err = db.View(func(tx *bolt.Tx) error {
				for _, bucket := range stateBuckets {
					if tx.Bucket(bucket) == nil {
						return fmt.Errorf("coordinator snapshot is missing bucket %q", bucket)
					}
				}
				return nil
			})
			if err != nil {
				db.Close()
			} else {
				s = &store{db: db}
			}
		}
	}
	if err != nil {
		os.Remove(path)
		return nil, "", err
	}
	return s, path, nil
}

func (p *projection) Apply(log *raft.Log) any {
	var op operation
	if err := json.Unmarshal(log.Data, &op); err != nil {
		panic(fmt.Errorf("invalid committed coordinator operation: %w", err))
	}
	if err := p.initialize(); err != nil {
		panic(fmt.Errorf("initialize committed coordinator state: %w", err))
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	s := p.store
	var result applied
	switch op.Kind {
	case "submit":
		result.job, result.err = s.submitAt(op.Submit, op.Key, op.At, op.MaxAttempts)
	case "claim":
		result.lease, result.err = s.claimAt(op.Claim, op.Key, op.Token, op.At, op.LeaseTime)
	case "renew":
		result.lease, result.err = s.renewAt(op.ID, op.Token, op.At, op.LeaseTime)
	case "finish":
		result.err = s.finishAt(op.ID, op.Finish, op.At)
	case "reap":
		result.err = s.reapAt(op.At)
	default:
		panic(fmt.Errorf("unknown committed coordinator operation %q", op.Kind))
	}
	switch result.err {
	case nil, errNotFound, errStale, errQueueFull, errKeyConflict:
		return result
	default:
		// Raft has committed this operation. A local storage failure must
		// stop this replica so it cannot continue from a divergent view.
		panic(fmt.Errorf("apply committed coordinator operation: %w", result.err))
	}
}

func (p *projection) Snapshot() (raft.FSMSnapshot, error) {
	if err := p.initialize(); err != nil {
		return nil, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Finish the read transaction before returning to Raft. Snapshot metadata
	// is fetched through the Raft loop, which may be busy installing a new
	// snapshot and waiting for this database to close.
	file, err := os.CreateTemp(p.directory, "snapshot-*.db")
	if err != nil {
		return nil, err
	}
	err = p.store.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(file)
		return err
	})
	if err := errors.Join(err, file.Close()); err != nil {
		os.Remove(file.Name())
		return nil, err
	}
	return &projectionSnapshot{path: file.Name()}, nil
}

func (p *projection) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	if err := p.initialize(); err != nil {
		return err
	}
	s, path, err := p.build(snapshot)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	oldStore, oldPath := p.store, p.path
	p.store, p.path = s, path
	oldStore.db.Close()
	os.Remove(oldPath)
	return nil
}

func (p *projection) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store == nil {
		return nil
	}
	err := p.store.db.Close()
	p.store = nil
	removeErr := os.Remove(p.path)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	return errors.Join(err, removeErr)
}

type projectionSnapshot struct{ path string }

func (s *projectionSnapshot) Persist(sink raft.SnapshotSink) error {
	file, err := os.Open(s.path)
	if err != nil {
		sink.Cancel()
		return err
	}
	_, err = io.Copy(sink, file)
	if err := errors.Join(err, file.Close()); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *projectionSnapshot) Release() { os.Remove(s.path) }
