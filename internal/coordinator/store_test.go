package coordinator

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
	bolt "go.etcd.io/bbolt"
)

func TestReaperMarksFinalAttemptFailed(t *testing.T) {
	s, err := openStore(filepath.Join(t.TempDir(), "jobs.db"), time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	job, err := s.submit(api.SubmitRequest{Engine: "echo", Prompt: "test", MaxTokens: 4})
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := s.claim(api.ClaimRequest{WorkerID: "pc", Engine: "echo"}); lease == nil || err != nil {
		t.Fatalf("claim: %+v, %v", lease, err)
	}
	expireLease(t, s, job.ID)
	if err := s.reap(); err != nil {
		t.Fatal(err)
	}
	done, err := s.status(job.ID)
	if err != nil || done.State != api.Failed || done.Attempt != 1 || done.Error == "" {
		t.Fatalf("failed job: %+v, %v", done, err)
	}
}

func expireLease(t *testing.T, s *store, id string) {
	t.Helper()
	key, err := jobKey(id)
	if err != nil {
		t.Fatal(err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		r, err := load(tx, key)
		if err != nil {
			return err
		}
		r.DueAt = time.Now().Add(-time.Second)
		return put(tx, key, r)
	})
	if err != nil {
		t.Fatal(err)
	}
}
