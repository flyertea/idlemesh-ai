package coordinator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
	bolt "go.etcd.io/bbolt"
)

func TestBackupRestorePreservesStateAndFencesOldLeases(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	source, err := openStore(filepath.Join(directory, "source.db"), time.Minute, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer source.db.Close()
	at := time.Now().UTC()
	worker := api.ClaimRequest{WorkerID: "pc", Engine: "echo"}
	retired, active := strings.Repeat("a", 64), strings.Repeat("b", 64)
	for _, digest := range []string{retired, active} {
		if err := source.ChangeWorkerCredential(ctx, api.WorkerCredentialChange{WorkerID: worker.WorkerID, Digest: digest}, digest); err != nil {
			t.Fatal(err)
		}
	}
	submit := api.SubmitRequest{Engine: "echo", Prompt: "save me", MaxTokens: 4}
	job1, lease1 := prepareBackupJob(t, source, submit, worker, active, "one", at, 2)
	job2, lease2 := prepareBackupJob(t, source, submit, worker, active, "two", at, 1)
	job3, lease3 := prepareBackupJob(t, source, submit, worker, active, "three", at, 2)
	finish3 := api.FinishRequest{Token: lease3.Token, Output: "retained result"}
	if err := source.Finish(ctx, job3.ID, finish3, active); err != nil {
		t.Fatal(err)
	}
	archive, err := source.Backup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// This outcome occurs after the snapshot and must not appear in recovery.
	if err := source.Finish(ctx, job1.ID, api.FinishRequest{Token: lease1.Token, Output: "too late"}, active); err != nil {
		archive.Close()
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(archive)
	if err := errors.Join(readErr, archive.Close()); err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) != archive.Size {
		t.Fatalf("archive advertised %d bytes, produced %d", archive.Size, len(data))
	}
	if _, err := os.Stat(archive.file.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup staging file left behind: %v", err)
	}
	archivePath := filepath.Join(directory, "backup.idm")
	if err := os.WriteFile(archivePath, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := CheckBackup(ctx, archivePath); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "restored.db")
	if err := RestoreBackup(ctx, archivePath, destination); err != nil {
		t.Fatal(err)
	}
	restored, err := openStore(destination, time.Minute, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.db.Close()
	for _, test := range []struct {
		id, state, output string
	}{
		{job1.ID, api.Queued, ""},
		{job2.ID, api.Failed, ""},
		{job3.ID, api.Succeeded, "retained result"},
	} {
		job, err := restored.Status(ctx, test.id)
		if err != nil || job.State != test.state || job.Output != test.output || job.Attempt != 1 {
			t.Fatalf("restored job %s: %+v, %v", test.id, job, err)
		}
	}
	if _, err := restored.Renew(ctx, job1.ID, lease1.Token, active); !errors.Is(err, errStale) {
		t.Fatalf("old lease renewed after recovery: %v", err)
	}
	if err := restored.Finish(ctx, job2.ID, api.FinishRequest{Token: lease2.Token}, active); !errors.Is(err, errStale) {
		t.Fatalf("old active lease finished after recovery: %v", err)
	}
	if err := restored.Finish(ctx, job3.ID, finish3, active); !errors.Is(err, errStale) {
		t.Fatalf("old finish receipt accepted after recovery: %v", err)
	}
	if lease, err := restored.Claim(ctx, worker, "claim-one", active); lease != nil || err != nil {
		t.Fatalf("old claim acquired a new lease: %+v, %v", lease, err)
	}
	if job, err := restored.Submit(ctx, submit, "submit-one"); err != nil || job.ID != job1.ID {
		t.Fatalf("submit idempotency was lost: %+v, %v", job, err)
	}
	if err := restored.ChangeWorkerCredential(ctx, api.WorkerCredentialChange{WorkerID: worker.WorkerID, Digest: retired}, "retry-retired"); !errors.Is(err, errWorkerDigestConflict) {
		t.Fatalf("retired credential reservation was lost: %v", err)
	}
	lease, err := restored.Claim(ctx, worker, "claim-after-restore", active)
	if err != nil || lease == nil || lease.ID != job1.ID || lease.Attempt != 2 || lease.Token == lease1.Token {
		t.Fatalf("replacement lease: %+v, %v", lease, err)
	}
}

func TestBackupRejectsCorruptionAndExistingDestination(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	source, err := openStore(filepath.Join(directory, "source.db"), time.Minute, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer source.db.Close()
	archive, err := source.Backup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(archive)
	if err := errors.Join(readErr, archive.Close()); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(directory, "backup.idm")
	if err := os.WriteFile(archivePath, data, 0600); err != nil {
		t.Fatal(err)
	}
	existing := filepath.Join(directory, "existing.db")
	if err := os.WriteFile(existing, []byte("do not overwrite"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := RestoreBackup(ctx, archivePath, existing); err == nil {
		t.Fatal("restore overwrote an existing path")
	}
	if contents, err := os.ReadFile(existing); err != nil || !bytes.Equal(contents, []byte("do not overwrite")) {
		t.Fatalf("existing destination changed: %q, %v", contents, err)
	}

	t.Run("checksum mismatch", func(t *testing.T) {
		corrupted := bytes.Clone(data)
		corrupted[len(corrupted)-1] ^= 0xff
		testInvalidBackup(t, corrupted)
	})
	t.Run("truncated", func(t *testing.T) {
		testInvalidBackup(t, data[:len(data)-1])
	})
	t.Run("unknown version", func(t *testing.T) {
		invalid := bytes.Clone(data)
		invalid[len(backupMagic)-2]++
		testInvalidBackup(t, invalid)
	})
	t.Run("unrelated bbolt schema", func(t *testing.T) {
		db, err := bolt.Open(filepath.Join(t.TempDir(), "unrelated.db"), 0600, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		archive, err := (&store{db: db}).Backup(ctx)
		if err != nil {
			t.Fatal(err)
		}
		unrelated, readErr := io.ReadAll(archive)
		if err := errors.Join(readErr, archive.Close()); err != nil {
			t.Fatal(err)
		}
		sourcePath := filepath.Join(directory, "unrelated.idm")
		destination := filepath.Join(directory, "unrelated-restore.db")
		if err := os.WriteFile(sourcePath, unrelated, 0600); err != nil {
			t.Fatal(err)
		}
		if err := CheckBackup(ctx, sourcePath); err != nil {
			t.Fatalf("unrelated schema still has a valid archive envelope: %v", err)
		}
		if err := RestoreBackup(ctx, sourcePath, destination); err == nil || !strings.Contains(err.Error(), "missing bucket") {
			t.Fatalf("unrelated bbolt database restored: %v", err)
		}
	})
}

func prepareBackupJob(t *testing.T, s *store, req api.SubmitRequest, worker api.ClaimRequest, digest, suffix string, at time.Time, attempts int) (api.Job, *api.Lease) {
	t.Helper()
	job, err := s.submitAt(req, "submit-"+suffix, at, attempts)
	if err != nil {
		t.Fatal(err)
	}
	token, err := newLeaseToken()
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.claimAt(worker, "claim-"+suffix, token, digest, at, time.Minute)
	if err != nil || lease == nil || lease.ID != job.ID {
		t.Fatalf("claim test job: %+v, %v", lease, err)
	}
	return job, lease
}

func testInvalidBackup(t *testing.T, data []byte) {
	t.Helper()
	directory := t.TempDir()
	source := filepath.Join(directory, "invalid.idm")
	destination := filepath.Join(directory, "destination.db")
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := CheckBackup(context.Background(), source); err == nil {
		t.Fatal("invalid archive passed verification")
	}
	if err := RestoreBackup(context.Background(), source, destination); err == nil {
		t.Fatal("invalid archive was restored")
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid archive published a destination: %v", err)
	}
}
