package coordinator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/flyertea/idlemesh-ai/internal/api"
	"go.etcd.io/bbolt"
)

const (
	backupMagic      = "IDLEMESH-BACKUP\x01\n"
	backupLengthEnd  = len(backupMagic) + 8
	backupHeaderSize = backupLengthEnd + sha256.Size
	maxBackupPayload = 8 << 30
)

// MaxBackupBytes is the maximum archive size including the framing header.
const MaxBackupBytes int64 = maxBackupPayload + int64(backupHeaderSize)

// Backup is a staged, versioned archive. Close removes the staging file.
type Backup struct {
	io.Reader
	Size int64
	file *os.File
}

func (b *Backup) Close() error {
	return errors.Join(b.file.Close(), os.Remove(b.file.Name()))
}

func (s *store) Backup(ctx context.Context) (*Backup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(filepath.Dir(s.db.Path()), ".idlemesh-backup-*")
	if err != nil {
		return nil, fmt.Errorf("stage backup: %w", err)
	}
	complete := false
	defer func() {
		if !complete {
			file.Close()
			os.Remove(file.Name())
		}
	}()

	hash := sha256.New()
	writer := contextWriter{ctx: ctx, writer: io.MultiWriter(file, hash)}
	var size int64
	if err := s.db.View(func(tx *bbolt.Tx) error {
		if tx.Size() > maxBackupPayload {
			return errors.New("coordinator snapshot exceeds the 8 GiB backup payload limit")
		}
		var err error
		size, err = tx.WriteTo(writer)
		return err
	}); err != nil {
		return nil, fmt.Errorf("copy coordinator snapshot: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind backup: %w", err)
	}
	header := make([]byte, backupHeaderSize)
	copy(header, backupMagic)
	binary.BigEndian.PutUint64(header[len(backupMagic):backupLengthEnd], uint64(size))
	copy(header[backupLengthEnd:], hash.Sum(nil))

	complete = true
	return &Backup{Reader: io.MultiReader(bytes.NewReader(header), file), Size: size + int64(backupHeaderSize), file: file}, nil
}

// PreparedBackup reads a validated bbolt snapshot with prior leases fenced.
// Close also removes the staging file.
type PreparedBackup struct{ *os.File }

func (b *PreparedBackup) Close() error {
	return errors.Join(b.File.Close(), os.Remove(b.Name()))
}

// PrepareBackup creates a sanitized snapshot for seeding a new coordinator.
func PrepareBackup(ctx context.Context, sourcePath string) (*PreparedBackup, error) {
	path, err := prepareBackup(ctx, sourcePath, os.TempDir())
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	return &PreparedBackup{File: file}, nil
}

// CheckBackup checks the archive framing and checksum without opening bbolt.
func CheckBackup(ctx context.Context, sourcePath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	archive, err := openBackupArchive(sourcePath)
	if err != nil {
		return err
	}
	defer archive.Close()
	return archive.copyPayload(ctx, io.Discard)
}

// RestoreBackup recovers a standalone queue into a new database path. The
// caller must stop the old service before directing workers and clients here.
func RestoreBackup(ctx context.Context, sourcePath, destinationDB string) error {
	if sourcePath == "" || destinationDB == "" {
		return errors.New("backup source and new database path are required")
	}
	if _, err := os.Lstat(destinationDB); err == nil {
		return fmt.Errorf("destination database already exists: %s", destinationDB)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	path, err := prepareBackup(ctx, sourcePath, filepath.Dir(destinationDB))
	if err != nil {
		return err
	}
	defer os.Remove(path)
	if err := ctx.Err(); err != nil {
		return err
	}
	// Publishing a hard link is atomic and fails if another process created
	// the destination. The staging file lives on the same filesystem.
	if err := os.Link(path, destinationDB); err != nil {
		return fmt.Errorf("publish restored database without replacing a file: %w", err)
	}
	return nil
}

func prepareBackup(ctx context.Context, sourcePath, stagingDirectory string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	archive, err := openBackupArchive(sourcePath)
	if err != nil {
		return "", err
	}
	defer archive.Close()

	staged, err := os.CreateTemp(stagingDirectory, ".idlemesh-restore-*")
	if err != nil {
		return "", fmt.Errorf("stage restored database: %w", err)
	}
	path := staged.Name()
	complete := false
	defer func() {
		if !complete {
			os.Remove(path)
		}
	}()
	err = archive.copyPayload(ctx, staged)
	if err == nil {
		err = staged.Sync()
	}
	if err := errors.Join(err, staged.Close()); err != nil {
		return "", err
	}
	s, err := openSnapshotStore(path)
	if err != nil {
		return "", fmt.Errorf("open restored coordinator snapshot: %w", err)
	}
	if err := errors.Join(s.recoverLeases(ctx), s.db.Close()); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	complete = true
	return path, nil
}

type backupArchive struct {
	*os.File
	payloadSize int64
	checksum    [sha256.Size]byte
}

func openBackupArchive(path string) (*backupArchive, error) {
	source, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	complete := false
	defer func() {
		if !complete {
			source.Close()
		}
	}()
	info, err := source.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("backup source must be a regular file")
	}
	if info.Size() > MaxBackupBytes {
		return nil, errors.New("backup payload exceeds the 8 GiB limit")
	}
	var header [backupHeaderSize]byte
	if _, err := io.ReadFull(source, header[:]); err != nil {
		return nil, fmt.Errorf("read backup header: %w", err)
	}
	if string(header[:len(backupMagic)]) != backupMagic {
		return nil, errors.New("unsupported IdleMesh backup format")
	}
	size := info.Size() - int64(backupHeaderSize)
	if size <= 0 || binary.BigEndian.Uint64(header[len(backupMagic):backupLengthEnd]) != uint64(size) {
		return nil, errors.New("backup length does not match its header")
	}
	archive := &backupArchive{File: source, payloadSize: size}
	copy(archive.checksum[:], header[backupLengthEnd:])
	complete = true
	return archive, nil
}

func (a *backupArchive) copyPayload(ctx context.Context, destination io.Writer) error {
	hash := sha256.New()
	writer := contextWriter{ctx: ctx, writer: io.MultiWriter(destination, hash)}
	if _, err := io.CopyN(writer, a.File, a.payloadSize); err != nil {
		return fmt.Errorf("read backup payload: %w", err)
	}
	var trailing [1]byte
	if n, err := a.Read(trailing[:]); n != 0 || err != io.EOF {
		return errors.New("backup has unexpected data after its payload")
	}
	if !bytes.Equal(a.checksum[:], hash.Sum(nil)) {
		return errors.New("backup checksum does not match its payload")
	}
	return ctx.Err()
}

func openSnapshotStore(path string) (*store, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, bucket := range stateBuckets {
			if tx.Bucket(bucket) == nil {
				return fmt.Errorf("coordinator snapshot is missing bucket %q", bucket)
			}
		}
		return ensureCredentialBuckets(tx)
	})
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &store{db: db}, nil
}

func (s *store) recoverLeases(ctx context.Context) error {
	if err := s.db.View(func(tx *bbolt.Tx) error {
		var first error
		for err := range tx.Check() {
			if first == nil {
				first = err
			}
		}
		return first
	}); err != nil {
		return fmt.Errorf("check backup database: %w", err)
	}
	if err := s.recoverBucket(ctx, jobsBucket, func(tx *bbolt.Tx, key []byte) error {
		r, err := load(tx, key)
		if err != nil {
			return err
		}
		hadToken := r.Token != "" || r.LastFinished != "" || r.LastFinishedWorker != ""
		r.LastFinished, r.LastFinishedWorker = "", ""
		if r.State == api.Running {
			// Independent voters seeding the same archive need identical state,
			// including timestamps. Preserve the original job timestamp.
			return stopRunningLease(tx, key, r, r.UpdatedAt, "worker lease invalidated by recovery")
		}
		if hadToken {
			r.Token = ""
			return put(tx, key, r)
		}
		return nil
	}); err != nil {
		return err
	}
	// Keep the original claim keys as no-work receipts so a retry can never
	// recover an old token or turn an old claim into a new lease.
	return s.recoverBucket(ctx, claimKeys, func(tx *bbolt.Tx, key []byte) error {
		bucket := tx.Bucket(claimKeys)
		var receipt claimed
		if err := json.Unmarshal(bucket.Get(key), &receipt); err != nil {
			return err
		}
		if receipt.Token == "" {
			return nil
		}
		receipt.Token = ""
		return putJSON(bucket, key, receipt)
	})
}

// The restore file is private until all batches finish. Batching avoids
// keeping changes to the entire job history in one write transaction.
func (s *store) recoverBucket(ctx context.Context, name []byte, update func(*bbolt.Tx, []byte) error) error {
	const batchSize = 128
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		keys := make([][]byte, 0, batchSize)
		if err := s.db.View(func(tx *bbolt.Tx) error {
			cursor := tx.Bucket(name).Cursor()
			key, _ := cursor.First()
			if after != nil {
				key, _ = cursor.Seek(after)
				if bytes.Equal(key, after) {
					key, _ = cursor.Next()
				}
			}
			for ; key != nil && len(keys) < batchSize; key, _ = cursor.Next() {
				keys = append(keys, bytes.Clone(key))
			}
			return nil
		}); err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if err := s.db.Update(func(tx *bbolt.Tx) error {
			for _, key := range keys {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := update(tx, key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("recover bucket %s: %w", name, err)
		}
		after = keys[len(keys)-1]
	}
}

type contextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w contextWriter) Write(data []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.writer.Write(data)
}
