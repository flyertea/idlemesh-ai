package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
	"github.com/flyertea/idlemesh-ai/internal/coordinator"
)

func backupQueue(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	server := addServerFlags(fs)
	output := fs.String("output", "", "write the backup archive to a new file")
	timeout := fs.Duration("timeout", 10*time.Minute, "total backup and verification timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *output == "" || *timeout <= 0 {
		return errors.New("provide --output and a positive --timeout")
	}
	if os.Getenv(adminToken) == "" {
		return errors.New("set IDLEMESH_ADMIN_TOKEN before downloading a backup")
	}
	client, err := server.client(adminToken)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	return writeBackup(ctx, client, *output)
}

func writeBackup(ctx context.Context, client *api.Client, destination string) error {
	if _, err := os.Lstat(destination); err == nil {
		return errors.New("backup destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	file, err := os.CreateTemp(filepath.Dir(destination), ".idlemesh-backup-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()

	body, size, err := client.OpenBackup(ctx)
	if err != nil {
		return err
	}
	copied, err := io.Copy(file, body)
	closeErr := body.Close()
	if err := errors.Join(err, closeErr); err != nil {
		return err
	}
	if copied != size {
		return fmt.Errorf("incomplete backup: received %d of %d bytes", copied, size)
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := coordinator.CheckBackup(ctx, file.Name()); err != nil {
		return fmt.Errorf("verify downloaded backup: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Link(file.Name(), destination); err != nil {
		return fmt.Errorf("publish backup to new destination: %w", err)
	}
	return nil
}

func restoreQueue(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	archive := fs.String("backup", "", "backup archive to restore")
	database := fs.String("db", "", "new standalone database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *archive == "" || *database == "" {
		return errors.New("provide --backup and a new --db destination")
	}
	return coordinator.RestoreBackup(ctx, *archive, *database)
}
