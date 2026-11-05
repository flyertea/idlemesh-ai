package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

func manageWorkerToken(ctx context.Context, args []string) error {
	const usage = "usage: idlemesh worker-token issue|revoke --id WORKER_ID [--server URL ...]"
	if len(args) == 0 {
		return errors.New(usage)
	}
	action := args[0]
	switch action {
	case "help", "-h", "--help":
		fmt.Fprintln(os.Stdout, usage)
		return nil
	case "issue", "revoke":
	default:
		return errors.New(usage)
	}
	fs := flag.NewFlagSet("worker-token "+action, flag.ContinueOnError)
	server := addServerFlags(fs)
	id := fs.String("id", "", "worker ID to bind to the credential")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || !api.ValidWorkerID(*id) {
		return errors.New("provide a valid --id")
	}
	if os.Getenv(adminToken) == "" {
		return errors.New("set IDLEMESH_ADMIN_TOKEN before managing worker credentials")
	}
	client, err := server.client(adminToken)
	if err != nil {
		return err
	}
	change := api.WorkerCredentialChange{WorkerID: *id}
	var token string
	if action == "issue" {
		token, change.Digest, err = api.NewWorkerCredential()
		if err != nil {
			return err
		}
	}
	if err := client.ChangeWorkerCredential(ctx, change); err != nil {
		return err
	}
	if token != "" {
		_, err := fmt.Fprintln(os.Stdout, token)
		return err
	}
	return nil
}
