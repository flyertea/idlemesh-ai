package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
	"github.com/flyertea/idlemesh-ai/internal/coordinator"
	"github.com/flyertea/idlemesh-ai/internal/worker"
)

const (
	defaultListen = "127.0.0.1:8787"
	defaultServer = "http://" + defaultListen
	clientToken   = "IDLEMESH_CLIENT_TOKEN"
	workerToken   = "IDLEMESH_WORKER_TOKEN"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := run(ctx, os.Args[1:])
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "idlemesh:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		usage(os.Stderr)
		return errors.New("choose a command")
	}
	switch args[0] {
	case "help", "-h", "--help":
		usage(os.Stdout)
		return nil
	case "version", "--version":
		if len(args) != 1 {
			return errors.New("version does not accept arguments")
		}
		fmt.Fprintln(os.Stdout, version)
		return nil
	case "coordinator":
		return runCoordinator(ctx, args[1:])
	case "worker":
		return runWorker(ctx, args[1:])
	case "submit":
		return submit(ctx, args[1:])
	case "status":
		return inspect(ctx, "status", args[1:])
	case "wait":
		return inspect(ctx, "wait", args[1:])
	default:
		return fmt.Errorf("unknown command %q (see idlemesh --help)", args[0])
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, `Usage: idlemesh <command> [options]

  coordinator   Serve a durable inference queue
  worker        Pull jobs and run local inference
  submit        Submit a prompt and print its job ID
  status        Print a job's status as JSON
  wait          Wait for a job and print its final output
  version       Print the build version

Run idlemesh <command> --help for options.`)
}

func runCoordinator(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	var cfg coordinator.Config
	fs.StringVar(&cfg.Listen, "listen", defaultListen, "TCP listen address")
	fs.StringVar(&cfg.Database, "db", "idlemesh.db", "path to the local job database")
	fs.DurationVar(&cfg.LeaseTime, "lease", 15*time.Second, "duration of a job lease")
	fs.IntVar(&cfg.MaxAttempts, "max-attempts", 3, "maximum execution attempts per job")
	fs.StringVar(&cfg.TLSCert, "tls-cert", "", "PEM TLS certificate file")
	fs.StringVar(&cfg.TLSKey, "tls-key", "", "PEM TLS private key file")
	fs.BoolVar(&cfg.AllowInsecureHTTP, "allow-insecure-http", false, "allow plaintext on a private encrypted network")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("coordinator does not accept positional arguments")
	}
	cfg.ClientToken = os.Getenv(clientToken)
	cfg.WorkerToken = os.Getenv(workerToken)
	cfg.Logger = log.New(os.Stderr, "", log.LstdFlags)
	return coordinator.Run(ctx, cfg)
}

func runWorker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	server := addServerFlags(fs)
	var cfg worker.Config
	fs.StringVar(&cfg.ID, "id", "", "unique worker ID")
	fs.StringVar(&cfg.Engine, "engine", "ollama", "inference engine: ollama or echo")
	fs.StringVar(&cfg.Model, "model", "", "exact locally installed Ollama model tag")
	fs.DurationVar(&cfg.PollInterval, "poll", time.Second, "poll interval when no work is available")
	fs.DurationVar(&cfg.JobTimeout, "job-timeout", 5*time.Minute, "maximum time for one inference attempt")
	ollamaURL := fs.String("ollama-url", "http://127.0.0.1:11434", "local Ollama HTTP origin")
	threads := fs.Int("threads", min(2, runtime.NumCPU()), "Ollama inference CPU threads")
	contextLength := fs.Int("context", 2048, "Ollama context length in tokens")
	echoDelay := fs.Duration("echo-delay", 0, "execution delay for the echo engine")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || !api.ValidWorkerID(cfg.ID) || !api.ValidCapability(cfg.Engine, cfg.Model) {
		return errors.New("provide a valid --id and engine; Ollama also requires --model")
	}
	if cfg.PollInterval <= 0 || cfg.JobTimeout <= 0 || *echoDelay < 0 {
		return errors.New("--poll and --job-timeout must be positive and --echo-delay must be nonnegative")
	}
	client, err := server.client(workerToken)
	if err != nil {
		return err
	}
	runner := worker.Echo(*echoDelay)
	if cfg.Engine == "ollama" {
		runner, err = worker.Ollama(*ollamaURL, cfg.Model, *threads, *contextLength)
		if err != nil {
			return err
		}
	}
	cfg.Logger = log.New(os.Stderr, "", log.LstdFlags)
	cfg.Logger.Printf("worker %s ready (%s %s)", cfg.ID, cfg.Engine, cfg.Model)
	return worker.Run(ctx, client, runner, cfg)
}

type serverFlags struct {
	url           string
	allowInsecure bool
}

func addServerFlags(fs *flag.FlagSet) *serverFlags {
	s := new(serverFlags)
	fs.StringVar(&s.url, "server", defaultServer, "coordinator HTTP(S) origin")
	fs.BoolVar(&s.allowInsecure, "allow-insecure-http", false, "allow non-loopback plaintext on a private encrypted network")
	return s
}

func (s *serverFlags) client(env string) (*api.Client, error) {
	return api.NewClient(s.url, os.Getenv(env), s.allowInsecure)
}

func submit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	server := addServerFlags(fs)
	var req api.SubmitRequest
	fs.StringVar(&req.Engine, "engine", "ollama", "inference engine: ollama or echo")
	fs.StringVar(&req.Model, "model", "", "exact Ollama model tag")
	fs.StringVar(&req.Prompt, "prompt", "", "text to send to the model")
	promptFile := fs.String("prompt-file", "", "read the prompt from a UTF-8 file or from stdin with '-'")
	fs.IntVar(&req.MaxTokens, "max-tokens", 256, "maximum generated tokens (1–4096)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || !api.ValidCapability(req.Engine, req.Model) || req.MaxTokens < 1 || req.MaxTokens > 4096 {
		return errors.New("invalid engine, model, or max-tokens")
	}
	if *promptFile != "" {
		if req.Prompt != "" {
			return errors.New("choose either --prompt or --prompt-file")
		}
		var err error
		req.Prompt, err = readPrompt(*promptFile)
		if err != nil {
			return err
		}
	}
	if !api.ValidPrompt(req.Prompt) {
		return fmt.Errorf("provide a nonempty UTF-8 prompt of at most %d bytes", api.MaxPromptBytes)
	}
	client, err := server.client(clientToken)
	if err != nil {
		return err
	}
	job, err := client.Submit(ctx, req)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, job.ID)
	return nil
}

func readPrompt(path string) (string, error) {
	f := os.Stdin
	if path != "-" {
		file, err := os.Open(path)
		if err != nil {
			return "", err
		}
		defer file.Close()
		f = file
	}
	data, err := io.ReadAll(io.LimitReader(f, api.MaxPromptBytes+1))
	return string(data), err
}

func inspect(ctx context.Context, command string, args []string) error {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	server := addServerFlags(fs)
	var timeout time.Duration
	if command == "wait" {
		fs.DurationVar(&timeout, "timeout", 10*time.Minute, "maximum time to wait")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: idlemesh %s [options] JOB_ID", command)
	}
	client, err := server.client(clientToken)
	if err != nil {
		return err
	}
	id := fs.Arg(0)
	if command == "status" {
		job, err := client.Status(ctx, id)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(job)
	}
	if timeout <= 0 {
		return errors.New("--timeout must be positive")
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return wait(waitCtx, client, id)
}

func wait(ctx context.Context, client *api.Client, id string) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := client.Status(ctx, id)
		if err == nil {
			switch job.State {
			case api.Succeeded:
				fmt.Fprintln(os.Stdout, job.Output)
				return nil
			case api.Failed:
				return fmt.Errorf("job %s failed: %s", id, job.Error)
			}
		} else if code := api.StatusCode(err); code >= 300 && code < 500 && code != http.StatusTooManyRequests {
			return err
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("waiting for job %s: %w", id, ctx.Err())
		}
	}
}
