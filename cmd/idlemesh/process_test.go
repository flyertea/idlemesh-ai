package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

func TestCLIProcessRoundTrip(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "idlemesh")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := func(args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = append(os.Environ(), "IDLEMESH_CLIENT_TOKEN=", "IDLEMESH_WORKER_TOKEN=")
		return cmd
	}
	run := func(args ...string) string {
		t.Helper()
		output, err := command(args...).CombinedOutput()
		if err != nil {
			t.Fatalf("idlemesh %s: %v\n%s", args[0], err, output)
		}
		return strings.TrimSpace(string(output))
	}
	start := func(cmd *exec.Cmd) {
		t.Helper()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		})
	}

	coordinator := command("coordinator", "--listen", "127.0.0.1:0", "--db", filepath.Join(dir, "idlemesh.db"))
	logs, err := coordinator.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	start(coordinator)
	ready := make(chan string, 1)
	go func() {
		defer close(ready)
		scanner := bufio.NewScanner(logs)
		for scanner.Scan() {
			if _, address, found := strings.Cut(scanner.Text(), "coordinator listening on "); found {
				ready <- address
			}
		}
	}()

	var address string
	select {
	case address = <-ready:
		if address == "" {
			t.Fatal("coordinator exited before listening")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for coordinator to listen")
	}
	start(command("worker", "--server", address, "--id", "cli-smoke", "--engine", "echo", "--poll", "50ms"))

	const prompt = "你好，IdleMesh！"
	id := run("submit", "--server", address, "--engine", "echo", "--prompt", prompt)
	if got := run("wait", "--server", address, "--timeout", "15s", id); got != prompt {
		t.Fatalf("wait output = %q, want %q", got, prompt)
	}
	var job api.Job
	if err := json.Unmarshal([]byte(run("status", "--server", address, id)), &job); err != nil {
		t.Fatalf("decode job status: %v", err)
	}
	if job.ID != id || job.State != api.Succeeded || job.WorkerID != "cli-smoke" || job.Attempt != 1 {
		t.Fatalf("unexpected finished job: %+v", job)
	}
}
