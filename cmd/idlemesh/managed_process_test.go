package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

func TestManagedWorkerCLIWithRaftFailover(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "idlemesh")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	if out, err := exec.CommandContext(t.Context(), "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()

	const submitSecret = "test-only-client-credential-1111"
	const adminSecret = "test-only-admin-credential-22222"
	clientEnv := []string{"IDLEMESH_CLIENT_TOKEN=" + submitSecret}
	adminEnv := []string{"IDLEMESH_ADMIN_TOKEN=" + adminSecret}
	command := func(env []string, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = append(os.Environ(), "IDLEMESH_CLIENT_TOKEN=", "IDLEMESH_ADMIN_TOKEN=", "IDLEMESH_WORKER_TOKEN=")
		cmd.Env = append(cmd.Env, env...)
		return cmd
	}
	run := func(env []string, args ...string) string {
		t.Helper()
		output, err := command(env, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("idlemesh %s: %v\n%s", args[0], err, output)
		}
		return string(output)
	}
	start := func(name string, cmd *exec.Cmd) func() {
		t.Helper()
		logFile, err := os.Create(filepath.Join(dir, name+".log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stdout, cmd.Stderr = logFile, logFile
		if err := cmd.Start(); err != nil {
			logFile.Close()
			t.Fatal(err)
		}
		stop := func() {
			if cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				_ = logFile.Close()
			}
		}
		t.Cleanup(func() {
			stop()
			if t.Failed() {
				contents, _ := os.ReadFile(logFile.Name())
				t.Logf("%s logs:\n%s", name, contents)
			}
		})
		return stop
	}

	addresses := reserveManagedAddresses(t, 6)
	ids := []string{"a", "b", "c"}
	ca, keys, certs := managedPeerTLS(t, dir, ids)
	var origins, servers, peers []string
	for i, id := range ids {
		origin := "http://" + addresses[i]
		origins = append(origins, origin)
		servers = append(servers, "--server", origin)
		peers = append(peers, "--raft-peer", id+"="+addresses[3+i])
	}
	var stopVoters []func()
	for i, id := range ids {
		args := []string{"coordinator", "--managed-workers", "--listen", addresses[i], "--lease", "2m",
			"--raft-id", id, "--raft-dir", filepath.Join(dir, id), "--raft-tls-ca", ca,
			"--raft-tls-key", keys[i], "--raft-tls-cert", certs[i]}
		args = append(args, peers...)
		if i == 0 {
			args = append(args, "--raft-bootstrap")
		}
		stopVoters = append(stopVoters, start(id, command(append([]string{clientEnv[0]}, adminEnv...), args...)))
	}
	awaitManagedLeader(t, ctx, origins, submitSecret, -1)
	issue := func(id string) string {
		t.Helper()
		output := run(adminEnv, append([]string{"worker-token", "issue", "--id", id}, servers...)...)
		token, newline := strings.CutSuffix(output, "\n")
		if _, ok := api.WorkerCredentialDigest(token); !newline || !ok {
			t.Fatal("issue did not print exactly one worker credential")
		}
		return token
	}
	clusterClient := func(token string) *api.Client {
		t.Helper()
		client, err := api.NewClusterClient(origins, token, false)
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	const prompt = "轮换、选主和撤销后，由备用节点完成。"
	oldToken, spareToken := issue("victim"), issue("spare")
	id := strings.TrimSpace(run(clientEnv, append([]string{"submit", "--engine", "echo", "--prompt", prompt}, servers...)...))
	oldWorker := clusterClient(oldToken)
	claim := func(client *api.Client, attempt int) *api.Lease {
		t.Helper()
		lease, err := client.Claim(ctx, api.ClaimRequest{WorkerID: "victim", Engine: "echo"})
		if err != nil || lease == nil || lease.ID != id || lease.Attempt != attempt {
			t.Fatalf("victim failed to claim attempt %d: %v", attempt, err)
		}
		return lease
	}
	denied := func(client *api.Client, lease *api.Lease) {
		t.Helper()
		_, claimErr := client.Claim(ctx, api.ClaimRequest{WorkerID: "victim", Engine: "echo"})
		_, renewErr := client.Renew(ctx, lease.ID, api.LeaseRequest{Token: lease.Token})
		finishErr := client.Finish(ctx, lease.ID, api.FinishRequest{Token: lease.Token, Output: "stale result"})
		if !api.IsStatus(claimErr, http.StatusUnauthorized) || !api.IsStatus(renewErr, http.StatusUnauthorized) || !api.IsStatus(finishErr, http.StatusUnauthorized) {
			t.Fatalf("old credential should be denied on claim, renew, and finish: %v, %v, %v", claimErr, renewErr, finishErr)
		}
	}
	if _, err := oldWorker.Claim(ctx, api.ClaimRequest{WorkerID: "spare", Engine: "echo"}); !api.IsStatus(err, http.StatusUnauthorized) {
		t.Fatalf("worker token accepted another identity: %v", err)
	}
	oldLease := claim(oldWorker, 1)
	rotated := issue("victim")
	if rotated == oldToken {
		t.Fatal("issue reused the previous credential")
	}
	denied(oldWorker, oldLease)
	newWorker := clusterClient(rotated)
	newLease := claim(newWorker, 2)
	revoke := func() {
		t.Helper()
		if output := run(adminEnv, append([]string{"worker-token", "revoke", "--id", "victim"}, servers...)...); output != "" {
			t.Fatal("revoke unexpectedly wrote output")
		}
	}
	revoke()
	denied(newWorker, newLease)
	failedLeader := awaitManagedLeader(t, ctx, origins, submitSecret, -1)
	stopVoters[failedLeader]()
	awaitManagedLeader(t, ctx, origins, submitSecret, failedLeader)
	denied(oldWorker, oldLease)
	denied(newWorker, newLease)
	revoke()
	start("spare", command([]string{"IDLEMESH_WORKER_TOKEN=" + spareToken}, append(
		[]string{"worker", "--engine", "echo", "--id", "spare", "--always-claim", "--poll", "50ms"}, servers...)...))
	wait := append([]string{"wait", "--timeout", "15s"}, servers...)
	if got := strings.TrimSpace(run(clientEnv, append(wait, id)...)); got != prompt {
		t.Fatalf("replacement worker output = %q, want %q", got, prompt)
	}
	job, err := clusterClient(submitSecret).Status(ctx, id)
	if err != nil || job.State != api.Succeeded || job.Attempt != 3 || job.WorkerID != "spare" {
		t.Fatalf("unexpected final job: %+v, %v", job, err)
	}
}

func awaitManagedLeader(t *testing.T, ctx context.Context, origins []string, token string, exclude int) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	transport := api.DirectTransport()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 350 * time.Millisecond}
	for ctx.Err() == nil {
		for i, origin := range origins {
			if i == exclude {
				continue
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/readyz", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+token)
			if response, err := client.Do(req); err == nil {
				response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return i
				}
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatal("no managed coordinator formed quorum")
	return -1
}

func reserveManagedAddresses(t *testing.T, count int) []string {
	t.Helper()
	addresses := make([]string, 0, count)
	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		addresses = append(addresses, listener.Addr().String())
	}
	return addresses
}

func managedPeerTLS(t *testing.T, dir string, ids []string) (string, []string, []string) {
	t.Helper()
	newKey := func() (ed25519.PublicKey, ed25519.PrivateKey) {
		t.Helper()
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return pub, key
	}
	writePEM := func(name, kind string, data []byte) string {
		t.Helper()
		path := filepath.Join(dir, name+".pem")
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: data}), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	now := time.Now()
	start, end := now.Add(-time.Hour), now.Add(time.Hour)
	caPub, caKey := newKey()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: start, NotAfter: end,
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caPub, caKey)
	if err != nil {
		t.Fatal(err)
	}
	var keys, certs []string
	for i, id := range ids {
		pub, key := newKey()
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), NotBefore: start, NotAfter: end,
			KeyUsage: x509.KeyUsageDigitalSignature, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
			URIs:        []*url.URL{{Scheme: "urn", Opaque: "idlemesh:raft:" + id}},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		cert, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, writePEM(id+"-key", "PRIVATE KEY", keyDER))
		certs = append(certs, writePEM(id, "CERTIFICATE", cert))
	}
	return writePEM("ca", "CERTIFICATE", caDER), keys, certs
}
