package ha

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestThreeVotersFailoverAndRestore(t *testing.T) {
	configs := testCluster(t)
	var nodes [3]*Node
	var fsms [3]*raft.MockFSM
	start := func(index int) {
		t.Helper()
		fsm := &raft.MockFSM{}
		node, err := Open(configs[index], fsm)
		if err != nil {
			t.Fatalf("start %s: %v", configs[index].ID, err)
		}
		nodes[index], fsms[index] = node, fsm
		t.Cleanup(func() {
			if err := node.Close(); err != nil {
				t.Errorf("close %s: %v", configs[index].ID, err)
			}
		})
	}
	stop := func(index int) {
		t.Helper()
		if err := nodes[index].Close(); err != nil {
			t.Fatal(err)
		}
		nodes[index] = nil
	}
	start(2)
	start(1)
	start(0)
	configs[0].Bootstrap = false

	firstLeader := awaitLeader(t, nodes)
	firstTerm := nodes[firstLeader].LeaderTerm()
	if firstTerm == 0 {
		t.Fatal("elected leader has no term")
	}
	for i, node := range nodes {
		if i != firstLeader {
			if term := node.LeaderTerm(); term != 0 {
				t.Fatalf("follower reports leader term %d", term)
			}
			if _, err := node.Apply(t.Context(), []byte("must not apply")); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("follower accepted a write: %v", err)
			}
		}
	}
	applyCount(t, nodes[firstLeader], "before failure", 1)
	if err := nodes[firstLeader].raft.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot leader: %v", err)
	}
	stop(firstLeader)
	newLeader := awaitLeader(t, nodes)
	if term := nodes[newLeader].LeaderTerm(); term <= firstTerm {
		t.Fatalf("replacement term = %d, want greater than %d", term, firstTerm)
	}
	applyCount(t, nodes[newLeader], "after failure", 2)

	start(firstLeader)
	current := awaitLeader(t, nodes)
	if err := nodes[current].Barrier(t.Context()); err != nil {
		t.Fatalf("barrier after rejoin: %v", err)
	}
	await(t, "restored voter applies both commands", func() bool {
		logs := fsms[firstLeader].Logs()
		return len(logs) == 2 && string(logs[0]) == "before failure" && string(logs[1]) == "after failure"
	})
	await(t, "every voter knows the elected leader", func() bool {
		for _, node := range nodes {
			if node.LeaderID() != configs[current].ID {
				return false
			}
		}
		return true
	})
	for i := range nodes {
		if i != current {
			stop(i)
		}
	}
	if _, err := nodes[current].Apply(t.Context(), []byte("without quorum")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("minority write error = %v", err)
	}
	if err := nodes[current].Barrier(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("minority barrier error = %v", err)
	}
	if logs := fsms[current].Logs(); len(logs) != 2 {
		t.Fatalf("minority committed a write: %q", logs)
	}
}

func TestPeerAuthenticationAndStaticRoster(t *testing.T) {
	configs := testCluster(t)
	badIdentity := configs[1]
	badIdentity.TLSCert, badIdentity.TLSKey = configs[2].TLSCert, configs[2].TLSKey
	if node, err := Open(badIdentity, &raft.MockFSM{}); err == nil {
		node.Close()
		t.Fatal("voter started using a certificate for a different ID")
	}

	first, err := Open(configs[0], &raft.MockFSM{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Close() })
	mismatched := configs[1]
	mismatched.Peers = append([]Peer(nil), mismatched.Peers...)
	mismatched.Peers[2].ID = "different-id"
	second, err := Open(mismatched, &raft.MockFSM{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close() })
	third, err := Open(configs[2], &raft.MockFSM{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { third.Close() })
	if index := awaitLeader(t, [3]*Node{first, nil, third}); index != 0 {
		t.Fatalf("unexpected initial leader %d", index)
	}
	applyCount(t, first, "only matching voters form quorum", 1)
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Barrier(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("mismatched voter formed a quorum: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if node, err := Open(configs[1], &raft.MockFSM{}); err == nil || !strings.Contains(err.Error(), "different node or peer roster") {
		if node != nil {
			node.Close()
		}
		t.Fatalf("persisted mismatched roster was accepted: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if node, err := Open(configs[0], &raft.MockFSM{}); err == nil || !strings.Contains(err.Error(), "restart without bootstrap") {
		if node != nil {
			node.Close()
		}
		t.Fatalf("bootstrapping existing state was accepted: %v", err)
	}
}

func TestAuthenticationPolicyMigration(t *testing.T) {
	configs := testCluster(t)
	legacy := configs[0]
	legacy.Listen = legacy.Peers[0].Address
	stream, err := newPeerStream(legacy, legacy.Peers, legacy.Peers[0])
	if err != nil {
		t.Fatal(err)
	}
	stream.Close()
	encoded, err := json.Marshal(legacy.Peers)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(encoded)
	if got, want := stream.clientTLS.NextProtos[0], "idlemesh-raft-v1-"+hex.EncodeToString(hash[:]); got != want {
		t.Fatalf("legacy ALPN = %q, want v0.3 protocol %q", got, want)
	}
	var nodes [3]*Node
	start := func() {
		t.Helper()
		for i, config := range configs {
			node, err := Open(config, &raft.MockFSM{})
			if err != nil {
				t.Fatalf("open %s: %v", config.ID, err)
			}
			nodes[i] = node
			t.Cleanup(func() { node.Close() })
		}
	}
	start()
	legacyLeader := awaitLeader(t, nodes)
	applyCount(t, nodes[legacyLeader], "legacy state", 1)
	for _, node := range nodes {
		if err := node.Close(); err != nil {
			t.Fatal(err)
		}
	}

	configs[0].Bootstrap = false
	configs[0].Policy = "managed-workers-v1"
	configs[1].Policy = "managed-workers-v1"
	start()
	leader := awaitLeader(t, [3]*Node{nodes[0], nodes[1], nil})
	applyCount(t, nodes[leader], "managed state", 2)
	if err := nodes[1-leader].Close(); err != nil {
		t.Fatal(err)
	}
	if err := nodes[leader].Barrier(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a legacy voter formed a managed quorum: %v", err)
	}
}

func applyCount(t *testing.T, node *Node, command string, count int) {
	t.Helper()
	got, err := node.Apply(t.Context(), []byte(command))
	if err != nil || got != count {
		t.Fatalf("apply %q = %v, %v, want %d", command, got, err, count)
	}
}

func awaitLeader(t *testing.T, nodes [3]*Node) int {
	t.Helper()
	var index int
	await(t, "elect a leader with quorum", func() bool {
		for i, node := range nodes {
			if node == nil || !node.IsLeader() {
				continue
			}
			ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
			err := node.Barrier(ctx)
			cancel()
			if err == nil {
				index = i
				return true
			}
		}
		return false
	})
	return index
}

func await(t *testing.T, description string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatalf("timed out waiting to %s", description)
}

func testCluster(t *testing.T) [3]Config {
	t.Helper()
	dir := t.TempDir()
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
	}()
	var peers []Peer
	for _, id := range []string{"a", "b", "c"} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		peers = append(peers, Peer{ID: id, Address: listener.Addr().String()})
	}
	newKey := func() *ecdsa.PrivateKey {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	writeCert := func(name string, der []byte) string {
		t.Helper()
		path := filepath.Join(dir, name+".pem")
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	now := time.Now()
	caKey := newKey()
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caFile := writeCert("ca", caDER)
	var configs [3]Config
	for index, peer := range peers {
		key := newKey()
		cert := &x509.Certificate{
			SerialNumber: big.NewInt(int64(index + 2)), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
			URIs:        []*url.URL{{Scheme: "urn", Opaque: "idlemesh:raft:" + peer.ID}},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		keyFile := filepath.Join(dir, peer.ID+"-key.pem")
		if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
			t.Fatal(err)
		}
		configs[index] = Config{
			ID: peer.ID, Peers: peers, Bootstrap: index == 0, DataDir: filepath.Join(dir, peer.ID),
			TLSCert: writeCert(peer.ID, der), TLSKey: keyFile, TLSCA: caFile, LogOutput: io.Discard,
		}
	}
	return configs
}
