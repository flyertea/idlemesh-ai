package ha

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

func TestReplaceRetiredVoter(t *testing.T) {
	configs := testVoters(t, []string{"a", "b", "c", "d"})
	original := configs[0].Peers[:3]
	target := []Peer{original[0], original[1], configs[0].Peers[3]}
	var nodes [3]*Node
	var machines [3]*raft.MockFSM
	start := func(config Config) (*Node, *raft.MockFSM) {
		t.Helper()
		machine := &raft.MockFSM{}
		node, err := Open(config, machine)
		if err != nil {
			t.Fatalf("open %s: %v", config.ID, err)
		}
		t.Cleanup(func() { node.Close() })
		return node, machine
	}
	for i := range nodes {
		configs[i].Peers = original
		nodes[i], machines[i] = start(configs[i])
		configs[i].Bootstrap = false
	}
	originalLeader := awaitLeader(t, nodes)
	applyCount(t, nodes[originalLeader], "before replacement", 1)
	await(t, "replicate the initial job to the survivors", func() bool {
		return len(machines[0].Logs()) == 1 && len(machines[1].Logs()) == 1
	})
	if err := nodes[0].raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	for _, node := range nodes {
		if err := node.Close(); err != nil {
			t.Fatal(err)
		}
	}

	staged := [3]Config{configs[0], configs[1], configs[3]}
	for i := range staged {
		staged[i].Peers = target
		staged[i].ReplaceID, staged[i].ReplaceNewID = "c", "d"
	}
	blank := staged[0]
	blank.DataDir = filepath.Join(t.TempDir(), "empty-survivor")
	if node, err := Open(blank, &raft.MockFSM{}); err == nil || !strings.Contains(err.Error(), "original Raft data directory") {
		if node != nil {
			node.Close()
		}
		t.Fatalf("empty surviving voter directory was accepted: %v", err)
	}
	wrongAddress := staged[0]
	wrongAddress.Peers = append([]Peer(nil), target...)
	wrongAddress.Peers[2].Address = original[2].Address
	if node, err := Open(wrongAddress, &raft.MockFSM{}); err == nil || !strings.Contains(err.Error(), "new Raft ID and address") {
		if node != nil {
			node.Close()
		}
		t.Fatalf("retired voter address could be reused: %v", err)
	}
	retired, _ := start(configs[2])
	for i := range nodes[:2] {
		nodes[i], machines[i] = start(staged[i])
	}
	nodes[2] = nil
	leader := awaitLeader(t, nodes)
	if err := nodes[leader].ReconcileReplacement(t.Context()); err != nil {
		t.Fatalf("remove retired voter: %v", err)
	}
	if got := nodes[leader].raft.GetConfiguration().Configuration(); len(got.Servers) != 2 {
		t.Fatalf("expected two-voter transition: %+v", got)
	}
	if err := nodes[leader].Close(); err != nil {
		t.Fatal(err)
	}
	if err := nodes[1-leader].Barrier(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("the retired voter counted toward quorum: %v", err)
	}
	nodes[leader], machines[leader] = start(staged[leader])
	if node, err := Open(staged[2], &raft.MockFSM{}); err == nil || !strings.Contains(err.Error(), "blank replacement directory") {
		if node != nil {
			node.Close()
		}
		t.Fatalf("blank replacement started without explicit first join: %v", err)
	}
	staged[2].JoinReplacement = true
	nodes[2], machines[2] = start(staged[2])
	staged[2].JoinReplacement = false
	leader = awaitLeader(t, nodes)
	if err := nodes[leader].ReconcileReplacement(t.Context()); err != nil {
		t.Fatalf("add replacement voter after restart: %v", err)
	}
	await(t, "commit the new roster on all three voters", func() bool {
		complete := true
		for _, node := range nodes {
			if err := node.ReconcileReplacement(t.Context()); err != nil && !errors.Is(err, ErrUnavailable) {
				t.Fatalf("record replacement completion: %v", err)
			}
			complete = complete && !node.ReplacementPending()
		}
		return complete
	})
	applyCount(t, nodes[awaitLeader(t, nodes)], "after replacement", 2)
	if err := nodes[2].Close(); err != nil {
		t.Fatal(err)
	}
	if node, err := Open(Config{
		ID: staged[2].ID, DataDir: staged[2].DataDir, Peers: staged[2].Peers,
		TLSCert: staged[2].TLSCert, TLSKey: staged[2].TLSKey, TLSCA: staged[2].TLSCA,
	}, &raft.MockFSM{}); err == nil || !strings.Contains(err.Error(), "original --raft-replace plan") {
		if node != nil {
			node.Close()
		}
		t.Fatalf("restart accepted without the replacement epoch: %v", err)
	}
	wrongDir := staged[2]
	wrongDir.DataDir = filepath.Join(t.TempDir(), "empty-after-replacement")
	if node, err := Open(wrongDir, &raft.MockFSM{}); err == nil || !strings.Contains(err.Error(), "blank replacement directory") {
		if node != nil {
			node.Close()
		}
		t.Fatalf("replacement restarted on an empty disk: %v", err)
	}
	nodes[2], machines[2] = start(staged[2])
	if nodes[2].ReplacementPending() {
		t.Fatal("completed plan was lost on restart")
	}
	if err := nodes[0].Close(); err != nil {
		t.Fatal(err)
	}
	nodes[0] = nil
	applyCount(t, nodes[awaitLeader(t, nodes)], "with replacement quorum", 3)
	if _, err := retired.Apply(context.Background(), []byte("must not apply")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("retired voter unexpectedly committed: %v", err)
	}
}
