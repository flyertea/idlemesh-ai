package ha

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/hashicorp/raft"
)

type seedBuffer struct{ bytes.Buffer }

func (*seedBuffer) ID() string    { return "seed" }
func (*seedBuffer) Close() error  { return nil }
func (*seedBuffer) Cancel() error { return nil }

func TestRecoveredSeedSurvivesBootstrapVoterLoss(t *testing.T) {
	configs := testCluster(t)
	old := &raft.MockFSM{}
	old.Apply(&raft.Log{Data: []byte("before disaster")})
	snapshot, err := old.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	var source seedBuffer
	if err := snapshot.Persist(&source); err != nil {
		t.Fatal(err)
	}
	configs[0].Seed = &source

	var nodes [3]*Node
	var states [3]*raft.MockFSM
	for _, index := range []int{2, 1, 0} {
		states[index] = &raft.MockFSM{}
		node, err := Open(configs[index], states[index])
		if err != nil {
			t.Fatalf("open %s: %v", configs[index].ID, err)
		}
		nodes[index] = node
		t.Cleanup(func() { node.Close() })
	}
	leader := awaitLeader(t, nodes)
	applyCount(t, nodes[leader], "after recovery", 2)
	await(t, "install the recovered snapshot on all voters", func() bool {
		for _, state := range states {
			logs := state.Logs()
			if len(logs) != 2 || string(logs[0]) != "before disaster" || string(logs[1]) != "after recovery" {
				return false
			}
		}
		return true
	})
	if err := nodes[0].Close(); err != nil {
		t.Fatal(err)
	}
	nodes[0] = nil
	leader = awaitLeader(t, nodes)
	applyCount(t, nodes[leader], "after recovered bootstrap voter loss", 3)
}

func TestInterruptedSeedRefusesOrdinaryRestart(t *testing.T) {
	cfg := testCluster(t)[0]
	broken := errors.New("test backup interruption")
	cfg.Seed = iotest.ErrReader(broken)
	if node, err := Open(cfg, &raft.MockFSM{}); !errors.Is(err, broken) {
		if node != nil {
			node.Close()
		}
		t.Fatalf("interrupted seed: %v", err)
	}
	cfg.Bootstrap, cfg.Seed = false, nil
	if node, err := Open(cfg, &raft.MockFSM{}); err == nil || !strings.Contains(err.Error(), "bootstrap is incomplete") {
		if node != nil {
			node.Close()
		}
		t.Fatalf("ordinary restart after incomplete seed: %v", err)
	}
}
