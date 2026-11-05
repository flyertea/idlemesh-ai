package ha

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
)

var (
	replacementKey     = []byte("idlemesh-replacement-plan-v1")
	replacementDoneKey = []byte("idlemesh-replacement-committed-v1")
)

type replacementPlan struct {
	OldID string `json:"old_id"`
	NewID string `json:"new_id"`
	Peers []Peer `json:"peers"`
}

type replacement struct {
	plan      replacementPlan
	oldPeer   Peer
	newPeer   Peer
	snapshots raft.SnapshotStore
	output    io.Writer
	mu        sync.Mutex
	complete  atomic.Bool
}

type membershipStage uint8

const (
	membershipEmpty membershipStage = iota
	membershipOriginal
	membershipSurvivors
	membershipFinal
)

func validateReplacementConfig(cfg Config, peers []Peer) error {
	if cfg.JoinReplacement && (cfg.ReplaceID == "" || cfg.ID != cfg.ReplaceNewID) {
		return errors.New("--raft-join-replacement is only for the new voter in a replacement plan")
	}
	if cfg.ReplaceID == "" && cfg.ReplaceNewID == "" {
		return nil
	}
	if !peerIDPattern.MatchString(cfg.ReplaceID) || !peerIDPattern.MatchString(cfg.ReplaceNewID) || cfg.ReplaceID == cfg.ReplaceNewID {
		return errors.New("Raft replacement requires distinct, valid old and new voter IDs")
	}
	if cfg.Bootstrap || cfg.Seed != nil {
		return errors.New("Raft replacement cannot bootstrap or restore a cluster")
	}
	if _, present := findPeer(peers, cfg.ReplaceID); present {
		return errors.New("the retired voter must be absent from the target Raft roster")
	}
	if _, present := findPeer(peers, cfg.ReplaceNewID); !present {
		return errors.New("the replacement voter must be present in the target Raft roster")
	}
	return nil
}

func findPeer(peers []Peer, id string) (Peer, bool) {
	index := slices.IndexFunc(peers, func(peer Peer) bool { return peer.ID == id })
	if index < 0 {
		return Peer{}, false
	}
	return peers[index], true
}

func prepareReplacement(cfg Config, peers []Peer, store *raftbolt.BoltStore, snapshots raft.SnapshotStore, manifest []byte, hasState bool) (*replacement, error) {
	saved, err := store.Get(replacementKey)
	if err != nil && !errors.Is(err, raftbolt.ErrKeyNotFound) {
		return nil, fmt.Errorf("read Raft replacement plan: %w", err)
	}
	existingPlan := err == nil
	if existingPlan && cfg.JoinReplacement {
		return nil, errors.New("replacement voter has already been initialized; restart without --raft-join-replacement")
	}
	if cfg.ReplaceID == "" {
		if existingPlan {
			return nil, errors.New("this Raft directory requires its original --raft-replace plan on restart")
		}
		return nil, nil
	}
	plan := replacementPlan{OldID: cfg.ReplaceID, NewID: cfg.ReplaceNewID, Peers: peers}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	if existingPlan && !bytes.Equal(saved, encoded) {
		return nil, errors.New("Raft replacement plan differs from the one already recorded in this directory")
	}
	var persisted nodeManifest
	if len(manifest) != 0 {
		if err := json.Unmarshal(manifest, &persisted); err != nil || persisted.ID != cfg.ID {
			return nil, errors.New("Raft manifest belongs to a different voter")
		}
	}
	newPeer, _ := findPeer(peers, plan.NewID)
	r := &replacement{plan: plan, newPeer: newPeer, snapshots: snapshots, output: cfg.LogOutput}
	if r.output == nil {
		r.output = os.Stderr
	}
	isNew := cfg.ID == plan.NewID
	if isNew {
		if !existingPlan && !cfg.JoinReplacement {
			return nil, errors.New("a blank replacement directory requires --raft-join-replacement on the new voter's first startup")
		}
		if (!existingPlan && (hasState || len(manifest) != 0)) ||
			(len(manifest) != 0 && !slices.Equal(persisted.Peers, peers)) {
			return nil, errors.New("the replacement voter requires a new Raft data directory")
		}
	} else {
		if !hasState || len(persisted.Peers) != 3 {
			return nil, errors.New("a surviving voter must retain its original Raft data directory")
		}
		old, found := findPeer(persisted.Peers, plan.OldID)
		if !found {
			return nil, errors.New("the retired voter is absent from the original Raft roster")
		}
		for _, peer := range persisted.Peers {
			if peer.ID == newPeer.ID || peer.Address == newPeer.Address {
				return nil, errors.New("the replacement voter needs a new Raft ID and address")
			}
			if peer.ID != old.ID {
				if target, found := findPeer(peers, peer.ID); !found || target != peer {
					return nil, errors.New("both surviving voter IDs and Raft addresses must remain identical")
				}
			}
		}
		r.oldPeer = old
	}
	configuration, index, err := storedMembership(store, snapshots)
	if err != nil {
		return nil, err
	}
	stage, err := r.stage(configuration)
	if err != nil {
		return nil, err
	}
	if (!isNew && stage == membershipEmpty) || (!existingPlan && !isNew && stage != membershipOriginal) {
		return nil, errors.New("Raft replacement requires the original committed roster on both survivors")
	}
	done, err := store.Get(replacementDoneKey)
	if err != nil && !errors.Is(err, raftbolt.ErrKeyNotFound) {
		return nil, fmt.Errorf("read Raft replacement completion: %w", err)
	}
	if err == nil {
		if !existingPlan || !bytes.Equal(done, encoded) || stage != membershipFinal {
			return nil, errors.New("persisted Raft replacement completion does not match the voter roster")
		}
		r.complete.Store(true)
		r.report(index)
	}
	if !existingPlan {
		if err := store.Set(replacementKey, encoded); err != nil {
			return nil, fmt.Errorf("persist Raft replacement plan: %w", err)
		}
	}
	return r, nil
}

// The Raft GetConfiguration future in the pinned Raft version does not
// populate its index. Read the latest configuration and exact index from the
// durable log or the snapshot that compacted it.
func storedMembership(logs raft.LogStore, snapshots raft.SnapshotStore) (raft.Configuration, uint64, error) {
	available, err := snapshots.List()
	if err != nil {
		return raft.Configuration{}, 0, err
	}
	var snapshot raft.SnapshotMeta
	if len(available) != 0 {
		snapshot = *available[0]
		if snapshot.Version != 1 {
			return raft.Configuration{}, 0, errors.New("unsupported Raft snapshot configuration")
		}
	}
	first, err := logs.FirstIndex()
	if err != nil {
		return raft.Configuration{}, 0, err
	}
	last, err := logs.LastIndex()
	if err != nil {
		return raft.Configuration{}, 0, err
	}
	for index := last; first != 0 && index >= first && index > snapshot.Index; index-- {
		var entry raft.Log
		if err := logs.GetLog(index, &entry); err != nil {
			return raft.Configuration{}, 0, err
		}
		if entry.Type == raft.LogConfiguration {
			return raft.DecodeConfiguration(entry.Data), index, nil
		}
	}
	return snapshot.Configuration, snapshot.ConfigurationIndex, nil
}

func (r *replacement) stage(configuration raft.Configuration) (membershipStage, error) {
	if len(configuration.Servers) == 0 {
		return membershipEmpty, nil
	}
	invalid := errors.New("stored Raft membership does not match an allowed replacement stage")
	seen := make(map[string]bool, len(configuration.Servers))
	for _, server := range configuration.Servers {
		id, address := string(server.ID), string(server.Address)
		if server.Suffrage != raft.Voter || seen[id] {
			return 0, invalid
		}
		seen[id] = true
		if id == r.plan.OldID {
			canonical, err := normalizeAddress(address)
			if err != nil || address != canonical || (r.oldPeer.Address != "" && address != r.oldPeer.Address) {
				return 0, invalid
			}
			for _, peer := range r.plan.Peers {
				if peer.Address == address {
					return 0, invalid
				}
			}
			continue
		}
		peer, found := findPeer(r.plan.Peers, id)
		if !found || peer.Address != address {
			return 0, invalid
		}
	}
	for _, peer := range r.plan.Peers {
		if peer.ID != r.plan.NewID && !seen[peer.ID] {
			return 0, invalid
		}
	}
	switch {
	case seen[r.plan.OldID] && seen[r.plan.NewID]:
		return 0, invalid
	case seen[r.plan.OldID]:
		return membershipOriginal, nil
	case seen[r.plan.NewID]:
		return membershipFinal, nil
	default:
		return membershipSurvivors, nil
	}
}

// ReplacementPending reports whether this voter has yet to record completion
// for its configured replacement plan.
func (n *Node) ReplacementPending() bool {
	return n.replacement != nil && !n.replacement.complete.Load()
}

// ReconcileReplacement advances one step on the current leader and records a
// locally committed final roster on every voter. It can be retried after
// leadership loss, interruption, or restart.
func (n *Node) ReconcileReplacement(ctx context.Context) error {
	if !n.ReplacementPending() {
		return nil
	}
	r := n.replacement
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.complete.Load() {
		return nil
	}
	// Capture the watermark before reading the log. An uncommitted
	// configuration might be overwritten by a committed entry at the same
	// index during an election, so a later watermark cannot validate it.
	watermark := min(n.raft.CommitIndex(), n.raft.AppliedIndex())
	if watermark == 0 {
		return nil
	}
	configuration, index, err := storedMembership(n.store, r.snapshots)
	if err != nil {
		return err
	}
	stage, err := r.stage(configuration)
	if err != nil {
		return err
	}
	if index == 0 || index > watermark {
		return nil
	}
	if stage == membershipFinal {
		encoded, err := json.Marshal(r.plan)
		if err != nil {
			return err
		}
		if err := n.store.Set(replacementDoneKey, encoded); err != nil {
			return fmt.Errorf("persist Raft replacement completion: %w", err)
		}
		r.complete.Store(true)
		r.report(index)
		return nil
	}
	if !n.IsLeader() {
		return nil
	}
	if err := n.Barrier(ctx); err != nil {
		return err
	}
	_, err = n.wait(ctx, func(timeout time.Duration) raft.Future {
		if stage == membershipOriginal {
			return n.raft.RemoveServer(raft.ServerID(r.plan.OldID), index, timeout)
		}
		return n.raft.AddVoter(raft.ServerID(r.newPeer.ID), raft.ServerAddress(r.newPeer.Address), index, timeout)
	})
	return err
}

func (r *replacement) report(index uint64) {
	fmt.Fprintf(r.output, "Raft replacement committed locally: %s -> %s at index %d\n", r.plan.OldID, r.plan.NewID, index)
}
