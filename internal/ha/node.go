// Package ha runs a three-voter Raft group over mutual TLS.
package ha

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	bolt "go.etcd.io/bbolt"
)

const (
	operationTimeout = 3 * time.Second
	transportTimeout = 3 * time.Second
	maxPending       = 64
)

// ErrUnavailable means this node could not confirm an operation. An Apply
// that returns it may still commit if it reached the Raft log before timeout.
var ErrUnavailable = errors.New("Raft operation unavailable")

var manifestKey = []byte("idlemesh-static-roster-v1")

// Config defines one coordinator's Raft identity and peer transport.
type Config struct {
	ID              string
	Listen          string // defaults to the local peer's advertised address
	DataDir         string
	Peers           []Peer // all three voters, including the local node
	Bootstrap       bool   // use only on the first startup of the lexically first ID
	Policy          string // optional compatibility mode, negotiated only on the wire
	ReplaceID       string // retired voter for the replacement epoch
	ReplaceNewID    string // only this voter may join the epoch with an empty disk
	JoinReplacement bool   // explicit first startup of the new voter only
	Seed            io.Reader
	TLSCert         string
	TLSKey          string
	TLSCA           string
	LogOutput       io.Writer
}

// Peer identifies a voter and its advertised Raft address (host:port).
type Peer struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// Node owns the Raft listener, state, and operation deadline handling.
type Node struct {
	raft        *raft.Raft
	transport   *raft.NetworkTransport
	store       *raftbolt.BoltStore
	pending     chan struct{}
	replacement *replacement
	closeOnce   sync.Once
	closeErr    error
}

type nodeManifest struct {
	ID    string `json:"local_id"`
	Peers []Peer `json:"peers"`
}

// Open starts one voter. fsm must support deterministic replay and fresh
// restore from a snapshot. The caller owns and closes the FSM after Close.
func Open(cfg Config, fsm raft.FSM) (*Node, error) {
	if cfg.DataDir == "" || fsm == nil {
		return nil, errors.New("Raft requires a data directory and state machine")
	}
	peers, local, err := normalizePeers(cfg.ID, cfg.Peers)
	if err != nil {
		return nil, err
	}
	if err := validateReplacementConfig(cfg, peers); err != nil {
		return nil, err
	}
	if cfg.Bootstrap && local.ID != peers[0].ID {
		return nil, fmt.Errorf("only the first voter (%s) can bootstrap", peers[0].ID)
	}
	if cfg.Listen == "" {
		cfg.Listen = local.Address
	}
	stream, err := newPeerStream(cfg, peers, local)
	if err != nil {
		return nil, err
	}
	opened := false
	defer func() {
		if !opened {
			stream.Close()
		}
	}()

	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, fmt.Errorf("create Raft data directory: %w", err)
	}
	store, err := raftbolt.New(raftbolt.Options{
		Path:        filepath.Join(cfg.DataDir, "raft.db"),
		BoltOptions: &bolt.Options{Timeout: time.Second},
	})
	if err != nil {
		return nil, fmt.Errorf("open Raft database: %w", err)
	}
	defer func() {
		if !opened {
			store.Close()
		}
	}()
	manifest, err := json.Marshal(nodeManifest{cfg.ID, peers})
	if err != nil {
		return nil, err
	}
	saved, err := store.Get(manifestKey)
	newManifest := errors.Is(err, raftbolt.ErrKeyNotFound)
	if err != nil && !newManifest {
		return nil, fmt.Errorf("read Raft roster: %w", err)
	}
	if cfg.ReplaceID == "" && !newManifest && !bytes.Equal(saved, manifest) {
		return nil, errors.New("Raft data directory belongs to a different node or peer roster")
	}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "snapshots"), 0700); err != nil {
		return nil, fmt.Errorf("create Raft snapshots directory: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, cfg.LogOutput)
	if err != nil {
		return nil, fmt.Errorf("open Raft snapshots: %w", err)
	}
	hasState, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		return nil, fmt.Errorf("read existing Raft state: %w", err)
	}
	if newManifest && hasState {
		return nil, errors.New("existing Raft state has no IdleMesh roster")
	}
	if err := checkSeedState(store, cfg.Seed, cfg.Bootstrap, hasState); err != nil {
		return nil, err
	}
	if cfg.Bootstrap && hasState {
		return nil, errors.New("Raft state already exists; restart without bootstrap")
	}
	replacement, err := prepareReplacement(cfg, peers, store, snapshots, saved, hasState)
	if err != nil {
		return nil, err
	}
	if newManifest {
		if err := store.Set(manifestKey, manifest); err != nil {
			return nil, fmt.Errorf("store Raft roster: %w", err)
		}
	}

	transport := raft.NewNetworkTransport(stream, 2, transportTimeout, cfg.LogOutput)
	defer func() {
		if !opened {
			transport.Close()
		}
	}()
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.ID)
	raftConfig.LogOutput = cfg.LogOutput
	raftConfig.LogLevel = "WARN"
	raftConfig.SnapshotThreshold = 512
	raftConfig.SnapshotInterval = 30 * time.Second
	raftConfig.TrailingLogs = 128
	if cfg.Bootstrap {
		membership := raft.Configuration{}
		for _, peer := range peers {
			membership.Servers = append(membership.Servers, raft.Server{
				ID: raft.ServerID(peer.ID), Address: raft.ServerAddress(peer.Address), Suffrage: raft.Voter,
			})
		}
		if err := bootstrapWithSeed(raftConfig, store, snapshots, transport, membership, cfg.Seed); err != nil {
			return nil, fmt.Errorf("bootstrap Raft: %w", err)
		}
	}
	r, err := raft.NewRaft(raftConfig, fsm, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("start Raft: %w", err)
	}
	opened = true
	return &Node{raft: r, transport: transport, store: store, pending: make(chan struct{}, maxPending), replacement: replacement}, nil
}

// IsLeader is a routing hint. Apply and Barrier enforce quorum separately.
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// LeaderTerm observes the term while this voter is the leader, or returns zero.
// Apply and Barrier enforce quorum separately.
func (n *Node) LeaderTerm() uint64 {
	term := n.raft.CurrentTerm()
	if !n.IsLeader() || n.raft.CurrentTerm() != term {
		return 0
	}
	return term
}

// LeaderID is the last known leader, or the empty string during an election.
func (n *Node) LeaderID() string {
	_, id := n.raft.LeaderWithID()
	return string(id)
}

// Apply commits and applies a command before returning its FSM response.
// A context error after submission leaves the outcome uncertain.
func (n *Node) Apply(ctx context.Context, command []byte) (any, error) {
	data := bytes.Clone(command)
	return n.wait(ctx, func(timeout time.Duration) raft.Future { return n.raft.Apply(data, timeout) })
}

// Barrier confirms the local voter can commit with quorum and has applied all
// earlier commands. Call it before querying the local FSM.
func (n *Node) Barrier(ctx context.Context) error {
	_, err := n.wait(ctx, n.raft.Barrier)
	return err
}

func (n *Node) wait(ctx context.Context, submit func(time.Duration) raft.Future) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if !n.IsLeader() {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, raft.ErrNotLeader)
	}
	select {
	case n.pending <- struct{}{}:
	default:
		return nil, fmt.Errorf("%w: too many pending operations", ErrUnavailable)
	}
	type result struct {
		value any
		err   error
	}
	finished := make(chan result, 1)
	deadline, _ := ctx.Deadline()
	go func() {
		defer func() { <-n.pending }()
		if err := ctx.Err(); err != nil {
			finished <- result{err: err}
			return
		}
		timeout := max(time.Until(deadline), time.Nanosecond)
		future := submit(timeout)
		if err := future.Error(); err != nil {
			finished <- result{err: err}
			return
		}
		if apply, ok := future.(raft.ApplyFuture); ok {
			finished <- result{value: apply.Response()}
			return
		}
		finished <- result{}
	}()
	select {
	case out := <-finished:
		if out.err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnavailable, out.err)
		}
		return out.value, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, ctx.Err())
	}
}

// Close stops Raft and releases the local database lock.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		shutdownErr := n.raft.Shutdown().Error()
		n.transport.CloseStreams()
		n.closeErr = errors.Join(shutdownErr, n.store.Close())
	})
	return n.closeErr
}
