package ha

import (
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
)

var seedMarkerKey = []byte("idlemesh-backup-bootstrap-v1")

const (
	seedPending  = "pending"
	seedComplete = "complete"
)

func checkSeedState(store *raftbolt.BoltStore, seed io.Reader, bootstrap, hasState bool) error {
	marker, err := store.Get(seedMarkerKey)
	if err != nil && !errors.Is(err, raftbolt.ErrKeyNotFound) {
		return fmt.Errorf("read backup bootstrap marker: %w", err)
	}
	if err == nil && (string(marker) != seedComplete || !hasState) {
		return errors.New("backup bootstrap is incomplete; use a new Raft data directory for recovery")
	}
	if seed != nil && (!bootstrap || hasState) {
		return errors.New("backup restore requires --raft-bootstrap and fresh Raft state")
	}
	return nil
}

func bootstrapWithSeed(
	conf *raft.Config,
	store *raftbolt.BoltStore,
	snapshots *raft.FileSnapshotStore,
	transport *raft.NetworkTransport,
	membership raft.Configuration,
	seed io.Reader,
) error {
	if seed != nil {
		if err := store.Set(seedMarkerKey, []byte(seedPending)); err != nil {
			return fmt.Errorf("mark backup bootstrap pending: %w", err)
		}
	}
	if err := raft.BootstrapCluster(conf, store, store, snapshots, transport, membership); err != nil {
		return err
	}
	if seed == nil {
		return nil
	}

	// BootstrapCluster writes the configuration at index 1 in term 1. A
	// snapshot at that same point seeds the application state before any
	// node starts Raft or serves requests for the new cluster.
	sink, err := snapshots.Create(1, 1, 1, membership, 1, transport)
	if err != nil {
		return fmt.Errorf("create recovered Raft snapshot: %w", err)
	}
	defer sink.Cancel()
	size, err := io.Copy(sink, seed)
	if err != nil {
		return fmt.Errorf("write recovered Raft snapshot: %w", err)
	}
	if size == 0 {
		return errors.New("prepared recovery snapshot is empty")
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("finish recovered Raft snapshot: %w", err)
	}
	// An empty follower must install the snapshot. If index 1 can be
	// replicated as an ordinary configuration log, it can skip the seed.
	if err := store.DeleteRange(1, 1); err != nil {
		return fmt.Errorf("compact recovered bootstrap log: %w", err)
	}
	if err := store.Set(seedMarkerKey, []byte(seedComplete)); err != nil {
		return fmt.Errorf("mark backup bootstrap complete: %w", err)
	}
	return nil
}
