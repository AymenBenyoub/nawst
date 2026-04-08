package core

import (
	"slices"
	"sync"
)

// Store holds all key-value data with vnode awareness.
// Performance: Get/Put = O(1) map ops; Delete = O(1) + O(1) set removal.
// vNodeIdx enables fast iteration of keys owned by each vnode during migration.
type Store struct {
	// storage: global map of all keys to values (unchanged from original).
	// No vnode tag on values to avoid doubling memory or requiring struct wrapping.
	storage map[string][]byte

	// vNodeMu: protects both storage and vNodeIdx to ensure consistency.
	// RWMutex allows concurrent Reads but serializes Writes (acceptable: writes are batched).
	vNodeMu sync.RWMutex

	// vNodeIdx: maps vnode ID → set of keys owned by that vnode.
	// Layout: map[uint16]map[string]bool where inner map = {key: true if owned}.
	// Enables O(1) key lookup within a vnode and O(n_keys_per_vnode) iteration.
	// Typical vnode = ~1024 keys / 4 nodes = ~250 keys, so iteration is fast.
	vNodeIdx map[uint16]map[string]bool

	// tombstoneIdx tracks deleted keys by vnode so migration can transfer delete markers.
	tombstoneIdx map[uint16]map[string]bool

	// meta stores per-key version/tombstone metadata used for conflict-safe migration.
	meta map[string]keyMeta
}

type keyMeta struct {
	Version   uint64
	Tombstone bool
}

func NewStore() *Store {
	return &Store{
		storage:      make(map[string][]byte),
		vNodeIdx:     make(map[uint16]map[string]bool),
		tombstoneIdx: make(map[uint16]map[string]bool),
		meta:         make(map[string]keyMeta),
	}
}

// Apply executes a command and updates vnode index.
// CRITICAL: Command must include VNodeID (computed from key hash at RPC layer before reaching here).
func (s *Store) Apply(cmd Command) error {
	s.vNodeMu.Lock()
	defer s.vNodeMu.Unlock()

	curMeta := s.meta[cmd.Key]
	if cmd.Version == 0 {
		// Legacy/unversioned write path fallback. Keep monotonicity locally.
		cmd.Version = curMeta.Version + 1
	} else if cmd.Version < curMeta.Version {
		// Ignore stale writes to avoid overwriting newer data during migration/replay.
		return nil
	}

	switch cmd.Op {
	case OpPut:
		// 1. Write value to global storage (fast, O(1)).
		s.storage[cmd.Key] = slices.Clone(cmd.Value)

		// 2. Register key in vnode index (fast, O(1) set insert).
		if s.vNodeIdx[cmd.VNodeID] == nil {
			s.vNodeIdx[cmd.VNodeID] = make(map[string]bool)
		}
		s.vNodeIdx[cmd.VNodeID][cmd.Key] = true
		if ts, ok := s.tombstoneIdx[cmd.VNodeID]; ok {
			delete(ts, cmd.Key)
		}
		s.meta[cmd.Key] = keyMeta{Version: cmd.Version, Tombstone: false}

	case OpDelete:
		// 1. Remove from global storage (fast, O(1)).
		delete(s.storage, cmd.Key)

		// 2. Deregister from vnode index (fast, O(1) set delete).
		if idx, ok := s.vNodeIdx[cmd.VNodeID]; ok {
			delete(idx, cmd.Key)
		}
		if s.tombstoneIdx[cmd.VNodeID] == nil {
			s.tombstoneIdx[cmd.VNodeID] = make(map[string]bool)
		}
		s.tombstoneIdx[cmd.VNodeID][cmd.Key] = true
		s.meta[cmd.Key] = keyMeta{Version: cmd.Version, Tombstone: true}

	default:
		return ErrInvalidOperation
	}
	return nil
}

// Get retrieves a key (read-only, no vnode index change).
func (s *Store) Get(key string) ([]byte, error) {
	s.vNodeMu.RLock()
	val, exists := s.storage[key]
	meta := s.meta[key]
	s.vNodeMu.RUnlock()

	if !exists || meta.Tombstone {
		return nil, ErrKeyNotFound
	}

	return slices.Clone(val), nil
}

// GetKeysForVNode returns all keys owned by a vnode (for migration/snapshot).
// O(n_keys_per_vnode) iteration, caller must not modify returned slice.
func (s *Store) GetKeysForVNode(vnodeID uint16) []string {
	s.vNodeMu.RLock()
	defer s.vNodeMu.RUnlock()

	indexSet, ok := s.vNodeIdx[vnodeID]
	if !ok {
		return []string{}
	}

	// Collect keys from the set into a slice.
	keys := make([]string, 0, len(indexSet))
	for key := range indexSet {
		keys = append(keys, key)
	}
	return keys
}

// SnapshotVNode returns a point-in-time copy of all key-value pairs for a vnode.
// Used during migration: snapshot → serialize → transfer → apply on target.
// O(n_keys_per_vnode * avg_value_size) but values are cloned to avoid races.
type VNodeSnapshot struct {
	VNodeID uint16
	Entries []struct {
		Key       string
		Value     []byte
		Version   uint64
		Tombstone bool
	}
}

// SnapshotVNode creates snapshot; O(n_keys_per_vnode) iteration + value clones.
func (s *Store) SnapshotVNode(vnodeID uint16) *VNodeSnapshot {
	s.vNodeMu.RLock()
	defer s.vNodeMu.RUnlock()

	indexSet, ok := s.vNodeIdx[vnodeID]
	if !ok {
		return &VNodeSnapshot{VNodeID: vnodeID, Entries: []struct {
			Key       string
			Value     []byte
			Version   uint64
			Tombstone bool
		}{}}
	}

	snapshot := &VNodeSnapshot{
		VNodeID: vnodeID,
		Entries: make([]struct {
			Key       string
			Value     []byte
			Version   uint64
			Tombstone bool
		}, 0, len(indexSet)),
	}

	for key := range indexSet {
		val, ok := s.storage[key]
		if !ok {
			// Key in index but missing from storage (should not happen; defensive).
			continue
		}
		meta := s.meta[key]
		snapshot.Entries = append(snapshot.Entries, struct {
			Key       string
			Value     []byte
			Version   uint64
			Tombstone bool
		}{Key: key, Value: slices.Clone(val), Version: meta.Version, Tombstone: meta.Tombstone})
	}

	for key := range s.tombstoneIdx[vnodeID] {
		meta := s.meta[key]
		snapshot.Entries = append(snapshot.Entries, struct {
			Key       string
			Value     []byte
			Version   uint64
			Tombstone bool
		}{Key: key, Value: nil, Version: meta.Version, Tombstone: true})
	}

	return snapshot
}

// DeleteVNodeData atomically removes all keys owned by a vnode.
// Used when vnode ownership is lost and migration is complete.
// O(n_keys_per_vnode) iteration + delete for each key.
func (s *Store) DeleteVNodeData(vnodeID uint16) {
	s.vNodeMu.Lock()
	defer s.vNodeMu.Unlock()

	indexSet, ok := s.vNodeIdx[vnodeID]
	if !ok {
		return
	}

	// Iterate index set and remove all keys from storage.
	for key := range indexSet {
		delete(s.storage, key)
		delete(s.meta, key)
	}
	for key := range s.tombstoneIdx[vnodeID] {
		delete(s.meta, key)
	}

	// Clear the vnode entry from index.
	delete(s.vNodeIdx, vnodeID)
	delete(s.tombstoneIdx, vnodeID)
}
