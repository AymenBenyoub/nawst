package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type placementApplyFn func(*Placement)

type raftCommand struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type installPlacementPayload struct {
	Placement Placement `json:"placement"`
}

type placementSnapshot struct {
	Placement Placement `json:"placement"`
}

type placementFSM struct {
	mu        sync.RWMutex
	placement *Placement
	onApply   placementApplyFn
}

func newPlacementFSM(onApply placementApplyFn) *placementFSM {
	return &placementFSM{onApply: onApply}
}

func (f *placementFSM) Apply(l *raft.Log) any {
	var cmd raftCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("decode raft command: %w", err)
	}

	switch cmd.Type {
	case "install_placement":
		var payload installPlacementPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			return fmt.Errorf("decode install_placement payload: %w", err)
		}

		var next Placement
		f.mu.Lock()
		if f.placement != nil && payload.Placement.Epoch <= f.placement.Epoch {
			f.mu.Unlock()
			return nil
		}
		next = payload.Placement
		f.placement = &next
		f.mu.Unlock() // nlock before callback

		if f.onApply != nil {
			f.onApply(&next)
		}
		return nil

	default:
		return fmt.Errorf("unknown raft command type: %s", cmd.Type)
	}
}

func (f *placementFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	s := placementSnapshot{}
	if f.placement != nil {
		s.Placement = *f.placement
	}
	return &fsmSnapshot{state: s}, nil
}

func (f *placementFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var s placementSnapshot
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}

	f.mu.Lock()
	f.placement = &s.Placement
	f.mu.Unlock()

	if f.onApply != nil {
		restored := s.Placement
		f.onApply(&restored)
	}
	return nil
}

func (f *placementFSM) currentPlacement() *Placement {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.placement == nil {
		return nil
	}
	cp := *f.placement
	return &cp
}

type fsmSnapshot struct {
	state placementSnapshot
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(&s.state); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

type RaftNode struct {
	mu        sync.RWMutex
	nodeID    string
	raft      *raft.Raft
	fsm       *placementFSM
	boltStore *raftboltdb.BoltStore
}

func NewRaftNode(nodeID string, raftBindAddr string, raftAdvertiseAddr string, raftDataDir string, bootstrap bool, onApply placementApplyFn) (*RaftNode, error) {
	if raftBindAddr == "" {
		return nil, fmt.Errorf("raft bind address is required")
	}
	if raftAdvertiseAddr == "" {
		return nil, fmt.Errorf("raft advertise address is required")
	}
	if raftDataDir == "" {
		return nil, fmt.Errorf("raft data dir is required")
	}
	if err := os.MkdirAll(raftDataDir, 0o755); err != nil {
		return nil, err
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(nodeID)
	cfg.LogOutput = io.Discard

	fsm := newPlacementFSM(onApply)
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("could not create bolt store: %s", err)
	}
	logStore, err := raft.NewLogCache(512, boltDB)
	if err != nil {
		return nil, fmt.Errorf("could not create log cache: %s", err)
	}
	stableStore := boltDB
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(raftDataDir, "snapshots"), 2, io.Discard)
	if err != nil {
		return nil, err
	}

	bindAddr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft bind address %q: %w", raftBindAddr, err)
	}
	advAddr, err := net.ResolveTCPAddr("tcp", raftAdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft advertise address %q: %w", raftAdvertiseAddr, err)
	}

	// Raft rejects transports whose local bind address is unspecified.
	// When users bind on 0.0.0.0/::, use the advertise IP as the effective bind host.
	if bindAddr.IP == nil || bindAddr.IP.IsUnspecified() {
		bindAddr.IP = advAddr.IP
		if bindAddr.Zone == "" {
			bindAddr.Zone = advAddr.Zone
		}
	}
	if bindAddr.Port == 0 {
		bindAddr.Port = advAddr.Port
	}

	effectiveBindAddr := bindAddr.String()
	transport, err := raft.NewTCPTransport(effectiveBindAddr, advAddr, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("create raft transport (bind=%s advertise=%s): %w", effectiveBindAddr, raftAdvertiseAddr, err)
	}

	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	rn := &RaftNode{nodeID: nodeID, raft: r, fsm: fsm, boltStore: boltDB}

	if bootstrap {
		c := raft.Configuration{Servers: []raft.Server{{
			ID:      raft.ServerID(nodeID),
			Address: raft.ServerAddress(raftAdvertiseAddr),
		}}}
		if fut := r.BootstrapCluster(c); fut.Error() != nil && fut.Error() != raft.ErrCantBootstrap {
			return nil, fut.Error()
		}
	}

	return rn, nil
}

func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.raft != nil && rn.raft.State() == raft.Leader
}

func (rn *RaftNode) LeaderAddr() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if rn.raft == nil {
		return ""
	}
	return string(rn.raft.Leader())
}

func (rn *RaftNode) ApplyPlacement(p *Placement, timeout time.Duration) error {
	if p == nil {
		return fmt.Errorf("nil placement")
	}
	payload, err := json.Marshal(installPlacementPayload{Placement: *p})
	if err != nil {
		return err
	}
	cmd, err := json.Marshal(raftCommand{Type: "install_placement", Payload: payload})
	if err != nil {
		return err
	}

	rn.mu.RLock()
	r := rn.raft
	rn.mu.RUnlock()
	if r == nil {
		return fmt.Errorf("raft is not initialized")
	}

	fut := r.Apply(cmd, timeout)
	return fut.Error()
}

func (rn *RaftNode) AddVoter(id, addr string, timeout time.Duration) error {
	rn.mu.RLock()
	r := rn.raft
	rn.mu.RUnlock()
	if r == nil {
		return fmt.Errorf("raft is not initialized")
	}
	fut := r.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, timeout)
	if err := fut.Error(); err != nil {
		if err == raft.ErrNotLeader {
			return nil
		}
		return err
	}
	return nil
}

func (rn *RaftNode) RemoveServer(id string, timeout time.Duration) error {
	rn.mu.RLock()
	r := rn.raft
	rn.mu.RUnlock()
	if r == nil {
		return fmt.Errorf("raft is not initialized")
	}
	fut := r.RemoveServer(raft.ServerID(id), 0, timeout)
	if err := fut.Error(); err != nil {
		if err == raft.ErrNotLeader {
			return nil
		}
		return err
	}
	return nil
}

func (rn *RaftNode) Configuration(timeout time.Duration) (raft.Configuration, error) {
	rn.mu.RLock()
	r := rn.raft
	rn.mu.RUnlock()
	if r == nil {
		return raft.Configuration{}, fmt.Errorf("raft is not initialized")
	}
	fut := r.GetConfiguration()
	if err := fut.Error(); err != nil {
		return raft.Configuration{}, err
	}
	return fut.Configuration(), nil
}

func (rn *RaftNode) CloseBoltDB() error {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if rn.raft == nil {
		return fmt.Errorf("raft is not initialized")
	}
	shutdownFuture := rn.raft.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		return fmt.Errorf("error shutting down raft: %w", err)
	}

	// 2. Close the BoltDB file (releases the file lock)
	if err := rn.boltStore.Close(); err != nil {
		return fmt.Errorf("error closing bolt store: %w", err)
	}
	return nil
}
