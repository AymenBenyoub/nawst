package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"

	"strings"
	"sync"
	"time"

	pb "github.com/AymenBenyoub/nawst/core/proto"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Replicator struct {
	ID string
	Ml *memberlist.Memberlist
	Rf *RaftNode

	mu    sync.Mutex
	peers map[string]pb.KVClient
	conns map[string]*grpc.ClientConn

	plMu      sync.RWMutex
	placement *Placement

	metricsMu sync.RWMutex
	metrics   []NodeMetrics
	rttMatrix map[string]map[string]float64

	updateMu sync.Mutex

	ReplicationFactor int
}

func NewReplicator(id string, ml *memberlist.Memberlist, rf int) *Replicator {
	return &Replicator{
		ID:                id,
		Ml:                ml,
		peers:             make(map[string]pb.KVClient),
		conns:             make(map[string]*grpc.ClientConn),
		metrics:           []NodeMetrics{},
		rttMatrix:         make(map[string]map[string]float64),
		ReplicationFactor: rf,
	}
}

func (r *Replicator) SetRaft(rfNode *RaftNode) {
	r.Rf = rfNode
}

func (r *Replicator) SetPlacement(p *Placement) {
	r.plMu.Lock()
	r.placement = p
	r.plMu.Unlock()
}

func (r *Replicator) SetMetrics(metrics []NodeMetrics, rttMatrix map[string]map[string]float64) {
	r.metricsMu.Lock()
	defer r.metricsMu.Unlock()
	r.metrics = append([]NodeMetrics{}, metrics...)
	r.rttMatrix = make(map[string]map[string]float64)
	maps.Copy(r.rttMatrix, rttMatrix)
	log.Printf("[replicator] stored metrics for %d nodes", len(metrics))
}

func (r *Replicator) UpdatePlacement() {
	if !r.updateMu.TryLock() {
		log.Printf("[replicator] placement update already in progress, skipping duplicate trigger")
		return
	}
	defer r.updateMu.Unlock()

	if r.Rf != nil && !r.Rf.IsLeader() {
		log.Printf("[replicator] skipping placement update on follower node %s", r.ID)
		return
	}

	if r.Ml == nil {
		log.Printf("[replicator] cannot update placement: memberlist is nil")
		return
	}

	// Get current cluster members
	members := r.Ml.Members()
	if len(members) == 0 {
		log.Printf("[replicator] cannot update placement: no members in cluster")
		return
	}

	// Extract node IDs from memberlist
	memberNodeIDs := make(map[string]bool)
	for _, member := range members {
		peerID, _, err := parseMeta(member.Meta)
		if err != nil {
			// Memberlist can surface nodes before metadata is fully propagated.
			// Fall back to member.Name so placement can still track active members.
			peerID = member.Name
			log.Printf("[replicator] metadata unavailable for %s, using member name as node id", member.Name)
		}
		memberNodeIDs[peerID] = true
	}

	r.metricsMu.RLock()
	allMetrics := r.metrics
	rttMatrix := r.rttMatrix
	r.metricsMu.RUnlock()

	// Filter metrics to only include nodes in current cluster
	var activeMetrics []NodeMetrics
	for _, m := range allMetrics {
		if memberNodeIDs[m.NodeID] {
			activeMetrics = append(activeMetrics, m)
		}
	}

	if len(activeMetrics) == 0 {
		log.Printf("[replicator] cannot update placement: no metrics found for active members")
		return
	}

	log.Printf("[replicator] updating placement for %d active nodes: %v", len(activeMetrics), memberNodeIDs)

	scores := CalculateScores(activeMetrics)
	prev := r.getPlacement()
	prevEpoch := uint64(0)
	var prevVNodes []VNode
	if prev != nil {
		prevEpoch = prev.Epoch
		if len(prev.VNodes) == VNodeCount {
			prevVNodes = cloneVNodes(prev.VNodes)
		}
	}

	pl := &Placement{Epoch: prevEpoch, Nodes: scores, VNodes: prevVNodes}
	counts := pl.GetTargetVNodeCount(scores)
	pl.AssignVNodes(counts)
	pl.AssignReplicas(activeMetrics, rttMatrix, r.ReplicationFactor)

	if r.Rf != nil {
		if err := r.Rf.ApplyPlacement(pl, 5*time.Second); err != nil {
			log.Printf("[replicator] failed to commit placement via raft: %v", err)
			return
		}
		log.Printf("[replicator] placement committed via raft (epoch=%d)", pl.Epoch)
		return
	}

	r.SetPlacement(pl)
	log.Printf("[replicator] placement updated locally (epoch=%d) with %d active nodes", pl.Epoch, len(scores))
}

func (r *Replicator) ApplyPlacementFromRaft(p *Placement) {
	if p == nil {
		return
	}
	r.SetPlacement(p)
	log.Printf("[replicator] applied committed placement from raft (epoch=%d)", p.Epoch)
}

func (r *Replicator) getPlacement() *Placement {
	r.plMu.RLock()
	defer r.plMu.RUnlock()
	return r.placement
}

func cloneVNodes(src []VNode) []VNode {
	if len(src) == 0 {
		return nil
	}
	dst := make([]VNode, len(src))
	for i := range src {
		dst[i] = src[i]
		if src[i].Replicas != nil {
			dst[i].Replicas = append([]string(nil), src[i].Replicas...)
		}
	}
	return dst
}

func parseMeta(meta []byte) (nodeID string, rpcAddr string, err error) {
	raw := strings.TrimSpace(string(meta))
	if raw == "" {
		return "", "", errors.New("empty member metadata")
	}

	if strings.Contains(raw, ",") {
		parts := strings.Split(raw, ",")
		if len(parts) < 2 {
			return "", "", fmt.Errorf("invalid member metadata format: %q", raw)
		}
		if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return "", "", fmt.Errorf("invalid member metadata fields: %q", raw)
		}
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
	}

	parts := strings.SplitN(raw, ":", 2)
	if len(parts) == 2 {
		if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return "", "", fmt.Errorf("invalid member metadata fields: %q", raw)
		}
		return parts[0], parts[1], nil
	}

	return "", "", fmt.Errorf("invalid member metadata format: %q", raw)
}

func parseMetaWithRaft(meta []byte) (nodeID string, rpcAddr string, raftAddr string, err error) {
	raw := strings.TrimSpace(string(meta))
	if raw == "" {
		return "", "", "", errors.New("empty member metadata")
	}
	parts := strings.Split(raw, ",")
	if len(parts) < 2 {
		id, rpc, e := parseMeta(meta)
		return id, rpc, "", e
	}
	nodeID = strings.TrimSpace(parts[0])
	rpcAddr = strings.TrimSpace(parts[1])
	if len(parts) > 2 {
		raftAddr = strings.TrimSpace(parts[2])
	}
	if nodeID == "" || rpcAddr == "" {
		return "", "", "", fmt.Errorf("invalid member metadata fields: %q", raw)
	}
	return nodeID, rpcAddr, raftAddr, nil
}

func (r *Replicator) getOrCreateClient(member *memberlist.Node) (pb.KVClient, error) {
	if member == nil {
		return nil, errors.New("nil member")
	}

	peerID, rpcAddr, err := parseMeta(member.Meta)
	if err != nil {
		return nil, fmt.Errorf("parse metadata for %s: %w", member.Name, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if c, ok := r.peers[peerID]; ok {
		return c, nil
	}

	conn, err := grpc.NewClient(
		rpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s (%s): %w", peerID, rpcAddr, err)
	}

	client := pb.NewKVClient(conn)
	r.peers[peerID] = client
	r.conns[peerID] = conn
	return client, nil
}

func (r *Replicator) getOrCreateClientByNodeID(nodeID string) (pb.KVClient, error) {
	if r.Ml == nil {
		return nil, errors.New("memberlist is nil")
	}
	for _, member := range r.Ml.Members() {
		peerID, _, err := parseMeta(member.Meta)
		if err != nil {
			continue
		}
		if peerID == nodeID {
			return r.getOrCreateClient(member)
		}
	}
	return nil, fmt.Errorf("node %s not found in membership", nodeID)
}

func (r *Replicator) ReplicateToAll(ctx context.Context, op pb.Op, key string, value []byte) error {
	if r.Ml == nil {
		return errors.New("memberlist is nil")
	}

	members := r.Ml.Members()
	if len(members) == 0 {
		return nil
	}

	// local primary write already succeeded before this function is called
	acks := 1

	effectiveRF := min(max(r.ReplicationFactor, 1), len(members))

	required := effectiveRF/2 + 1
	if acks >= required {
		return nil
	}

	type target struct {
		id     string
		client pb.KVClient
	}

	targets := make([]target, 0, len(members)-1)
	var firstErr error

	pl := r.getPlacement()
	if pl != nil && len(pl.VNodes) == VNodeCount {
		_, replicas := pl.GetNodesForKey(key)
		log.Printf("[replicator] key=%q routes to replicas: %v", key, replicas)
		seen := make(map[string]struct{})
		for _, replicaID := range replicas {
			if replicaID == "" || replicaID == r.ID {
				continue
			}
			if _, exists := seen[replicaID]; exists {
				continue
			}
			seen[replicaID] = struct{}{}

			client, err := r.getOrCreateClientByNodeID(replicaID)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			targets = append(targets, target{id: replicaID, client: client})
		}
	}

	if len(targets) == 0 {
		log.Printf("[replicator] no placement targets, falling back to all members for key=%q", key)
		for _, member := range members {
			peerID, _, err := parseMeta(member.Meta)
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("member %s metadata: %w", member.Name, err)
				}
				continue
			}
			if peerID == r.ID {
				continue
			}

			client, err := r.getOrCreateClient(member)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}

			targets = append(targets, target{id: peerID, client: client})
		}
	}

	remaining := len(targets)
	if acks+remaining < required {
		if firstErr != nil {
			return fmt.Errorf("quorum impossible: need %d acks, have %d local + %d remotes: %w", required, acks, remaining, firstErr)
		}
		return fmt.Errorf("quorum impossible: need %d acks, have %d local + %d remotes", required, acks, remaining)
	}

	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan error, len(targets))
	var wg sync.WaitGroup

	for _, t := range targets {
		wg.Add(1)
		go func(pid string, c pb.KVClient) {
			defer wg.Done()

			cctx := callCtx
			if _, hasDeadline := callCtx.Deadline(); !hasDeadline {
				var cCancel context.CancelFunc
				cctx, cCancel = context.WithTimeout(callCtx, 2*time.Second)
				defer cCancel()
			}
			req := &pb.ReplicationRequest{
				Op:    op,
				Key:   key,
				Value: value,
			}

			log.Printf("[replicator] sending replication request to %s: op=%v key=%q", pid, op, key)
			_, err := c.Replicate(cctx, req)
			if err == nil {
				log.Printf("[replicator] replication to %s succeeded: key=%q", pid, key)
				resultCh <- nil
				return
			}

			log.Printf("[replicator] replication to %s failed (will retry): key=%q error=%v", pid, key, err)
			retryErr := retryReplication(pid, c, req, cctx)
			if retryErr != nil {
				log.Printf("[replicator] replication to %s failed after retries: key=%q", pid, key)
				resultCh <- fmt.Errorf("replicate to %s failed after retries: %w", pid, retryErr)
				return
			}

			log.Printf("[replicator] replication to %s succeeded after retries: key=%q", pid, key)
			resultCh <- nil
		}(t.id, t.client)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for err := range resultCh {
		remaining--

		if err == nil {
			acks++
			if acks >= required {
				cancel()
				return nil
			}
		} else if firstErr == nil {
			firstErr = err
		}

		if acks+remaining < required {
			cancel()
			if firstErr != nil {
				return fmt.Errorf("write quorum not reached: got %d/%d acks: %w", acks, required, firstErr)
			}
			return fmt.Errorf("write quorum not reached: got %d/%d acks", acks, required)
		}
	}

	if acks >= required {
		return nil
	}
	if firstErr != nil {
		return fmt.Errorf("write quorum not reached: got %d/%d acks: %w", acks, required, firstErr)
	}
	return fmt.Errorf("write quorum not reached: got %d/%d acks", acks, required)
}

func (r *Replicator) ReplicatePut(ctx context.Context, key string, value []byte) error {
	return r.ReplicateToAll(ctx, pb.Op_PUT, key, value)
}

func (r *Replicator) ReplicateDelete(ctx context.Context, key string) error {
	return r.ReplicateToAll(ctx, pb.Op_DELETE, key, nil)
}
func retryReplication(pid string, c pb.KVClient, req *pb.ReplicationRequest, cctx context.Context) error {
	const maxAttempts = 3
	backoff := 500 * time.Millisecond

	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-cctx.Done():
			return fmt.Errorf("context cancelled while retrying replication to %s: %w", pid, cctx.Err())
		default:
		}

		_, err := c.Replicate(cctx, req)
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt < maxAttempts-1 {
			select {
			case <-cctx.Done():
				return fmt.Errorf("context cancelled while retrying replication to %s: %w", pid, cctx.Err())
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}

	return fmt.Errorf("retry replication to %s exhausted: %w", pid, lastErr)
}
func (r *Replicator) CheckOwnership(key string) (bool, string) {
	pl := r.getPlacement()
	if pl == nil {
		return true, r.ID
	}
	owner, _ := pl.GetNodesForKey(key)
	return strings.EqualFold(owner, r.ID), owner
}

func (r *Replicator) IsLeader() bool {
	if r.Rf == nil {
		return true
	}
	return r.Rf.IsLeader()
}

func (r *Replicator) ReconcileRaftWithMembership(members []*memberlist.Node) error {
	if r.Rf == nil || !r.Rf.IsLeader() {
		return nil
	}

	cfg, err := r.Rf.Configuration(5 * time.Second)
	if err != nil {
		return err
	}

	existing := make(map[string]raft.Server)
	for _, s := range cfg.Servers {
		existing[string(s.ID)] = s
	}

	seen := make(map[string]struct{})
	for _, m := range members {
		nodeID, _, raftAddr, err := parseMetaWithRaft(m.Meta)
		if err != nil {
			continue
		}
		if raftAddr == "" {
			continue
		}
		seen[nodeID] = struct{}{}
		if _, ok := existing[nodeID]; ok {
			continue
		}
		if err := r.Rf.AddVoter(nodeID, raftAddr, 5*time.Second); err != nil {
			log.Printf("[raft-reconcile] add voter failed for %s (%s): %v", nodeID, raftAddr, err)
		}
	}

	for id := range existing {
		if id == r.ID {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		if err := r.Rf.RemoveServer(id, 5*time.Second); err != nil {
			log.Printf("[raft-reconcile] remove server failed for %s: %v", id, err)
		}
	}

	// After reconciling membership, recompute and commit placement as leader.
	r.UpdatePlacement()

	return nil
}

func (r *Replicator) ForwardToOwner(ctx context.Context, owner string, req any) error {
	client, err := r.getOrCreateClientByNodeID(owner)
	if err != nil {
		return err
	}
	switch req := req.(type) {
	case *pb.PutRequest:
		_, err := client.Put(ctx, req)
		if err != nil {
			return err
		}

	case *pb.DeleteRequest:
		_, err := client.Delete(ctx, req)
		if err != nil {
			return err
		}
	}
	return nil
}
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var firstErr error
	for peerID, conn := range r.conns {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close connection to %s: %w", peerID, err)
		}
	}
	r.peers = make(map[string]pb.KVClient)
	r.conns = make(map[string]*grpc.ClientConn)
	return firstErr
}

// Compile-time check that this file imports emptypb only when generated stubs are in sync.
var _ = emptypb.Empty{}
