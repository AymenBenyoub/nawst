package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"

	"strings"
	"sync"
	"time"

	pb "github.com/AymenBenyoub/nawst/core/proto"
	"github.com/hashicorp/memberlist"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Replicator struct {
	ID string
	Ml *memberlist.Memberlist

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
	for k, v := range rttMatrix {
		r.rttMatrix[k] = v
	}
	log.Printf("[replicator] stored metrics for %d nodes", len(metrics))
}

func (r *Replicator) UpdatePlacement() {
	if !r.updateMu.TryLock() {
		log.Printf("[replicator] placement update already in progress, skipping duplicate trigger")
		return
	}
	defer r.updateMu.Unlock()

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
	pl := &Placement{Nodes: scores}
	counts := pl.GetTargetVNodeCount(scores)
	pl.AssignVNodes(counts)
	pl.AssignReplicas(activeMetrics, rttMatrix, r.ReplicationFactor)

	r.SetPlacement(pl)
	log.Printf("[replicator] placement updated (epoch=%d) with %d active nodes", pl.Epoch, len(scores))
}

func (r *Replicator) getPlacement() *Placement {
	r.plMu.RLock()
	defer r.plMu.RUnlock()
	return r.placement
}

func parseMeta(meta []byte) (nodeID string, rpcAddr string, err error) {
	raw := strings.TrimSpace(string(meta))
	if raw == "" {
		return "", "", errors.New("empty member metadata")
	}

	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid member metadata format: %q", raw)
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid member metadata fields: %q", raw)
	}
	return parts[0], parts[1], nil
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
