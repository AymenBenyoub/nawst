package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"

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
	memberSig string

	ReplicationFactor int
}

func NewReplicator(id string, ml *memberlist.Memberlist, rf int) *Replicator {
	return &Replicator{
		ID:                id,
		Ml:                ml,
		peers:             make(map[string]pb.KVClient),
		conns:             make(map[string]*grpc.ClientConn),
		ReplicationFactor: rf,
	}
}

func (r *Replicator) SetPlacement(p *Placement) {
	r.plMu.Lock()
	r.placement = p
	r.plMu.Unlock()
}

func (r *Replicator) RefreshPlacementNow() {
	if r == nil || r.Ml == nil {
		return
	}
	r.refreshPlacementFromMembership(r.Ml.Members())
}

func (r *Replicator) defaultMetricForNode(nodeID string) NodeMetrics {
	defaults := map[string]NodeMetrics{
		"node-9999":  {NodeID: "node-9999", AvgRTT: 1.1, BandwidthMbps: 1000, NetUsage: 0.25},
		"node-10000": {NodeID: "node-10000", AvgRTT: 1.5, BandwidthMbps: 900, NetUsage: 0.30},
		"node-10001": {NodeID: "node-10001", AvgRTT: 2.0, BandwidthMbps: 800, NetUsage: 0.35},
		"node-10002": {NodeID: "node-10002", AvgRTT: 1.3, BandwidthMbps: 950, NetUsage: 0.28},
	}
	if m, ok := defaults[nodeID]; ok {
		return m
	}
	return NodeMetrics{NodeID: nodeID, AvgRTT: 1.5, BandwidthMbps: 900, NetUsage: 0.30}
}

func (r *Replicator) buildDemoRTTMatrix(nodeIDs []string) map[string]map[string]float64 {
	overrides := map[string]map[string]float64{
		"node-9999":  {"node-10000": 1.2, "node-10001": 1.9, "node-10002": 1.4},
		"node-10000": {"node-9999": 1.2, "node-10001": 1.6, "node-10002": 1.1},
		"node-10001": {"node-9999": 1.9, "node-10000": 1.6, "node-10002": 1.8},
		"node-10002": {"node-9999": 1.4, "node-10000": 1.1, "node-10001": 1.8},
	}

	m := make(map[string]map[string]float64, len(nodeIDs))
	for _, src := range nodeIDs {
		m[src] = make(map[string]float64, len(nodeIDs)-1)
		for _, dst := range nodeIDs {
			if src == dst {
				continue
			}
			val := 1.5
			if row, ok := overrides[src]; ok {
				if ov, ok := row[dst]; ok {
					val = ov
				}
			}
			m[src][dst] = val
		}
	}
	return m
}

func (r *Replicator) refreshPlacementFromMembership(members []*memberlist.Node) {
	nodeIDs := make([]string, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		nodeID, _, err := parseMeta(member.Meta)
		if err != nil || nodeID == "" {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		nodeIDs = append(nodeIDs, nodeID)
	}

	sort.Strings(nodeIDs)
	if len(nodeIDs) == 0 {
		return
	}

	sig := strings.Join(nodeIDs, ",")
	r.plMu.RLock()
	same := (sig == r.memberSig && r.placement != nil && len(r.placement.VNodes) == VNodeCount)
	r.plMu.RUnlock()
	if same {
		return
	}

	metrics := make([]NodeMetrics, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		metrics = append(metrics, r.defaultMetricForNode(nodeID))
	}
	rttMatrix := r.buildDemoRTTMatrix(nodeIDs)

	scores := CalculateScores(metrics)
	pl := &Placement{Nodes: scores}
	counts := pl.GetTargetVNodeCount(scores)
	pl.AssignVNodes(counts)
	pl.AssignReplicas(metrics, rttMatrix, r.ReplicationFactor)

	r.plMu.Lock()
	r.placement = pl
	r.memberSig = sig
	r.plMu.Unlock()

	assigned := make(map[string]int)
	for _, vnode := range pl.VNodes {
		assigned[vnode.Primary]++
	}
	for nodeID, cnt := range assigned {
		log.Printf("placement local-summary: node=%s vnodes=%d", nodeID, cnt)
	}
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
	r.refreshPlacementFromMembership(members)

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
		primary, replicas := pl.GetNodesForKey(key)
		log.Printf("replication plan: op=%s key=%q from=%s primary=%s will_replicate_to=%v", op.String(), key, r.ID, primary, replicas)
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
		log.Printf("replication route: op=%s key=%q from=%s fallback=membership", op.String(), key, r.ID)
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
			log.Printf("replication send: op=%s key=%q from=%s to=%s", op.String(), key, r.ID, pid)

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

			_, err := c.Replicate(cctx, req)
			if err == nil {
				log.Printf("replication ack: op=%s key=%q from=%s to=%s", op.String(), key, r.ID, pid)
				resultCh <- nil
				return
			}
			log.Printf("replication retry-needed: op=%s key=%q from=%s to=%s err=%v", op.String(), key, r.ID, pid, err)

			retryErr := retryReplication(pid, c, req, cctx)
			if retryErr != nil {
				log.Printf("replication failed: op=%s key=%q from=%s to=%s err=%v", op.String(), key, r.ID, pid, retryErr)
				resultCh <- fmt.Errorf("replicate to %s failed after retries: %w", pid, retryErr)
				return
			}
			log.Printf("replication ack-after-retry: op=%s key=%q from=%s to=%s", op.String(), key, r.ID, pid)

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
		log.Printf("replication retry: attempt=%d key=%q to=%s", attempt+1, req.Key, pid)
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
