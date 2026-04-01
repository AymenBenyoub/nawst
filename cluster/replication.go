package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"sort"

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

	mu    sync.RWMutex
	peers map[string]pb.KVClient
	conns map[string]*grpc.ClientConn

	plMu      sync.RWMutex
	placement *Placement

	metricsMu sync.RWMutex
	metrics   []NodeMetrics
	rttMatrix map[string]map[string]float64
	collector *MetricsCollector
	gossipFn  func([]byte) error

	updateMu sync.Mutex
	stopMu   sync.Mutex
	stopCh   chan struct{}

	nodeAddrMu   sync.RWMutex
	nodeRPCAddrs map[string]string

	deadRowMu   sync.Mutex
	deadRow     map[string]time.Time // NodeID -> Time it went missing
	GracePeriod time.Duration

	metricEMA      map[string]NodeMetrics
	degradedStreak map[string]int

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
		nodeRPCAddrs:      make(map[string]string),
		ReplicationFactor: rf,
		stopCh:            make(chan struct{}),
		deadRow:           make(map[string]time.Time),
		metricEMA:         make(map[string]NodeMetrics),
		degradedStreak:    make(map[string]int),
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
}

func (r *Replicator) SetMetricsCollector(c *MetricsCollector) {
	r.metricsMu.Lock()
	defer r.metricsMu.Unlock()
	r.collector = c
}

func (r *Replicator) SetGossipBroadcaster(fn func([]byte) error) {
	r.metricsMu.Lock()
	defer r.metricsMu.Unlock()
	r.gossipFn = fn
}

func (r *Replicator) HandleMetricsMessage(msg *GossipMetricsMessage) {
	if msg == nil || msg.Metrics.NodeID == "" {
		return
	}

	r.metricsMu.Lock()
	defer r.metricsMu.Unlock()

	smoothed := r.smoothMetricsLocked(msg.Metrics)

	found := false
	for i := range r.metrics {
		if r.metrics[i].NodeID == smoothed.NodeID {
			r.metrics[i] = smoothed
			found = true
			break
		}
	}
	if !found {
		r.metrics = append(r.metrics, smoothed)
	}

	if _, ok := r.rttMatrix[smoothed.NodeID]; !ok {
		r.rttMatrix[smoothed.NodeID] = make(map[string]float64)
	}
	maps.Copy(r.rttMatrix[smoothed.NodeID], msg.RTTData)

}

func (r *Replicator) StartMetricsReporter(interval time.Duration) {
	if interval <= 0 {
		interval = 4 * time.Second
	}

	publishTicker := time.NewTicker(interval)
	shortEvalTicker := time.NewTicker(3 * interval)
	longEvalTicker := time.NewTicker(15 * interval)
	go func() {
		defer publishTicker.Stop()
		defer shortEvalTicker.Stop()
		defer longEvalTicker.Stop()

		initialPlacementDone := false
		for {
			select {
			case <-r.stopCh:
				return
			case <-publishTicker.C:
				r.publishLocalMetrics()
				if !initialPlacementDone && (r.Rf == nil || r.Rf.IsLeader()) {
					r.UpdatePlacement()
					initialPlacementDone = true
				}
			case <-shortEvalTicker.C:
				if r.Rf == nil || r.Rf.IsLeader() {
					r.UpdatePlacementShortTerm()
				}
			case <-longEvalTicker.C:
				if r.Rf == nil || r.Rf.IsLeader() {
					r.UpdatePlacement()
				}
			}
		}
	}()
}

func (r *Replicator) publishLocalMetrics() {
	r.metricsMu.RLock()
	collector := r.collector
	gossipFn := r.gossipFn
	r.metricsMu.RUnlock()

	if collector == nil {
		return
	}

	if r.Ml != nil {
		for _, member := range r.Ml.Members() {
			peerID, rpcAddr, err := parseMeta(member.Meta)
			if err != nil {
				continue
			}
			r.setNodeRPCAddr(peerID, rpcAddr)
			if peerID == r.ID {
				continue
			}
			collector.ProbePeerRTT(peerID, rpcAddr, 300*time.Millisecond)
		}
	}

	m := collector.GetCurrentMetrics()
	rtt := collector.SnapshotRTT()

	msg := &GossipMetricsMessage{
		Type:    "metrics",
		NodeID:  m.NodeID,
		Metrics: m,
		RTTData: rtt,
		Time:    time.Now().UnixMilli(),
	}
	r.HandleMetricsMessage(msg)

	b, err := EncodeGossipMetricsMessage(m, rtt)
	if err != nil {
		return
	}
	if gossipFn != nil {
		_ = gossipFn(b)
	}

	LogResourceUsage(r.ID, m)

}

func (r *Replicator) StopMetricsReporter() {
	r.stopMu.Lock()
	defer r.stopMu.Unlock()
	select {
	case <-r.stopCh:
		return
	default:
		close(r.stopCh)
	}
}

func (r *Replicator) UpdatePlacementShortTerm() {
	if !r.updateMu.TryLock() {
		return
	}
	defer r.updateMu.Unlock()

	if r.Rf != nil && !r.Rf.IsLeader() {
		return
	}

	prev := r.getPlacement()
	if prev == nil || len(prev.VNodes) != VNodeCount {
		// Short-term role swaps require an existing committed placement.
		return
	}

	if r.Ml == nil {
		return
	}

	members := r.Ml.Members()
	if len(members) == 0 {
		return
	}

	memberNodeIDs := make(map[string]bool)
	for _, member := range members {
		peerID, _, err := parseMeta(member.Meta)
		if err != nil {
			peerID = member.Name
		}
		memberNodeIDs[peerID] = true
	}

	r.metricsMu.RLock()
	allMetrics := r.metrics
	r.metricsMu.RUnlock()

	var activeMetrics []NodeMetrics
	for _, m := range allMetrics {
		if memberNodeIDs[m.NodeID] {
			activeMetrics = append(activeMetrics, m)
		}
	}

	if len(activeMetrics) == 0 {
		return
	}

	scores := CalculateScores(activeMetrics)
	log.Printf("[placement-short] score snapshot: %s", formatScoreSummary(scores, 6))
	scoreByID := make(map[string]float64, len(scores))
	for _, s := range scores {
		scoreByID[s.ID] = s.Score
	}

	clusterAvgRTT := averageRTT(activeMetrics)
	degradedNow := make(map[string]bool, len(activeMetrics))
	sustainedDegraded := make(map[string]bool, len(activeMetrics))
	for _, m := range activeMetrics {
		now := isSeverelyDegraded(m, clusterAvgRTT)
		degradedNow[m.NodeID] = now
		if now {
			r.degradedStreak[m.NodeID]++
		} else {
			r.degradedStreak[m.NodeID] = 0
		}
		if r.degradedStreak[m.NodeID] >= 3 {
			sustainedDegraded[m.NodeID] = true
		}
	}
	log.Printf("[placement-short] degraded-now=%s sustained=%s", formatBoolNodeSet(degradedNow), formatBoolNodeSet(sustainedDegraded))

	nextVNodes := cloneVNodes(prev.VNodes)
	swaps := 0

	for i := range nextVNodes {
		v := &nextVNodes[i]
		if v.Primary == "" || len(v.Replicas) == 0 {
			continue
		}
		if !sustainedDegraded[v.Primary] {
			continue
		}

		bestID := v.Primary
		bestScore, ok := scoreByID[v.Primary]
		if !ok {
			continue
		}
		bestReplicaIdx := -1

		for idx, replicaID := range v.Replicas {
			if degradedNow[replicaID] {
				continue
			}
			replicaScore, ok := scoreByID[replicaID]
			if !ok {
				continue
			}
			if replicaScore < bestScore*0.90 {
				bestID = replicaID
				bestScore = replicaScore
				bestReplicaIdx = idx
			}
		}

		if bestReplicaIdx >= 0 && bestID != v.Primary {
			oldPrimary := v.Primary
			v.Primary = bestID
			v.Replicas[bestReplicaIdx] = oldPrimary
			swaps++
		}
	}

	if swaps == 0 {
		return
	}

	shortThreshold := (VNodeCount * 1) / 100
	if shortThreshold < 1 {
		shortThreshold = 1
	}
	if swaps < shortThreshold {
		log.Printf("[placement-short] swap delta (%d) below threshold (%d), preserving epoch %d", swaps, shortThreshold, prev.Epoch)
		return
	}

	pl := &Placement{Epoch: prev.Epoch + 1, Nodes: scores, VNodes: nextVNodes}

	if r.Rf != nil {
		if err := r.Rf.ApplyPlacement(pl, 5*time.Second); err != nil {
			log.Printf("[placement-short] failed to commit short-term placement: %v", err)
			return
		}
		log.Printf("[placement-short] committed short-term role swaps: %d vnodes (epoch=%d)", swaps, pl.Epoch)
		return
	}

	r.SetPlacement(pl)
	log.Printf("[placement-short] applied short-term role swaps locally: %d vnodes (epoch=%d)", swaps, pl.Epoch)
}

func (r *Replicator) UpdatePlacement() {
	if !r.updateMu.TryLock() {
		return
	}
	defer r.updateMu.Unlock()

	if r.Rf != nil && !r.Rf.IsLeader() {
		return
	}

	if r.Ml == nil {
		return
	}

	// Get current cluster members
	members := r.Ml.Members()
	if len(members) == 0 {
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
		return
	}

	scores := CalculateScores(activeMetrics)
	log.Printf("[placement-long] score snapshot: %s", formatScoreSummary(scores, 8))
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
	log.Printf("[placement-long] target vnode counts: %s", formatVNodeCounts(counts))
	pl.AssignVNodes(counts)
	pl.AssignReplicas(activeMetrics, rttMatrix, r.ReplicationFactor)

	// --- Hysteresis Logic to prevent Placement Thrashing ---
	if prev != nil && len(prev.VNodes) == VNodeCount {
		movedPrimaryCount := 0
		movedReplicaCount := 0
		for i := range VNodeCount {
			if pl.VNodes[i].Primary != prev.VNodes[i].Primary {
				movedPrimaryCount++
			}

			// Also check replica drift
			if len(pl.VNodes[i].Replicas) != len(prev.VNodes[i].Replicas) {
				movedReplicaCount++
			} else {
				for j, rep := range pl.VNodes[i].Replicas {
					if rep != prev.VNodes[i].Replicas[j] {
						movedReplicaCount++
						break
					}
				}
			}
		}

		// Calculate total drift

		primaryThreshold := (VNodeCount * 5) / 100
		replicaThreshold := (VNodeCount * (r.ReplicationFactor - 1) * 20) / 100

		if movedPrimaryCount < primaryThreshold && movedReplicaCount < replicaThreshold {

			// Drift is negligible. Abort update to prevent thrashing.
			log.Printf("[placement-long] delta (%d,%d) below threshold (%d,%d), preserving epoch %d", movedPrimaryCount, movedReplicaCount, primaryThreshold, replicaThreshold, prev.Epoch)
			return
		}

		pl.Epoch = prev.Epoch + 1
		log.Printf("[placement-long] significant drift (%d,%d). incrementing epoch to %d", movedPrimaryCount, movedReplicaCount, pl.Epoch)
	} else {
		pl.Epoch = 1
	}
	// --------------------------------------------------------

	// Log VNode distribution only if we are actually applying/committing it
	distribution := make(map[string]int)
	for _, v := range pl.VNodes {
		if v.Primary != "" {
			distribution[v.Primary]++
		}
	}
	log.Printf("[placement] epoch=%d proposed vnode distribution: %v", pl.Epoch, distribution)

	if r.Rf != nil {
		if err := r.Rf.ApplyPlacement(pl, 5*time.Second); err != nil {
			log.Printf("[placement-long] failed to commit placement: %v", err)
			return
		}
		log.Printf("[placement-long] placement committed (epoch=%d)", pl.Epoch)
		return
	}

	r.SetPlacement(pl)
	log.Printf("[placement-long] placement updated locally (epoch=%d) with %d active nodes", pl.Epoch, len(scores))
}

func (r *Replicator) ApplyPlacementFromRaft(p *Placement) {
	if p == nil {
		return
	}
	r.SetPlacement(p)
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

func (r *Replicator) smoothMetricsLocked(raw NodeMetrics) NodeMetrics {
	const alpha = 0.25

	raw.CPUUsage = clampUnit(raw.CPUUsage)
	raw.MemUsage = clampUnit(raw.MemUsage)
	raw.NetUsage = clampUnit(raw.NetUsage)
	raw.DiskUsage = clampUnit(raw.DiskUsage)

	prev, ok := r.metricEMA[raw.NodeID]
	if !ok {
		r.metricEMA[raw.NodeID] = raw
		return raw
	}

	smoothed := raw
	smoothed.CPUUsage = alpha*raw.CPUUsage + (1-alpha)*prev.CPUUsage
	smoothed.MemUsage = alpha*raw.MemUsage + (1-alpha)*prev.MemUsage
	smoothed.NetUsage = alpha*raw.NetUsage + (1-alpha)*prev.NetUsage
	smoothed.DiskUsage = alpha*raw.DiskUsage + (1-alpha)*prev.DiskUsage

	if raw.AvgRTT > 0 {
		if prev.AvgRTT <= 0 {
			smoothed.AvgRTT = raw.AvgRTT
		} else {
			smoothed.AvgRTT = alpha*raw.AvgRTT + (1-alpha)*prev.AvgRTT
		}
	} else {
		smoothed.AvgRTT = prev.AvgRTT
	}

	if smoothed.CPUCores <= 0 {
		smoothed.CPUCores = prev.CPUCores
	}
	if smoothed.MemGB <= 0 {
		smoothed.MemGB = prev.MemGB
	}
	if smoothed.BandwidthMbps <= 0 {
		smoothed.BandwidthMbps = prev.BandwidthMbps
	}
	if smoothed.DiskGB <= 0 {
		smoothed.DiskGB = prev.DiskGB
	}

	r.metricEMA[raw.NodeID] = smoothed
	return smoothed
}

func averageRTT(metrics []NodeMetrics) float64 {
	if len(metrics) == 0 {
		return 0
	}
	var total float64
	var count int
	for _, m := range metrics {
		if m.AvgRTT > 0 {
			total += m.AvgRTT
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func formatScoreSummary(scores []NodeInfo, limit int) string {
	if len(scores) == 0 {
		return "none"
	}
	local := append([]NodeInfo(nil), scores...)
	sort.Slice(local, func(i, j int) bool {
		if local[i].Score == local[j].Score {
			return local[i].ID < local[j].ID
		}
		return local[i].Score < local[j].Score
	})
	if limit <= 0 || limit > len(local) {
		limit = len(local)
	}
	parts := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		parts = append(parts, fmt.Sprintf("%s=%.4f", local[i].ID, local[i].Score))
	}
	return strings.Join(parts, ", ")
}

func formatVNodeCounts(counts map[string]int) string {
	if len(counts) == 0 {
		return "none"
	}
	ids := make([]string, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%s=%d", id, counts[id]))
	}
	return strings.Join(parts, ", ")
}

func formatBoolNodeSet(nodes map[string]bool) string {
	if len(nodes) == 0 {
		return "none"
	}
	ids := make([]string, 0, len(nodes))
	for id, ok := range nodes {
		if ok {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return "none"
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

func isSeverelyDegraded(m NodeMetrics, clusterAvgRTT float64) bool {
	if m.CPUUsage >= 0.90 {
		return true
	}
	if m.DiskUsage >= 0.90 {
		return true
	}

	rttThreshold := 120.0
	if clusterAvgRTT > 0 {
		candidate := 2.0 * clusterAvgRTT
		if candidate > rttThreshold {
			rttThreshold = candidate
		}
	}
	if m.AvgRTT > 0 && m.AvgRTT >= rttThreshold {
		return true
	}

	// Error-rate signal is not yet available in NodeMetrics.
	return false
}

func clampUnit(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func parseMeta(meta []byte) (nodeID string, rpcAddr string, err error) {
	raw := strings.TrimSpace(string(meta))
	if raw == "" {
		return "", "", errors.New("empty member metadata")
	}

	parts := strings.Split(raw, ",")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid member metadata format (expected 'nodeID,rpcAddr[,raftAddr]'): %q", raw)
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid member metadata fields: %q", raw)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
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
	r.setNodeRPCAddr(peerID, rpcAddr)
	return r.getOrCreateClientByAddr(peerID, rpcAddr)
}

func (r *Replicator) getOrCreateClientByAddr(nodeID, rpcAddr string) (pb.KVClient, error) {
    // 1. Fast path read lock
    r.mu.RLock()
    if c, ok := r.peers[nodeID]; ok {
        r.mu.RUnlock()
        return c, nil
    }
    r.mu.RUnlock()

    // 2. Dial OUTSIDE the lock (prevents stalling the whole node)
    conn, err := grpc.NewClient(rpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }
    client := pb.NewKVClient(conn)

    // 3. Write lock just to save it
    r.mu.Lock()
    defer r.mu.Unlock()
    // Double-check someone else didn't make it while we were dialing
    if c, ok := r.peers[nodeID]; ok {
        conn.Close() // throw ours away
        return c, nil
    }
    r.peers[nodeID] = client
    r.conns[nodeID] = conn
    return client, nil
}
func (r *Replicator) setNodeRPCAddr(nodeID, rpcAddr string) {
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(rpcAddr) == "" {
		return
	}
	r.nodeAddrMu.Lock()
	r.nodeRPCAddrs[nodeID] = rpcAddr
	r.nodeAddrMu.Unlock()
}

func (r *Replicator) getNodeRPCAddr(nodeID string) (string, bool) {
	r.nodeAddrMu.RLock()
	addr, ok := r.nodeRPCAddrs[nodeID]
	r.nodeAddrMu.RUnlock()
	return addr, ok
}

func (r *Replicator) getOrCreateClientByNodeID(nodeID string) (pb.KVClient, error) {
	r.mu.Lock()
	if c, ok := r.peers[nodeID]; ok {
		r.mu.Unlock()
		return c, nil
	}
	r.mu.Unlock()

	if rpcAddr, ok := r.getNodeRPCAddr(nodeID); ok {
		return r.getOrCreateClientByAddr(nodeID, rpcAddr)
	}

	if r.Ml == nil {
		return nil, errors.New("memberlist is nil")
	}
	for _, member := range r.Ml.Members() {
		peerID, rpcAddr, err := parseMeta(member.Meta)
		if err != nil {
			continue
		}
		r.setNodeRPCAddr(peerID, rpcAddr)
		if peerID == nodeID {
			return r.getOrCreateClientByAddr(peerID, rpcAddr)
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
		log.Printf("[replicator] no placement targets for key=%q; keeping write local only", key)
		return nil
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

	for attempt := range maxAttempts {
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
func (r *Replicator) CheckOwnership(key string) (bool, bool, string) {
	pl := r.getPlacement()
	if pl == nil {
		return true, false, ""
	}
	owner, replicas := pl.GetNodesForKey(key)
	return owner == r.ID, slices.Contains(replicas, r.ID), owner
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

	existingInRaft := make(map[string]raft.Server)
	for _, s := range cfg.Servers {
		existingInRaft[string(s.ID)] = s
	}

	// 1. Mark who we see in Gossip
	seenInGossip := make(map[string]struct{})
	for _, m := range members {
		nodeID, rpcAddr, raftAddr, err := parseMetaWithRaft(m.Meta)
		if err != nil || raftAddr == "" {
			continue
		}
		r.setNodeRPCAddr(nodeID, rpcAddr)
		seenInGossip[nodeID] = struct{}{}

		// If it's a new node, add it to Raft immediately
		if _, ok := existingInRaft[nodeID]; !ok {
			if err := r.Rf.AddVoter(nodeID, raftAddr, 5*time.Second); err != nil {
				_ = err
			}
		}
	}

	// 2. Handle nodes that are in Raft but MISSING from Gossip
	r.deadRowMu.Lock()
	defer r.deadRowMu.Unlock()

	for id, server := range existingInRaft {
		if id == r.ID {
			continue // Don't remove ourselves!
		}

		// If the node is healthy in Gossip, make sure it's off Death Row
		if _, ok := seenInGossip[id]; ok {
			delete(r.deadRow, id)
			continue
		}

		// Node is in Raft but MISSING from Gossip!
		firstSeenMissing, tracking := r.deadRow[id]
		if !tracking {
			r.deadRow[id] = time.Now()
			continue
		}

		// Node is on Death Row. Let's calculate its execution time.
		gracePeriod := r.GracePeriod
		if gracePeriod == 0 {
			gracePeriod = 5 * time.Minute // Safe default
		}

		// --- CHECK IF IT IS A BACKUP (Non-Voter) ---
		// If it's just a backup (NonVoter), we can use a shorter grace period.
		// If it's a Voter, we wait the full duration to protect quorum.
		if server.Suffrage == raft.Nonvoter {
			gracePeriod = 1 * time.Minute // Shorter window for non-critical backups
		}

		if time.Since(firstSeenMissing) > gracePeriod {
			if err := r.Rf.RemoveServer(id, 5*time.Second); err != nil {
				_ = err
			} else {
				delete(r.deadRow, id) // Clean up after successful removal

				r.mu.Lock()
				if conn, ok := r.conns[id]; ok {
					conn.Close()
					delete(r.conns, id)
					delete(r.peers, id)
				}
				r.mu.Unlock()

				r.metricsMu.Lock()
				delete(r.metricEMA, id)
				delete(r.rttMatrix, id)
				r.metricsMu.Unlock()
			}
		}
	}

	// After reconciling membership, recompute and commit placement as leader.
	r.UpdatePlacement()

	return nil
}

func (r *Replicator) ForwardToOwner(ctx context.Context, owner string, req any) ([]byte, error) {
	client, err := r.getOrCreateClientByNodeID(owner)
	var val []byte = nil
	if err != nil {
		return nil, err
	}
	switch req := req.(type) {
	case *pb.PutRequest:
		_, err := client.Put(ctx, req)
		if err != nil {
			return nil, err
		}

	case *pb.DeleteRequest:
		_, err := client.Delete(ctx, req)
		if err != nil {
			return nil, err
		}
	case *pb.GetRequest:
		resp, err := client.Get(ctx, req)
		if err != nil {
			return nil, err
		}
		val = resp.Value

	default:
		return nil, fmt.Errorf("unsupported request type: %T", req)
	}
	return val, nil
}
func (r *Replicator) Close() error {
	r.StopMetricsReporter()

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
