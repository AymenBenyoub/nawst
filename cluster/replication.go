package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"slices"
	"sort"

	"strings"
	"sync"
	"time"

	"github.com/AymenBenyoub/nawst/core"
	pb "github.com/AymenBenyoub/nawst/core/proto"
	"github.com/AymenBenyoub/nawst/observability"
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

	verbose bool

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
	transferApplyFn   func(core.Command) error
	transferDropFn    func(uint16) error

	migMu           sync.RWMutex
	migrationSource map[uint16]string // vnode -> preferred source node during catch-up

	replicationMu      sync.Mutex
	replicationWorkers map[string]*replicationBatchWorker
}

func NewReplicator(id string, ml *memberlist.Memberlist, rf int) *Replicator {
	return &Replicator{
		ID:                 id,
		Ml:                 ml,
		peers:              make(map[string]pb.KVClient),
		conns:              make(map[string]*grpc.ClientConn),
		metrics:            []NodeMetrics{},
		rttMatrix:          make(map[string]map[string]float64),
		nodeRPCAddrs:       make(map[string]string),
		ReplicationFactor:  rf,
		stopCh:             make(chan struct{}),
		deadRow:            make(map[string]time.Time),
		GracePeriod:        2 * time.Second,
		metricEMA:          make(map[string]NodeMetrics),
		degradedStreak:     make(map[string]int),
		migrationSource:    make(map[uint16]string),
		replicationWorkers: make(map[string]*replicationBatchWorker),
	}
}

func (r *Replicator) SetRaft(rfNode *RaftNode) {
	r.Rf = rfNode
}

func (r *Replicator) SetVerbose(enabled bool) {
	r.mu.Lock()
	r.verbose = enabled
	r.mu.Unlock()
}

func (r *Replicator) debugf(format string, args ...any) {
	r.mu.RLock()
	enabled := r.verbose
	r.mu.RUnlock()
	if !enabled {
		return
	}
	log.Printf(format, args...)
}

// SetTransferApplier sets the function used to apply transferred vnode entries locally.
// Recommended implementation routes through the normal write path so WAL/event-loop semantics are preserved.
func (r *Replicator) SetTransferApplier(fn func(core.Command) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transferApplyFn = fn
}

func (r *Replicator) getTransferApplier() func(core.Command) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.transferApplyFn
}

func (r *Replicator) SetTransferDropper(fn func(uint16) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transferDropFn = fn
}

func (r *Replicator) getTransferDropper() func(uint16) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.transferDropFn
}

func (r *Replicator) setMigrationSource(vnodeID uint16, source string) {
	r.migMu.Lock()
	defer r.migMu.Unlock()
	if strings.TrimSpace(source) == "" {
		delete(r.migrationSource, vnodeID)
		return
	}
	r.migrationSource[vnodeID] = source
}

func (r *Replicator) clearMigrationSource(vnodeID uint16) {
	r.migMu.Lock()
	delete(r.migrationSource, vnodeID)
	r.migMu.Unlock()
}

func (r *Replicator) GetMigrationSourceForKey(key string) string {
	pl := r.getPlacement()
	if pl == nil {
		return ""
	}
	vnodeID := pl.GetVNodeIDForKey(key)
	r.migMu.RLock()
	src := r.migrationSource[vnodeID]
	r.migMu.RUnlock()
	return src
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
	shortEvalTicker := time.NewTicker(6 * interval)
	longEvalTicker := time.NewTicker(24 * interval)
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
	observability.ObserveLocalCapacity(
		m.CPUUsage,
		m.MemUsage,
		m.NetUsage,
		m.DiskUsage,
		m.AvgRTT,
		m.CPUCores,
		m.MemGB,
		m.BandwidthMbps,
		m.DiskGB,
	)
	if r.Ml != nil {
		observability.SetClusterMembers(len(r.Ml.Members()))
	} else {
		observability.SetClusterMembers(1)
	}

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
		if r.degradedStreak[m.NodeID] >= 6 {
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
	startedAt := time.Now()

	// Compare old placement to new to detect changes
	oldPl := r.getPlacement()
	r.SetPlacement(p)

	// If this is the first placement, skip migration (no old vnodes to compare)
	if oldPl == nil {
		myVNodes := 0
		var myReplicas []string
		for _, v := range p.VNodes {
			if v.Primary == r.ID {
				myVNodes++
				if len(myReplicas) == 0 && len(v.Replicas) > 0 {
					myReplicas = v.Replicas
				}
			}
		}
		log.Printf("[placement-raft] initial placement epoch=%d, assigned %d vnodes, replicating to: %v", p.Epoch, myVNodes, myReplicas)
		return
	}

	// Build vnode maps for old and new placement
	// oldMap[vnodeID] = nodeName (the old primary)
	// newMap[vnodeID] = nodeName (the new primary)
	oldMap := make(map[uint16]string, len(oldPl.VNodes))
	for _, v := range oldPl.VNodes {
		oldMap[v.ID] = v.Primary
	}

	newMap := make(map[uint16]string, len(p.VNodes))
	for _, v := range p.VNodes {
		newMap[v.ID] = v.Primary
	}

	// Detect changes: ownership and replica membership deltas.
	var gainedVNodes []uint16       // Newly promoted to primary
	var lostVNodes []uint16         // No longer primary
	var changedReplicas []uint16    // Still primary but replica set changed
	var addedReplicaVNodes []uint16 // Newly added as replica

	for vnodeID := range newMap {
		oldOwner := oldMap[vnodeID]
		newOwner := newMap[vnodeID]

		if oldOwner == r.ID && newOwner != r.ID {
			// Lost primary: was owner, no longer
			lostVNodes = append(lostVNodes, vnodeID)
		} else if oldOwner != r.ID && newOwner == r.ID {
			// Gained primary: now owner, wasn't before
			gainedVNodes = append(gainedVNodes, vnodeID)
		} else if oldOwner == newOwner && oldOwner == r.ID {
			// Still primary: check if replica set changed
			oldReplicas := oldPl.VNodes[vnodeID].Replicas
			newReplicas := p.VNodes[vnodeID].Replicas
			if !slices.Equal(oldReplicas, newReplicas) {
				changedReplicas = append(changedReplicas, vnodeID)
			}
		} else {
			oldReplicas := oldPl.VNodes[vnodeID].Replicas
			newReplicas := p.VNodes[vnodeID].Replicas
			if !slices.Contains(oldReplicas, r.ID) && slices.Contains(newReplicas, r.ID) {
				addedReplicaVNodes = append(addedReplicaVNodes, vnodeID)
			}
		}
	}

	log.Printf("[placement-raft] epoch=%d->%d: gained=%d lost=%d primary-replica-changes=%d new-replicas=%d",
		oldPl.Epoch, p.Epoch, len(gainedVNodes), len(lostVNodes), len(changedReplicas), len(addedReplicaVNodes))
	observability.ObservePlacementApply(p.Epoch, len(gainedVNodes), len(lostVNodes), len(changedReplicas), len(addedReplicaVNodes), time.Since(startedAt))

	// Start async migration goroutines (non-blocking return)
	go func() {
		// 1. Gain vnodes from old primary (or replicas if primary is down)
		for _, vnodeID := range gainedVNodes {
			observability.IncMigrationActive()
			oldOwner := oldMap[vnodeID]
			candidates := make([]string, 0, 1+len(oldPl.VNodes[vnodeID].Replicas)+len(p.VNodes[vnodeID].Replicas))
			if oldOwner != "" && oldOwner != r.ID {
				candidates = append(candidates, oldOwner)
			}
			for _, n := range oldPl.VNodes[vnodeID].Replicas {
				if n != "" && n != r.ID && !slices.Contains(candidates, n) {
					candidates = append(candidates, n)
				}
			}
			for _, n := range p.VNodes[vnodeID].Replicas {
				if n != "" && n != r.ID && !slices.Contains(candidates, n) {
					candidates = append(candidates, n)
				}
			}

			if len(candidates) > 0 {
				r.setMigrationSource(vnodeID, candidates[0])
			}

			var transferErr error
			success := false
			for _, src := range candidates {
				if err := r.transferVNodeFrom(src, vnodeID, p.Epoch); err != nil {
					transferErr = err
					log.Printf("[migration] transfer attempt failed vnode=%d source=%s err=%v", vnodeID, src, err)
					continue
				}
				log.Printf("[migration] successfully gained vnode=%d from %s", vnodeID, src)
				success = true
				break
			}
			if !success {
				log.Printf("[migration] failed to gain vnode=%d from all candidates=%v last_err=%v", vnodeID, candidates, transferErr)
			} else {
				r.clearMigrationSource(vnodeID)
			}
			observability.DecMigrationActive()
		}

		// 1b. Newly-added replicas must catch up from primary/other replicas.
		for _, vnodeID := range addedReplicaVNodes {
			observability.IncMigrationActive()
			owner := newMap[vnodeID]
			candidates := make([]string, 0, 1+len(p.VNodes[vnodeID].Replicas))
			if owner != "" && owner != r.ID {
				candidates = append(candidates, owner)
			}
			for _, rep := range p.VNodes[vnodeID].Replicas {
				if rep != "" && rep != r.ID && !slices.Contains(candidates, rep) {
					candidates = append(candidates, rep)
				}
			}

			var transferErr error
			success := false
			for _, src := range candidates {
				if err := r.transferVNodeFrom(src, vnodeID, p.Epoch); err != nil {
					transferErr = err
					continue
				}
				success = true
				break
			}
			if !success {
				log.Printf("[migration] failed to catch up new replica vnode=%d candidates=%v err=%v", vnodeID, candidates, transferErr)
			} else {
				log.Printf("[migration] caught up new replica vnode=%d", vnodeID)
			}
			observability.DecMigrationActive()
		}

		// 2. Delete lost vnodes after grace period.
		dropFn := r.getTransferDropper()
		if dropFn == nil {
			log.Printf("[migration] transfer dropper not configured; skipping lost vnode cleanup")
		}
		for _, vnodeID := range lostVNodes {
			if r.GracePeriod > 0 {
				time.Sleep(r.GracePeriod)
			}
			if dropFn != nil {
				if err := dropFn(vnodeID); err != nil {
					log.Printf("[migration] failed dropping lost vnode=%d: %v", vnodeID, err)
				} else {
					log.Printf("[migration] dropped lost vnode=%d", vnodeID)
				}
			}
			r.clearMigrationSource(vnodeID)
		}

		// 3. Update replica set for changed vnodes
		// (In future: may need to transfer to new replicas or notify old replicas)
		log.Printf("[migration] processing %d vnodes with replica set changes", len(changedReplicas))
	}()
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
	for attempt := 0; attempt < 3; attempt++ {
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
		if attempt < 2 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	return nil, fmt.Errorf("node %s not found in membership", nodeID)
}

func (r *Replicator) ReplicateToAll(ctx context.Context, op pb.Op, key string, value []byte, vnodeID uint16, version uint64) error {
	if r.Ml == nil {
		return errors.New("memberlist is nil")
	}

	members := r.Ml.Members()
	if len(members) == 0 {
		return nil
	}

	// local primary write already succeeded before this function is called
	acks := 1
	report := func(result string, targets int) {
		observability.ObserveReplicationQuorum(result, acks, targets)
	}

	effectiveRF := min(max(r.ReplicationFactor, 1), len(members))

	required := effectiveRF/2 + 1
	if acks >= required {
		report("ok", 0)
		return nil
	}

	targets := make([]string, 0, len(members)-1)
	var firstErr error

	resolveTargetsFromPlacement := func() {
		targets = targets[:0]
		pl := r.getPlacement()
		if pl == nil || len(pl.VNodes) != VNodeCount {
			return
		}

		_, replicas := pl.GetNodesForKey(key)
		r.debugf("[replicator] key=%q routes to replicas: %v", key, replicas)
		seen := make(map[string]struct{})
		for _, replicaID := range replicas {
			if replicaID == "" || replicaID == r.ID {
				continue
			}
			if _, exists := seen[replicaID]; exists {
				continue
			}
			seen[replicaID] = struct{}{}
			targets = append(targets, replicaID)
		}

	}

	resolveTargetsFromPlacement()

	if len(targets) == 0 {
		// Docker startup commonly reaches epoch 1 (single-node ring) before full metrics/raft settle.
		// If we can lead placement, force one immediate long-term refresh and retry once.
		if r.Rf != nil && r.Rf.IsLeader() {
			r.UpdatePlacement()
			resolveTargetsFromPlacement()
		}
	}

	if len(targets) == 0 && len(members) > 1 {
		// Strict placement adherence: if we know there are other members but Raft hasn't
		// given us a valid placement yet, return an error to force client retries instead
		// of silently falling back to a local-only write or gossip-based random targets.
		report("placement_not_ready", 0)
		return fmt.Errorf("placement not ready: waiting for Raft placement sync")
	}

	if len(targets) == 0 {
		log.Printf("[replicator] no placement targets for key=%q; keeping write local only", key)
		report("local_only", 0)
		return nil
	}
	if acks+len(targets) < required {
		if firstErr != nil {
			report("quorum_impossible", len(targets))
			return fmt.Errorf("quorum impossible: need %d acks, have %d local + %d remotes: %w", required, acks, len(targets), firstErr)
		}
		report("quorum_impossible", len(targets))
		return fmt.Errorf("quorum impossible: need %d acks, have %d local + %d remotes", required, acks, len(targets))
	}

	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan error, len(targets))
	req := &pb.ReplicationRequest{
		Op:      op,
		Key:     key,
		Value:   value,
		VnodeId: uint32(vnodeID),
		Version: version,
	}

	for _, targetID := range targets {
		done, err := r.submitReplicationTask(targetID, req)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			resultCh <- err
			continue
		}

		go func(pid string, done <-chan error) {
			select {
			case err := <-done:
				if err == nil {
					r.debugf("[replicator] replication batch to %s succeeded: key=%q", pid, key)
				} else {
					r.debugf("[replicator] replication batch to %s failed: key=%q error=%v", pid, key, err)
				}
				resultCh <- err
			case <-callCtx.Done():
				resultCh <- callCtx.Err()
			}
		}(targetID, done)
	}

	quorumReached := acks >= required
	remaining := len(targets)

	for remaining > 0 {
		select {
		case err := <-resultCh:
			remaining--

			if err == nil {
				acks++
				if acks >= required {
					quorumReached = true
					cancel()
					report("ok", len(targets))
					return nil
				}
			} else if firstErr == nil {
				firstErr = err
			}

			if !quorumReached && acks+remaining < required {
				cancel()
				if firstErr != nil {
					report("quorum_not_reached", len(targets))
					return fmt.Errorf("write quorum not reached: got %d/%d acks: %w", acks, required, firstErr)
				}
				report("quorum_not_reached", len(targets))
				return fmt.Errorf("write quorum not reached: got %d/%d acks", acks, required)
			}
		case <-callCtx.Done():
			if firstErr != nil {
				report("quorum_not_reached", len(targets))
				return fmt.Errorf("write quorum not reached: got %d/%d acks: %w", acks, required, firstErr)
			}
			report("quorum_not_reached", len(targets))
			return fmt.Errorf("write quorum not reached: got %d/%d acks: %w", acks, required, callCtx.Err())
		}
	}

	if quorumReached {
		report("ok", len(targets))
		return nil
	}
	if firstErr != nil {
		report("quorum_not_reached", len(targets))
		return fmt.Errorf("write quorum not reached: got %d/%d acks: %w", acks, required, firstErr)
	}
	report("quorum_not_reached", len(targets))
	return fmt.Errorf("write quorum not reached: got %d/%d acks", acks, required)
}

func (r *Replicator) ReplicatePut(ctx context.Context, key string, value []byte) error {
	vnodeID := r.GetVNodeForKey(key)
	return r.ReplicateToAll(ctx, pb.Op_PUT, key, value, vnodeID, 0)
}

func (r *Replicator) ReplicateDelete(ctx context.Context, key string) error {
	vnodeID := r.GetVNodeForKey(key)
	return r.ReplicateToAll(ctx, pb.Op_DELETE, key, nil, vnodeID, 0)
}
func (r *Replicator) CheckOwnership(key string) (bool, bool, string) {
	pl := r.getPlacement()
	if pl == nil {
		return true, false, ""
	}
	owner, replicas := pl.GetNodesForKey(key)
	return owner == r.ID, slices.Contains(replicas, r.ID), owner
}

// GetVNodeForKey returns the vnode ID that owns a key.
// Called by RPC layer to annotate commands with vnode ownership.
// O(1) operation using the same arithmetic division as GetNodesForKey.
func (r *Replicator) GetVNodeForKey(key string) uint16 {
	pl := r.getPlacement()
	if pl == nil {
		return 0 // Fallback if placement not initialized
	}
	return pl.GetVNodeIDForKey(key)
}

// transferVNodeFrom requests a vnode snapshot from a source node and applies it locally.
// Called during placement migration when this node becomes primary for a vnode it didn't own before.
// 1. Opens streaming RPC to source node
// 2. Requests vnode snapshot by ID
// 3. Streams entries back and applies each entry to local store
// 4. Returns error if source unreachable or snapshot transfer fails
//
// Concurrency: Runs async from ApplyPlacementFromRaft; safe to call multiple times concurrently
// (each call goes to different source nodes).
//
// Conflict resolution: Each entry includes version; local store may drop older versions.
// (TODO: Implement version-aware merge for concurrent writes during migration.)
func (r *Replicator) transferVNodeFrom(sourceNodeID string, vnodeID uint16, epoch uint64) error {
	startedAt := time.Now()
	entriesReceived := 0
	bytesReceived := 0
	result := "failed"
	defer func() {
		observability.ObserveTransfer(result, time.Since(startedAt), entriesReceived, bytesReceived)
	}()

	applyFn := r.getTransferApplier()
	if applyFn == nil {
		return fmt.Errorf("transfer applier not configured")
	}

	// Get RPC client for source node
	client, err := r.getOrCreateClientByNodeID(sourceNodeID)
	if err != nil {
		return fmt.Errorf("failed to get client for %s: %w", sourceNodeID, err)
	}

	// Open bidirectional stream
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := client.TransferVNode(ctx)
	if err != nil {
		return fmt.Errorf("failed to open transfer stream to %s: %w", sourceNodeID, err)
	}

	// Send request for this vnode
	if err := stream.Send(&pb.VNodeTransferReq{
		VnodeId:        uint32(vnodeID),
		PlacementEpoch: epoch,
	}); err != nil {
		return fmt.Errorf("failed to send transfer request: %w", err)
	}

	// Stream and apply entries
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break // Stream closed normally
		}
		if err != nil {
			return fmt.Errorf("failed to receive snapshot entry: %w", err)
		}

		// Check for error in response
		if resp.Error != "" {
			return fmt.Errorf("source node returned error: %s", resp.Error)
		}

		// End-of-stream marker
		if resp.EmptyEntries {
			log.Printf("[transfer] received end-of-stream for vnode=%d", vnodeID)
			break
		}

		// Apply entry locally
		// Entries have key, value, version from snapshot.
		// Apply to store with vnode context preserved.
		entriesReceived++
		bytesReceived += len(resp.Key) + len(resp.Value)

		// TODO: For now, skipping version-aware merge. In production:
		// - Compare resp.Version with local version for this key
		// - Drop if local version is newer (avoid overwriting concurrent writes)
		// - Apply if remote version is newer or key is new
		//
		// For this MVP, assume source is authoritative.
		op := core.OpPut
		if resp.Tombstone {
			op = core.OpDelete
		}
		cmd := core.Command{
			Op:      op,
			Key:     resp.Key,
			Value:   resp.Value,
			VNodeID: uint16(resp.VnodeId),
			Version: resp.Version,
		}
		if err := applyFn(cmd); err != nil {
			return fmt.Errorf("failed applying transferred entry key=%q vnode=%d: %w", resp.Key, resp.VnodeId, err)
		}

		// Applied through transfer applier callback, which should route through normal write path.
		// TODO: Add per-vnode replay queue to reduce interleaving during high write pressure.
	}

	log.Printf("[transfer] applied %d entries for vnode=%d from %s", entriesReceived, vnodeID, sourceNodeID)
	result = "success"
	return nil
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

func (r *Replicator) Close() error {
	r.StopMetricsReporter()
	r.stopReplicationWorkers()

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
