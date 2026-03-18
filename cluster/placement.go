package cluster

import (
	"log"
	"math"
	"math/bits"
	"sort"

	"github.com/cespare/xxhash/v2"
)

const (
	VNodeCount = 1024

	// MaxUint64 represents the maximum possible hash value.
	MaxUint64 = ^uint64(0)

	// TokenBucketSize statically divides the keyspace into 1024 equal chunks.
	TokenBucketSize = MaxUint64 / uint64(VNodeCount)
)

type VNode struct {
	ID       uint16   // index, 0..1023
	Token    uint64   // The absolute starting boundary of this VNode's keyspace
	Primary  string   // The Node ID of the Primary
	Replicas []string // The Node IDs of the Replicas
}

type NodeInfo struct {
	ID    string
	Score float64 // The dynamic cost score (lower is better)
}

type Placement struct {
	Epoch  uint64
	Nodes  []NodeInfo
	VNodes []VNode // Will always have exactly 1024 elements
}

// HashKey hashes the client's key into the uint64 keyspace
func HashKey(key string) uint64 {
	return xxhash.Sum64String(key)
}

// InitializeVNodes runs ONCE at cluster bootstrap.
// It defines the static tokens for all 1024 VNodes forever.
func InitializeVNodes() []VNode {
	vnodes := make([]VNode, VNodeCount)
	for i := range VNodeCount {
		vnodes[i] = VNode{
			ID:    uint16(i),
			Token: uint64(i) * TokenBucketSize,
		}
	}
	return vnodes
}

// GetNodesForKey uses O(1) arithmetic division to find the VNode instantly.
// No binary search, no sorting required.
func (p *Placement) GetNodesForKey(key string) (primary string, replicas []string) {
	if len(p.VNodes) != VNodeCount {
		return "", nil // Safety check: Shard map not initialized
	}

	token := HashKey(key)
	// O(1) instant lookup using full-range scaling with no tail-bucket skew.
	vnodeIdx, _ := bits.Mul64(token, uint64(VNodeCount))

	vnode := p.VNodes[vnodeIdx]
	return vnode.Primary, vnode.Replicas
}

// GetTargetVNodeCount calculates how many VNodes each node deserves based on its Score.
func (p *Placement) GetTargetVNodeCount(nodes []NodeInfo) map[string]int {
	counts := make(map[string]int)
	if len(nodes) == 0 {
		return counts
	}

	local := append([]NodeInfo(nil), nodes...)

	type alloc struct {
		id   string
		base int
		frac float64
	}

	const eps = 1e-9
	weights := make([]float64, len(local))
	var total float64

	for i, n := range local {
		s := n.Score
		if s < 0 || math.IsNaN(s) || math.IsInf(s, 0) {
			s = 1.0
		}
		w := 1.0 / (s + eps)
		weights[i] = w
		total += w
	}

	if total <= 0 || math.IsNaN(total) || math.IsInf(total, 0) {
		each := VNodeCount / len(local)
		rem := VNodeCount % len(local)
		for i, n := range local {
			c := each
			if i < rem {
				c++
			}
			counts[n.ID] = c
		}
		return counts
	}

	allocs := make([]alloc, 0, len(local))
	assigned := 0
	for i, n := range local {
		ideal := (weights[i] / total) * float64(VNodeCount)
		base := int(math.Floor(ideal))
		allocs = append(allocs, alloc{id: n.ID, base: base, frac: ideal - float64(base)})
		assigned += base
	}

	sort.Slice(allocs, func(i, j int) bool {
		if allocs[i].frac == allocs[j].frac {
			return allocs[i].id < allocs[j].id
		}
		return allocs[i].frac > allocs[j].frac
	})

	rest := VNodeCount - assigned
	for i := range allocs {
		counts[allocs[i].id] = allocs[i].base
	}
	for i := range rest {
		counts[allocs[i].id]++
	}

	return counts
}
func (p *Placement) AssignVNodes(nodeCounts map[string]int) {
	// 1. Initialize VNodes if this is Epoch 0
	if len(p.VNodes) == 0 {
		p.VNodes = InitializeVNodes()
	}

	// 2. Count current allocations
	currentCounts := make(map[string]int)
	for _, v := range p.VNodes {
		if v.Primary != "" {
			currentCounts[v.Primary]++
		}
	}

	// 3. Calculate Deficits (Nodes that need MORE VNodes)
	// and unassign Surpluses (Nodes that have TOO MANY)
	var orphanedVNodes []uint16 // Store VNode IDs that need new homes

	for i := range p.VNodes {
		owner := p.VNodes[i].Primary
		if owner == "" {
			orphanedVNodes = append(orphanedVNodes, p.VNodes[i].ID)
			continue
		}

		// If the owner doesn't exist in the new map, or has too many, revoke it
		allowed, exists := nodeCounts[owner]
		if !exists || currentCounts[owner] > allowed {
			currentCounts[owner]--
			p.VNodes[i].Primary = "" // Strip the primary
			orphanedVNodes = append(orphanedVNodes, p.VNodes[i].ID)
		}
	}

	// 4. Calculate exactly who needs how many
	var nodesNeeding []string
	for nodeID, target := range nodeCounts {
		curr := currentCounts[nodeID]
		for j := 0; j < (target - curr); j++ {
			nodesNeeding = append(nodesNeeding, nodeID)
		}
	}

	// Sort deterministically to prevent flakiness
	sort.Strings(nodesNeeding)

	// 5. Assign the orphaned VNodes to the nodes with deficits
	if len(orphanedVNodes) != len(nodesNeeding) {
		// Panic or log fatal: Math is broken if these don't match exactly
		panic("Mismatch between orphaned VNodes and deficit counts")
	}

	for i, vnodeID := range orphanedVNodes {
		p.VNodes[vnodeID].Primary = nodesNeeding[i]
	}

	finalCounts := make(map[string]int)
	for _, v := range p.VNodes {
		if v.Primary != "" {
			finalCounts[v.Primary]++
		}
	}
	for nodeID, cnt := range finalCounts {
		log.Printf("placement: vnodes assigned node=%s count=%d", nodeID, cnt)
	}

	p.Epoch++
}

func (p *Placement) AssignReplicas(metrics []NodeMetrics, rttMatrix map[string]map[string]float64, rf int) {
	if rf <= 1 {
		for i := range p.VNodes {
			p.VNodes[i].Replicas = nil
		}
		return
	}

	// 1. Create a lookup map for metrics by NodeID for O(1) access
	statsMap := make(map[string]NodeMetrics)
	for _, m := range metrics {
		statsMap[m.NodeID] = m
	}

	for i := range p.VNodes {
		v := &p.VNodes[i]
		primaryID := v.Primary
		if primaryID == "" {
			continue // Can't assign replicas for an unowned VNode
		}

		type candidate struct {
			id    string
			score float64
		}
		var candidates []candidate

		for _, node := range p.Nodes {
			// Rule: A replica cannot be on the same physical node as the Primary
			if node.ID == primaryID {
				continue
			}

			m, exists := statsMap[node.ID]
			if !exists {
				continue
			}

			// RTT from the specific Primary to this potential Replica
			rtt := 1000.0
			if row, ok := rttMatrix[primaryID]; ok {
				if val, ok := row[node.ID]; ok && val > 0 {
					rtt = val
				}
			}

			// BW Penalty: Use the asymptotic formula on NetUsage (0.0 - 1.0)
			// This prevents picking a node that is currently saturated.
			bwPenalty := 1.0 / (1.01 - m.NetUsage)

			// Capacity Factor: Adjust for the physical size of the pipe.
			// Higher bandwidth capacity should LOWER the total cost.
			// (Assuming BandwidthMbps is the static capacity)
			capFactor := 1.0 / (float64(m.BandwidthMbps) + 1e-9)

			// Final Suitability Score: Lower is better.
			// We want low RTT, low Saturation, and high Capacity.
			suitability := rtt * bwPenalty * capFactor

			candidates = append(candidates, candidate{node.ID, suitability})
		}

		// Sort candidates by the best (lowest) suitability score
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].score == candidates[j].score {
				return candidates[i].id < candidates[j].id
			}
			return candidates[i].score < candidates[j].score
		})

		// Assign the top rf-1 candidates as replicas
		v.Replicas = []string{}
		for j := 0; j < rf-1 && j < len(candidates); j++ {
			v.Replicas = append(v.Replicas, candidates[j].id)
		}
	}

}
