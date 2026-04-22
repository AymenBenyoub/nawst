package cluster

import (
	"sort"

	pb "github.com/AymenBenyoub/nawst/core/proto"
)

// SnapshotClusterState returns the current placement plus node RPC addresses in the
// protobuf shape used by placement-aware clients.
func (r *Replicator) SnapshotClusterState() *pb.ClusterState {
	state := &pb.ClusterState{}

	pl := r.getPlacement()
	if pl != nil {
		state.Epoch = pl.Epoch
		state.Vnodes = make([]*pb.ClusterVNode, 0, len(pl.VNodes))
		for i := range pl.VNodes {
			v := pl.VNodes[i]
			state.Vnodes = append(state.Vnodes, &pb.ClusterVNode{
				Id:       uint32(v.ID),
				Primary:  v.Primary,
				Replicas: append([]string(nil), v.Replicas...),
			})
		}

		state.Nodes = make([]*pb.ClusterNode, 0, len(pl.Nodes))
		for _, node := range pl.Nodes {
			rpcAddr, _ := r.getNodeRPCAddr(node.ID)
			state.Nodes = append(state.Nodes, &pb.ClusterNode{
				Id:      node.ID,
				RpcAddr: rpcAddr,
				Score:   node.Score,
			})
		}
	}

	r.nodeAddrMu.RLock()
	if len(r.nodeRPCAddrs) > 0 {
		known := make(map[string]struct{}, len(state.Nodes))
		for _, node := range state.Nodes {
			known[node.Id] = struct{}{}
		}
		ids := make([]string, 0, len(r.nodeRPCAddrs))
		for id := range r.nodeRPCAddrs {
			if _, ok := known[id]; ok {
				continue
			}
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			state.Nodes = append(state.Nodes, &pb.ClusterNode{Id: id, RpcAddr: r.nodeRPCAddrs[id]})
		}
	}
	r.nodeAddrMu.RUnlock()

	return state
}
