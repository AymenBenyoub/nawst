package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/AymenBenyoub/nawst/core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PlacementRouter routes client traffic directly to the current vnode primary.
type PlacementRouter struct {
	bootstrapAddr string

	mu    sync.RWMutex
	state *pb.ClusterState

	bootstrapMu     sync.Mutex
	bootstrapConn   *grpc.ClientConn
	bootstrapClient pb.KVClient

	connMu  sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]*clientPool

	readCursor atomic.Uint64
}
type clientPool struct {
	conns   []*grpc.ClientConn
	clients []pb.KVClient
	next    atomic.Uint64
}

func NewPlacementRouter(bootstrapAddr string) *PlacementRouter {
	return &PlacementRouter{
		bootstrapAddr: strings.TrimSpace(bootstrapAddr),
		conns:         make(map[string]*grpc.ClientConn),
		clients:       make(map[string]*clientPool),
	}
}

func (r *PlacementRouter) Close() error {
	r.connMu.Lock()
	defer r.connMu.Unlock()

	var firstErr error
	if r.bootstrapConn != nil {
		if err := r.bootstrapConn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.bootstrapConn = nil
		r.bootstrapClient = nil
	}
	for nodeID, conn := range r.conns {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close connection to %s: %w", nodeID, err)
		}
		delete(r.conns, nodeID)
		delete(r.clients, nodeID)
	}
	return firstErr
}

func (r *PlacementRouter) Refresh(ctx context.Context) error {
	client, err := r.bootstrapKVClient()
	if err != nil {
		return err
	}
	state, err := client.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.state = state
	r.mu.Unlock()
	return nil
}

func (r *PlacementRouter) bootstrapKVClient() (pb.KVClient, error) {
	r.bootstrapMu.Lock()
	defer r.bootstrapMu.Unlock()

	if r.bootstrapClient != nil {
		return r.bootstrapClient, nil
	}
	if r.bootstrapAddr == "" {
		return nil, errors.New("bootstrap address is empty")
	}

	conn, err := grpc.NewClient(r.bootstrapAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	r.bootstrapConn = conn
	r.bootstrapClient = pb.NewKVClient(conn)
	return r.bootstrapClient, nil
}

func (r *PlacementRouter) clientForNode(nodeID, rpcAddr string) (pb.KVClient, error) {
	nodeID = strings.TrimSpace(nodeID)
	rpcAddr = strings.TrimSpace(rpcAddr)
	if nodeID == "" || rpcAddr == "" {
		return r.bootstrapKVClient()
	}

	r.connMu.Lock()
	pool, ok := r.clients[nodeID]
	r.connMu.Unlock()

	if ok {
		idx := int(pool.next.Add(1) % uint64(len(pool.clients)))
		return pool.clients[idx], nil
	}

	// create pool
	const poolSize = 4

	conns := make([]*grpc.ClientConn, 0, poolSize)
	clients := make([]pb.KVClient, 0, poolSize)

	for i := 0; i < poolSize; i++ {
		conn, err := grpc.NewClient(
			rpcAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(16<<20),
				grpc.MaxCallSendMsgSize(16<<20),
			),
		)
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
		clients = append(clients, pb.NewKVClient(conn))
	}

	pool = &clientPool{
		conns:   conns,
		clients: clients,
	}

	r.connMu.Lock()
	r.clients[nodeID] = pool
	r.connMu.Unlock()

	return clients[0], nil
}

func (r *PlacementRouter) snapshotState() *pb.ClusterState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func vnodeIDForKey(key string) uint16 {
	token := HashKey(key)
	idx, _ := bits.Mul64(token, uint64(VNodeCount))
	return uint16(idx)
}

type routeTarget struct {
	nodeID  string
	rpcAddr string
}

func (r *PlacementRouter) routeForKey(key string, readAnyNode bool) ([]routeTarget, bool) {
	state := r.snapshotState()
	if state == nil {
		return nil, false
	}

	if len(state.Vnodes) != VNodeCount {
		return nil, false
	}

	vnodeID := vnodeIDForKey(key)
	if int(vnodeID) >= len(state.Vnodes) {
		return nil, false
	}

	vnode := state.Vnodes[vnodeID]
	if vnode.Primary == "" {
		return nil, false
	}

	// Build nodeID -> addr map
	addrByNode := make(map[string]string, len(state.Nodes))
	for _, node := range state.Nodes {
		addrByNode[node.GetId()] = strings.TrimSpace(node.GetRpcAddr())
	}

	// Always include primary first
	targets := make([]routeTarget, 0, 1+len(vnode.Replicas))

	if addr := addrByNode[vnode.Primary]; addr != "" {
		targets = append(targets, routeTarget{
			nodeID:  vnode.Primary,
			rpcAddr: addr,
		})
	}

	// If readAnyNode → include replicas
	if readAnyNode {
		for _, replica := range vnode.Replicas {
			if addr := addrByNode[replica]; addr != "" {
				targets = append(targets, routeTarget{
					nodeID:  replica,
					rpcAddr: addr,
				})
			}
		}

		// rotate for load balancing
		if len(targets) > 1 {
			offset := int(r.readCursor.Add(1) % uint64(len(targets)))
			rotated := make([]routeTarget, 0, len(targets))
			rotated = append(rotated, targets[offset:]...)
			rotated = append(rotated, targets[:offset]...)
			return rotated, true
		}
	}

	// 	if readAnyNode {
	//     vnodeID := vnodeIDForKey(key)
	//     vnode := state.Vnodes[vnodeID]

	//     candidates := make([]routeTarget, 0, 1+len(vnode.Replicas))

	//     addrByNode := make(map[string]string)
	//     for _, n := range state.Nodes {
	//         addrByNode[n.Id] = n.RpcAddr
	//     }

	//     if addr := addrByNode[vnode.Primary]; addr != "" {
	//         candidates = append(candidates, routeTarget{vnode.Primary, addr})
	//     }

	//     for _, rID := range vnode.Replicas {
	//         if addr := addrByNode[rID]; addr != "" {
	//             candidates = append(candidates, routeTarget{rID, addr})
	//         }
	//     }

	//     if len(candidates) == 0 {
	//         return nil, false
	//     }

	//     return candidates, true
	// }
	return targets, true
}

func (r *PlacementRouter) rpcWithPlacement(ctx context.Context, key string, readAnyNode bool, call func(pb.KVClient) error) error {
	invoke := func() error {
		targets, ok := r.routeForKey(key, readAnyNode)
		if !ok || len(targets) == 0 {
			return errors.New("placement not ready for key routing")
		}

		var firstErr error
		for _, target := range targets {
			client, err := r.clientForNode(target.nodeID, target.rpcAddr)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			err = call(client)
			if err == nil {
				return nil
			}

			st, ok := status.FromError(err)
			if ok {
				switch st.Code() {
				case codes.Unavailable, codes.DeadlineExceeded:
					// retry other nodes
					if firstErr == nil {
						firstErr = err
					}
					continue

				case codes.NotFound:
					// valid result → key doesn't exist
					return err

				case codes.FailedPrecondition:
					// stale routing → trigger refresh
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
			}
			return err
		}
		if firstErr != nil {
			return firstErr
		}
		return errors.New("no route candidates available")
	}

	if err := invoke(); err == nil {
		return nil
	} else {
		refreshCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = r.Refresh(refreshCtx)
		cancel()
		return invoke()
	}
}

func (r *PlacementRouter) GetWithMinVersion(ctx context.Context, key string, minVersion uint64) ([]byte, uint64, error) {
	var value []byte
	var version uint64
	err := r.rpcWithPlacement(ctx, key, true, func(client pb.KVClient) error {
		resp, err := client.Get(ctx, &pb.GetRequest{Key: key, MinVersion: minVersion})
		if err != nil {
			return err
		}
		value = append([]byte(nil), resp.GetValue()...)
		version = resp.GetVersion()
		return nil
	})
	return value, version, err
}

func (r *PlacementRouter) Get(ctx context.Context, key string) ([]byte, error) {
	value, _, err := r.GetWithMinVersion(ctx, key, 0)
	return value, err
}

func (r *PlacementRouter) Put(ctx context.Context, key string, value []byte) error {
	return r.rpcWithPlacement(ctx, key, false, func(client pb.KVClient) error {
		_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		return err
	})
}

func (r *PlacementRouter) Delete(ctx context.Context, key string) error {
	return r.rpcWithPlacement(ctx, key, false, func(client pb.KVClient) error {
		_, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
		return err
	})
}

func (r *PlacementRouter) StateSummary() string {
	state := r.snapshotState()
	if state == nil {
		return "<no placement>"
	}
	parts := make([]string, 0, len(state.Nodes))
	for _, node := range state.Nodes {
		parts = append(parts, fmt.Sprintf("%s=%s", node.GetId(), node.GetRpcAddr()))
	}
	sort.Strings(parts)
	return fmt.Sprintf("epoch=%d nodes=%s", state.GetEpoch(), strings.Join(parts, ","))
}
