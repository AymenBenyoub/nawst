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
	clients map[string]pb.KVClient

	readCursor atomic.Uint64
}

func NewPlacementRouter(bootstrapAddr string) *PlacementRouter {
	return &PlacementRouter{
		bootstrapAddr: strings.TrimSpace(bootstrapAddr),
		conns:         make(map[string]*grpc.ClientConn),
		clients:       make(map[string]pb.KVClient),
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
	if client, ok := r.clients[nodeID]; ok {
		r.connMu.Unlock()
		return client, nil
	}
	r.connMu.Unlock()

	conn, err := grpc.NewClient(rpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewKVClient(conn)

	r.connMu.Lock()
	defer r.connMu.Unlock()
	if existing, ok := r.clients[nodeID]; ok {
		_ = conn.Close()
		return existing, nil
	}
	r.conns[nodeID] = conn
	r.clients[nodeID] = client
	return client, nil
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

	if readAnyNode {
		targets := make([]routeTarget, 0, len(state.Nodes))
		for _, node := range state.Nodes {
			rpcAddr := strings.TrimSpace(node.GetRpcAddr())
			if node.GetId() == "" || rpcAddr == "" {
				continue
			}
			targets = append(targets, routeTarget{nodeID: node.GetId(), rpcAddr: rpcAddr})
		}
		if len(targets) == 0 {
			return nil, false
		}
		offset := int(r.readCursor.Add(1) % uint64(len(targets)))
		rotated := make([]routeTarget, 0, len(targets))
		rotated = append(rotated, targets[offset:]...)
		rotated = append(rotated, targets[:offset]...)
		return rotated, true
	}

	if len(state.Vnodes) != VNodeCount {
		return nil, false
	}

	vnodeID := vnodeIDForKey(key)
	if int(vnodeID) >= len(state.Vnodes) {
		return nil, false
	}
	vnode := state.Vnodes[vnodeID]
	owner := vnode.Primary
	if owner == "" {
		return nil, false
	}

	addrByNode := make(map[string]string, len(state.Nodes))
	for _, node := range state.Nodes {
		addrByNode[node.GetId()] = node.GetRpcAddr()
	}

	primaryAddr := strings.TrimSpace(addrByNode[owner])
	if primaryAddr == "" {
		return nil, false
	}

	return []routeTarget{{nodeID: owner, rpcAddr: primaryAddr}}, true
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
			if ok && st.Code() == codes.NotFound {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if ok && st.Code() == codes.FailedPrecondition {
				if firstErr == nil {
					firstErr = err
				}
				continue
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
