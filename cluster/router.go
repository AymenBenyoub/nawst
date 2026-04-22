package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/AymenBenyoub/nawst/core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func (r *PlacementRouter) ownerForKey(key string) (nodeID string, rpcAddr string, ok bool) {
	state := r.snapshotState()
	if state == nil || len(state.Vnodes) != VNodeCount {
		return "", "", false
	}

	vnodeID := vnodeIDForKey(key)
	if int(vnodeID) >= len(state.Vnodes) {
		return "", "", false
	}
	owner := state.Vnodes[vnodeID].Primary
	if owner == "" {
		return "", "", false
	}
	for _, node := range state.Nodes {
		if node.GetId() == owner {
			return owner, node.GetRpcAddr(), true
		}
	}
	return owner, "", false
}

func (r *PlacementRouter) clientForKey(key string) (pb.KVClient, string, error) {
	if nodeID, rpcAddr, ok := r.ownerForKey(key); ok {
		client, err := r.clientForNode(nodeID, rpcAddr)
		if err == nil {
			return client, nodeID, nil
		}
	}
	client, err := r.bootstrapKVClient()
	return client, "", err
}

func (r *PlacementRouter) retryWithRefresh(ctx context.Context, key string, call func(pb.KVClient) error) error {
	client, _, err := r.clientForKey(key)
	if err != nil {
		return err
	}
	if err := call(client); err == nil {
		return nil
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_ = r.Refresh(refreshCtx)
	cancel()
	client, _, err = r.clientForKey(key)
	if err != nil {
		return err
	}
	return call(client)
}

func (r *PlacementRouter) Get(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	err := r.retryWithRefresh(ctx, key, func(client pb.KVClient) error {
		resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		value = append([]byte(nil), resp.GetValue()...)
		return nil
	})
	return value, err
}

func (r *PlacementRouter) Put(ctx context.Context, key string, value []byte) error {
	return r.retryWithRefresh(ctx, key, func(client pb.KVClient) error {
		_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		return err
	})
}

func (r *PlacementRouter) Delete(ctx context.Context, key string) error {
	return r.retryWithRefresh(ctx, key, func(client pb.KVClient) error {
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
