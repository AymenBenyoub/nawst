package core

import (
	"context"
	"errors"
	"io"
	"log"

	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/AymenBenyoub/nawst/core/proto"
	"github.com/AymenBenyoub/nawst/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Replicator interface {
	ReplicateToAll(ctx context.Context, op pb.Op, key string, value []byte, vnodeID uint16, version uint64) error
	CheckOwnership(key string) (bool, bool, string)
	GetVNodeForKey(key string) uint16 // Returns vnode ID for a key; used to annotate commands
	GetMigrationSourceForKey(key string) string
	SnapshotClusterState() *pb.ClusterState
}

type Server struct {
	pb.UnimplementedKVServer
	reqCh          chan<- Request
	Replicator     Replicator
	Store          *Store        // Store reference for snapshots during vnode transfer
	versionCounter atomic.Uint64 // Atomic counter for logical versioning; increments on each write
	rpcVerbose     bool
}

type Request struct {
	Op           OpType
	Key          string
	Value        []byte
	VNodeID      uint16 // Vnode owner for this key; computed by Server before sending
	Version      uint64 // Logical version for conflict resolution; incremented per write
	ResponseChan chan Response
}

type Response struct {
	Op    OpType
	Value []byte
	Err   error
}

func NewServer(reqCh chan<- Request, store *Store) *Server {
	return &Server{
		reqCh: reqCh,
		Store: store,
	}
}

func (s *Server) SetRPCVerbose(enabled bool) {
	s.rpcVerbose = enabled
}

func (s *Server) debugf(format string, args ...any) {
	if s.rpcVerbose {
		log.Printf(format, args...)
	}
}

func (s *Server) Start(port int) error {
	lis, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(port))
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVServer(grpcServer, s)
	reflection.Register(grpcServer)
	// clean shutdown on SIGINT/SIGTERM
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-stopCh
		log.Println("Shutting down gRPC server...")
		grpcServer.GracefulStop()
	}()

	log.Printf("gRPC server listening on address %s\n", lis.Addr())
	return grpcServer.Serve(lis)
}

func ownershipError(owner string, replica bool) error {
	if replica {
		return status.Errorf(codes.FailedPrecondition, "node is not responsible for key (owner=%s, replica=true)", owner)
	}
	return status.Errorf(codes.FailedPrecondition, "node is not responsible for key (owner=%s)", owner)
}

func (s *Server) sendRequest(ctx context.Context, req Request) Response {
	select {
	case s.reqCh <- req:
		// sent to event loop
	case <-ctx.Done():
		return Response{Err: status.Error(codes.DeadlineExceeded, "request cancelled")}
	}

	select {
	case resp := <-req.ResponseChan:
		return resp
	case <-ctx.Done():
		return Response{Err: status.Error(codes.DeadlineExceeded, "request cancelled")}
	}
}

// SendInternal allows trusted internal components (e.g., migration applier)
// to reuse the same EventLoop request path as gRPC requests.
func (s *Server) SendInternal(ctx context.Context, req Request) Response {
	return s.sendRequest(ctx, req)
}

func (s *Server) NextVersion() uint64 {
	for {
		cur := s.versionCounter.Load()
		cand := uint64(time.Now().UnixNano())
		if cand <= cur {
			cand = cur + 1
		}
		if s.versionCounter.CompareAndSwap(cur, cand) {
			return cand
		}
	}
}

// gRPC Put RPC
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*emptypb.Empty, error) {
	started := time.Now()
	result := "error"
	clientResult := "error"
	defer func() {
		observability.ObserveRPC("put", result, time.Since(started))
		observability.ObserveClientRequest("put", clientResult)
	}()
	s.debugf("[rpc] PUT request key=%q bytes=%d", req.Key, len(req.Value))
	isOwner, _, owner := s.Replicator.CheckOwnership(req.Key)
	if !isOwner {
		clientResult = "error"
		result = "rejected"
		return nil, ownershipError(owner, false)
	}

	// Compute vnode and version before sending request to EventLoop.
	// Vnode is deterministic from key hash; version increments per write.
	vnodeID := s.Replicator.GetVNodeForKey(req.Key)
	version := s.NextVersion()

	resp := s.sendRequest(ctx, Request{
		Op:           OpPut,
		Key:          req.Key,
		Value:        req.Value,
		VNodeID:      vnodeID,
		Version:      version,
		ResponseChan: make(chan Response, 1),
	})
	if resp.Err != nil {
		clientResult = "error"
		return nil, status.Errorf(codes.Internal, "Failed to PUT key: %v", resp.Err)
	}
	s.debugf("[rpc] PUT key=%q applied locally", req.Key)
	if s.Replicator != nil {
		repCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := s.Replicator.ReplicateToAll(repCtx, pb.Op_PUT, req.Key, req.Value, vnodeID, version); err != nil {
			cancel()
			clientResult = "error"
			return nil, status.Errorf(codes.Internal, "Failed to replicate PUT: %v", err)
		}

		cancel()
	}
	result = "ok"
	clientResult = "ok"
	s.debugf("[rpc] PUT key=%q completed", req.Key)
	return &emptypb.Empty{}, nil
}

// gRPC Get RPC

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	started := time.Now()
	result := "error"
	clientResult := "error"
	defer func() {
		observability.ObserveRPC("get", result, time.Since(started))
		observability.ObserveClientRequest("get", clientResult)
	}()
	s.debugf("[rpc] GET request key=%q", req.Key)
	val, version, err := s.Store.GetWithVersion(req.Key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			result = "not_found"
			clientResult = "error"
			return nil, status.Error(codes.NotFound, "key not found")
		}
		result = "error"
		clientResult = "error"
		return nil, status.Errorf(codes.Internal, "Failed to GET key: %v", err)
	}
	if req.GetMinVersion() > 0 && version < req.GetMinVersion() {
		result = "stale"
		clientResult = "error"
		return nil, status.Errorf(codes.FailedPrecondition, "stale read: local_version=%d min_version=%d", version, req.GetMinVersion())
	}
	result = "ok"
	clientResult = "ok"
	s.debugf("[rpc] GET key=%q served locally via direct store read", req.Key)
	return &pb.GetResponse{Value: val, Version: version}, nil
}

// gRPC Delete RPC
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*emptypb.Empty, error) {
	started := time.Now()
	result := "error"
	clientResult := "error"
	defer func() {
		observability.ObserveRPC("delete", result, time.Since(started))
		observability.ObserveClientRequest("delete", clientResult)
	}()
	s.debugf("[rpc] DELETE request key=%q", req.Key)
	isOwner, _, owner := s.Replicator.CheckOwnership(req.Key)
	if !isOwner {
		clientResult = "error"
		result = "rejected"
		return nil, ownershipError(owner, false)
	}

	// Compute vnode and version before sending request to EventLoop.
	vnodeID := s.Replicator.GetVNodeForKey(req.Key)
	version := s.NextVersion()

	resp := s.sendRequest(ctx, Request{
		Op:           OpDelete,
		Key:          req.Key,
		VNodeID:      vnodeID,
		Version:      version,
		ResponseChan: make(chan Response, 1),
	})
	if resp.Err != nil {
		clientResult = "error"
		return nil, status.Errorf(codes.Internal, "Failed to DELETE key: %v", resp.Err)
	}
	s.debugf("[rpc] DELETE key=%q applied locally", req.Key)
	if s.Replicator != nil {

		repCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := s.Replicator.ReplicateToAll(repCtx, pb.Op_DELETE, req.Key, nil, vnodeID, version); err != nil {
			cancel()
			clientResult = "error"
			return nil, status.Errorf(codes.Internal, "Failed to REPLICATE DELETE: %v", err)
		}

		cancel()
	}
	result = "ok"
	clientResult = "ok"
	s.debugf("[rpc] DELETE key=%q completed", req.Key)
	return &emptypb.Empty{}, nil
}

func (s *Server) applyReplicationBatch(ctx context.Context, req *pb.ReplicationBatchRequest) error {
	if req == nil || len(req.Requests) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(req.Requests))
	for _, item := range req.Requests {
		if item == nil {
			continue
		}
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := s.sendRequest(ctx, Request{
				Op: func() OpType {
					switch item.Op {
					case pb.Op_PUT:
						return OpPut
					case pb.Op_DELETE:
						return OpDelete
					default:
						return OpGet
					}
				}(),
				Key:          item.Key,
				Value:        item.Value,
				VNodeID:      uint16(item.VnodeId),
				Version:      item.Version,
				ResponseChan: make(chan Response, 1),
			})
			if resp.Err != nil {
				errCh <- resp.Err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) ReplicateBatch(ctx context.Context, req *pb.ReplicationBatchRequest) (*emptypb.Empty, error) {
	started := time.Now()
	result := "error"
	defer func() {
		observability.ObserveRPC("replicate_batch", result, time.Since(started))
	}()
	if err := s.applyReplicationBatch(ctx, req); err != nil {
		return nil, status.Errorf(codes.Internal, "replication batch failed: %v", err)
	}
	result = "ok"
	return &emptypb.Empty{}, nil
}

func (s *Server) ReplicateStream(stream pb.KV_ReplicateStreamServer) error {
	started := time.Now()
	result := "error"
	defer func() {
		observability.ObserveRPC("replicate_stream", result, time.Since(started))
	}()

	ctx := stream.Context()
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			result = "ok"
			return nil
		}
		if err != nil {
			return err
		}

		ack := &pb.ReplicationBatchAck{}
		if batch != nil {
			ack.Applied = uint32(len(batch.Requests))
		}
		if err := s.applyReplicationBatch(ctx, batch); err != nil {
			ack.Error = err.Error()
			if sendErr := stream.Send(ack); sendErr != nil {
				return sendErr
			}
			return status.Errorf(codes.Internal, "replication stream batch failed: %v", err)
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

func (s *Server) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.ClusterState, error) {
	if s.Replicator == nil {
		return &pb.ClusterState{}, nil
	}
	return s.Replicator.SnapshotClusterState(), nil
}
func (s *Server) Replicate(ctx context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	started := time.Now()
	result := "error"
	defer func() {
		observability.ObserveRPC("replicate", result, time.Since(started))
	}()
	s.debugf("[replicator] received replication request: op=%v key=%q", req.Op, req.Key)

	var op OpType
	switch req.Op {
	case pb.Op_PUT:
		op = OpPut
	case pb.Op_DELETE:
		op = OpDelete
	default:
		return nil, status.Error(codes.InvalidArgument, "Invalid operation type for replication")
	}

	resp := s.sendRequest(ctx, Request{
		Op:    op,
		Key:   req.Key,
		Value: req.Value,
		VNodeID: func() uint16 {
			if req.VnodeId != 0 {
				return uint16(req.VnodeId)
			}
			return s.Replicator.GetVNodeForKey(req.Key)
		}(),
		Version: func() uint64 {
			if req.Version != 0 {
				return req.Version
			}
			return s.NextVersion()
		}(),
		ResponseChan: make(chan Response, 1),
	})
	if resp.Err != nil {
		return nil, status.Errorf(codes.Internal, "Replication failed: %v", resp.Err)
	}
	result = "ok"

	s.debugf("[replicator] applied replicated request: op=%v key=%q", req.Op, req.Key)
	return &emptypb.Empty{}, nil
}
func (s *Server) StreamKV(stream pb.KV_StreamKVServer) error {
	// Buffer to hold ACKs coming back from the EventLoop
	respCh := make(chan Response, 10000)
	ctx := stream.Context()

	var inFlight sync.WaitGroup
	sendErrCh := make(chan error, 1)

	// Receiver Goroutine: Streams ACKs back to the client
	go func() {
		for {
			select {
			case <-ctx.Done():
				// The client violently disconnected or timed out. Exit cleanly.
				return
			case resp := <-respCh:
				inFlight.Done()

				errStr := ""
				if resp.Err != nil {
					errStr = resp.Err.Error()
				}
				var protoOp pb.Op
				if resp.Op == OpPut {
					protoOp = pb.Op_PUT
				} else if resp.Op == OpGet {
					protoOp = pb.Op_GET
				} else {
					protoOp = pb.Op_DELETE
				}
				// Blast the response back to the client
				err := stream.Send(&pb.StreamResp{
					Op:    protoOp,
					Value: resp.Value,
					Error: errStr,
				})
				if err != nil {
					select {
					case sendErrCh <- err:
					default:
					}
					return
				}
			}
		}
	}()

	// Sender Loop: Reads from the client stream
	for {
		select {
		case err := <-sendErrCh:
			return err
		default:
		}

		req, err := stream.Recv()
		if err == io.EOF {
			break // Client sent all requests and called CloseSend()
		}
		if err != nil {
			return err
		}

		var coreOp OpType
		switch req.Op {
		case pb.Op_PUT:
			coreOp = OpPut
		case pb.Op_GET:
			coreOp = OpGet
		default:
			coreOp = OpDelete
		}
		log.Printf("[rpc-stream] request op=%v key=%q bytes=%d", req.Op, req.Key, len(req.Value))

		// Track that we have a new request in flight
		inFlight.Add(1)

		// Send to EventLoop
		vnodeID := s.Replicator.GetVNodeForKey(req.Key)
		version := uint64(0)
		if coreOp == OpPut || coreOp == OpDelete {
			version = s.NextVersion()
		}
		s.reqCh <- Request{
			Op:           coreOp,
			Key:          req.Key,
			Value:        req.Value,
			VNodeID:      vnodeID,
			Version:      version,
			ResponseChan: respCh,
		}
	}

	// Graceful drain without polling: wait until all pending responses are observed.
	drainDone := make(chan struct{})
	go func() {
		inFlight.Wait()
		close(drainDone)
	}()

	select {
	case err := <-sendErrCh:
		return err
	case <-drainDone:
		select {
		case err := <-sendErrCh:
			return err
		default:
		}
		return nil
	case <-ctx.Done():
		select {
		case err := <-sendErrCh:
			return err
		default:
		}
		return ctx.Err()
	}
}

// TransferVNode streams a snapshot of all key-value pairs owned by a vnode.
// Client initiates transfer; server responds with all entries for that vnode.
// Used during placement migration: receiving node applies snapshot to catch up.
// Receiver reconstructs vnode index on apply; sender includes version for conflict detection.
func (s *Server) TransferVNode(stream pb.KV_TransferVNodeServer) error {
	started := time.Now()
	result := "error"
	defer func() {
		observability.ObserveRPC("transfer_vnode", result, time.Since(started))
	}()
	// Receive first request with vnode ID and placement epoch
	req, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "Failed to receive transfer request: %v", err)
	}

	vnodeID := uint16(req.VnodeId)
	epoch := req.PlacementEpoch

	log.Printf("[transfer] starting vnode=%d snapshot (epoch=%d)", vnodeID, epoch)

	// Snapshot all keys owned by this vnode
	snapshot := s.Store.SnapshotVNode(vnodeID)

	// Stream back each entry as a separate response
	for _, entry := range snapshot.Entries {
		err := stream.Send(&pb.VNodeTransferResp{
			VnodeId:        uint32(vnodeID),
			PlacementEpoch: epoch,
			Key:            entry.Key,
			Value:          entry.Value,
			Version:        entry.Version,
			EmptyEntries:   false,
			Error:          "",
			Tombstone:      entry.Tombstone,
		})
		if err != nil {
			log.Printf("[transfer] failed to send entry for vnode=%d key=%q: %v", vnodeID, entry.Key, err)
			return status.Errorf(codes.Internal, "Failed to send snapshot entry: %v", err)
		}
	}

	// Send end-of-stream marker
	err = stream.Send(&pb.VNodeTransferResp{
		VnodeId:        uint32(vnodeID),
		PlacementEpoch: epoch,
		EmptyEntries:   true,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "Failed to send end-of-stream: %v", err)
	}

	result = "ok"
	log.Printf("[transfer] completed vnode=%d snapshot (%d entries)", vnodeID, len(snapshot.Entries))
	return nil
}
