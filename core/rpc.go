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
	"syscall"
	"time"

	pb "github.com/AymenBenyoub/nawst/core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Replicator interface {
	ReplicateToAll(ctx context.Context, op pb.Op, key string, value []byte) error
	CheckOwnership(key string) (bool, bool, string)
	ForwardToOwner(ctx context.Context, owner string, req any) ([]byte, error)
}

type Server struct {
	pb.UnimplementedKVServer
	reqCh      chan<- Request
	Replicator Replicator
}

type Request struct {
	Op           OpType
	Key          string
	Value        []byte
	ResponseChan chan Response
}

type Response struct {
	Op    OpType
	Value []byte
	Err   error
}

func NewServer(reqCh chan<- Request) *Server {
	return &Server{
		reqCh: reqCh,
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

// gRPC Put RPC
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*emptypb.Empty, error) {
	is_powner, _, owner := s.Replicator.CheckOwnership(req.Key)
	if !is_powner {
		_, err := s.Replicator.ForwardToOwner(ctx, owner, req)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to forward PUT to owner %s: %v", owner, err)
		}
		log.Printf("Forwarded PUT request for key %q to owner %s", req.Key, owner)
		return &emptypb.Empty{}, nil
	} else {
		resp := s.sendRequest(ctx, Request{
			Op:           OpPut,
			Key:          req.Key,
			Value:        req.Value,
			ResponseChan: make(chan Response, 1),
		})
		if resp.Err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to PUT key: %v", resp.Err)
		}
		if s.Replicator != nil {
			repCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := s.Replicator.ReplicateToAll(repCtx, pb.Op_PUT, req.Key, req.Value); err != nil {
				cancel()
				return nil, status.Errorf(codes.Internal, "Failed to replicate PUT: %v", err)
			}

			cancel()
		}
		return &emptypb.Empty{}, nil
	}
}

// gRPC Get RPC

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	is_powner, is_replica, owner := s.Replicator.CheckOwnership(req.Key)
	if is_powner || is_replica {
		resp := s.sendRequest(ctx, Request{
			Op:           OpGet,
			Key:          req.Key,
			ResponseChan: make(chan Response, 1),
		})
		if resp.Err != nil {
			if errors.Is(resp.Err, ErrKeyNotFound) {
				if owner != "" {
					// we're a replica but don't have the key locally.
					val, err := s.Replicator.ForwardToOwner(ctx, owner, req)
					if err != nil {
						return nil, status.Errorf(codes.Internal, "Failed to forward GET to owner %s: %v", owner, err)
					}
					log.Printf("Forwarded GET request for key %q to owner %s", req.Key, owner)
					return &pb.GetResponse{Value: val}, nil
				}
			}
			return nil, status.Errorf(codes.Internal, "Failed to GET key: %v", resp.Err)
		}
		return &pb.GetResponse{Value: resp.Value}, nil
	}
	// not responsible for this key - forward to owner
	val, err := s.Replicator.ForwardToOwner(ctx, owner, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to forward GET to owner %s: %v", owner, err)
	}
	log.Printf("Forwarded GET request for key %q to owner %s", req.Key, owner)
	return &pb.GetResponse{Value: val}, nil
}

// gRPC Delete RPC
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*emptypb.Empty, error) {
	is_powner, _, owner := s.Replicator.CheckOwnership(req.Key)
	if !is_powner {
		_, err := s.Replicator.ForwardToOwner(ctx, owner, req)

		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to forward DELETE to owner %s: %v", owner, err)
		}
		log.Printf("Forwarded DELETE request for key %q to owner %s", req.Key, owner)
		return &emptypb.Empty{}, nil
	} else {
		resp := s.sendRequest(ctx, Request{
			Op:           OpDelete,
			Key:          req.Key,
			ResponseChan: make(chan Response, 1),
		})
		if resp.Err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to DELETE key: %v", resp.Err)
		}
		if s.Replicator != nil {

			repCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := s.Replicator.ReplicateToAll(repCtx, pb.Op_DELETE, req.Key, nil); err != nil {
				cancel()
				return nil, status.Errorf(codes.Internal, "Failed to REPLICATE DELETE: %v", err)
			}

			cancel()
		}
		return &emptypb.Empty{}, nil
	}
}
func (s *Server) Replicate(ctx context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	log.Printf("[replicator] received replication request: op=%v key=%q", req.Op, req.Key)

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
		Op:           op,
		Key:          req.Key,
		Value:        req.Value,
		ResponseChan: make(chan Response, 1),
	})
	if resp.Err != nil {
		return nil, status.Errorf(codes.Internal, "Replication failed: %v", resp.Err)
	}

	log.Printf("[replicator] applied replicated request: op=%v key=%q", req.Op, req.Key)
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

		// Track that we have a new request in flight
		inFlight.Add(1)

		// Send to EventLoop
		s.reqCh <- Request{
			Op:           coreOp,
			Key:          req.Key,
			Value:        req.Value,
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
