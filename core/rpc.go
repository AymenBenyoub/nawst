package core

import (
	"context"
	"errors"
	"io"

	"sync/atomic"
	"time"

	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	pb "github.com/AymenBenyoub/nawst/core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pb.UnimplementedKVServer
	reqCh chan<- Request
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

	resp := s.sendRequest(ctx, Request{
		Op:           OpPut,
		Key:          req.Key,
		Value:        req.Value,
		ResponseChan: make(chan Response, 1),
	})
	if resp.Err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to put key: %v", resp.Err)
	}
	return &emptypb.Empty{}, nil
}

// gRPC Get RPC
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {

	resp := s.sendRequest(ctx, Request{
		Op:           OpGet,
		Key:          req.Key,
		ResponseChan: make(chan Response, 1),
	})
	if resp.Err != nil {
		if errors.Is(resp.Err, ErrKeyNotFound) {
			return nil, status.Error(codes.NotFound, "Key not found")
		}
		return nil, status.Errorf(codes.Internal, "Failed to get key: %v", resp.Err)
	}
	return &pb.GetResponse{Value: resp.Value}, nil
}

// gRPC Delete RPC
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*emptypb.Empty, error) {

	resp := s.sendRequest(ctx, Request{
		Op:           OpDelete,
		Key:          req.Key,
		ResponseChan: make(chan Response, 1),
	})
	if resp.Err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to delete key: %v", resp.Err)
	}
	return &emptypb.Empty{}, nil
}
func (s *Server) StreamKV(stream pb.KV_StreamKVServer) error {
	// Buffer to hold ACKs coming back from the EventLoop
	respCh := make(chan Response, 10000)
	ctx := stream.Context()

	var inFlight int64

	// Receiver Goroutine: Streams ACKs back to the client
	go func() {
		for {
			select {
			case <-ctx.Done():
				// The client violently disconnected or timed out. Exit cleanly.
				return
			case resp := <-respCh:
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
				stream.Send(&pb.StreamResp{
					Op:    protoOp,
					Value: resp.Value,
					Error: errStr,
				})

				// Mark one request as safely handled
				atomic.AddInt64(&inFlight, -1)
			}
		}
	}()

	// Sender Loop: Reads from the client stream
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break // Client sent all requests and called CloseSend()
		}
		if err != nil {
			return err
		}

		var coreOp OpType
		if req.Op == pb.Op_PUT {
			coreOp = OpPut
		} else if req.Op == pb.Op_GET {
			coreOp = OpGet
		} else {
			coreOp = OpDelete
		}

		// Track that we have a new request in flight
		atomic.AddInt64(&inFlight, 1)

		// Send to EventLoop
		s.reqCh <- Request{
			Op:           coreOp,
			Key:          req.Key,
			Value:        req.Value,
			ResponseChan: respCh,
		}
	}

	// 🔥 GRACEFUL DRAIN 🔥
	// The client is done sending, but the EventLoop might still be writing the final batch to disk.
	// We wait here until every single request has been answered.
	for atomic.LoadInt64(&inFlight) > 0 {
		if ctx.Err() != nil {
			break // Stop waiting if the client disconnected entirely
		}
		time.Sleep(1 * time.Millisecond)
	}

	// We return nil. gRPC automatically closes the stream and kills the context.
	// Notice we DO NOT close(respCh) here, preventing the EventLoop from panicking
	// if it happens to be running a split millisecond behind.
	return nil
}
