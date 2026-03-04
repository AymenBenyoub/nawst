package core

import (
	"context"
	"errors"
	"fmt"
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
	Exists bool
	Value  []byte
	Err    error
}

// NewServer returns a server using the given event loop request channel
func NewServer(reqCh chan<- Request) *Server {
	return &Server{
		reqCh: reqCh,
	}
}

// Start listens on the given port and runs the gRPC server
func (s *Server) Start(port int) error {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
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

	log.Printf("gRPC server listening on port %d\n", port)
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
	fmt.Printf("RPC Put %s length: %d val: %v\n", req.Key, len(req.Value), string(req.Value))
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
	fmt.Printf("RPC Get %s length: %d val: %v\n", req.Key, len(resp.Value), string(resp.Value))
	return &pb.GetResponse{Value: resp.Value}, nil
}

// gRPC Delete RPC
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*emptypb.Empty, error) {
	fmt.Printf("RPC Delete %s\n", req.Key)
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
