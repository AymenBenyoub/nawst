package core

import (
	"context"
	"flag"
	"log"
	"net"
	"strconv"

	pb "github.com/AymenBenyoub/nawst/core/proto"
	"google.golang.org/grpc"
)

//  gRPC server process for the kv store, handles rpc calls and forwards them to the event loop for
// processing, should contain purely transport logic.

type Server struct {
	pb.UnimplementedKVServer
	reqCh chan<- Request
}
type Request struct {
	Op           int // 0 put, 1 delete, see Operation type in store.go
	Key          string
	Value        []byte
	ResponseChan chan Response
}
type Response struct {
	Exists bool
	Value  []byte
	Err    error
}

func NewServer(reqCh chan<- Request) *Server {
	return &Server{
		reqCh: reqCh,
	}
}

var (
	port = flag.Int("port", 50051, "The server port")
)

func (s *Server) Start() {
	flag.Parse()
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", *port, err)
	}
	server := grpc.NewServer()
	reqCh := make(chan<- Request, 1024)
	pb.RegisterKVServer(server, NewServer(reqCh))
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
	log.Println("Server started on port", *port)

}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error)          {}
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error)          {}
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {}
