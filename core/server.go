package core

import (
	pb "github.com/AymenBenyoub/nawst/core/proto"
)

//  gRPC server process for the kv store, handles rpc calls and forwards them to the event loop for
// processing, should contain purely transport logic.

type Server struct {
	pb.UnimplementedKVServer
}
type Request struct {
	Op           int // 0 put, 1 get, 2 delete
	Key          string
	Value        []byte
	ResponseChan chan Response
}
type Response struct {
	Exists bool
	Value  []byte
	Err    error
}

func (s *Server) Start() {

}
