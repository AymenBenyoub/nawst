package core

import (
	
	
)

//  gRPC server process for the kv store, handles rpc calls and forwards them to the event loop for 
// processing, should contain purely transport logic.

type Server struct {
	store    *Store
	
}



func (s *Server) Start(){


}