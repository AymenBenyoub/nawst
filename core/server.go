package core

import (
)

//  gRPC server process for the kv store, contains a single threaded event loop that accepts clients 
// requests
// and applies them to the store.

type Server struct {
	store    *Store
	
}



func (s *Server) Start(){


}