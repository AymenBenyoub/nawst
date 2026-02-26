package core

// the main event loop for the kv store, reads client requests from a channel
// (which comes from the grpc server) and calls the relevant store methods.
// writes the WAL entries into a buffers before calling the store methods, which will be written to disk
// in batches asynchronously by another routine.
// note that the log order is defined before the memory write. and that its sequential and single 
// threaded. 



func Loop() {}
