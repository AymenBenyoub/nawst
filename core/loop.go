package core

// the main event loop for the kv store, reads client requests from a channel
// (which comes from the grpc server) and calls the relevant store methods.
// writes the WAL entries into a buffers before calling the store methods, which will be written to disk
// in batches asynchronously by another routine.
// note that the log order is defined before the memory write. and that its sequential and single
// threaded.
type EventLoop struct {
	Store   *Store
	ReqChan <-chan Request
	Wal     *Wal
	AckMode AckMode
}

type OpType uint8
type AckMode uint8

const (
	OpPut OpType = iota
	OpDelete
)

type Command struct {
	Op    OpType
	Key   string
	Value []byte
}

func (el *EventLoop) Run() {

	for {
		req := <-el.ReqChan
		el.HandleRequest(req)

	}
}

func (el *EventLoop) HandleRequest(req Request) {
	switch req.Op {
	case 0:
		err := el.Store.Put(req.Key, req.Value)
		req.ResponseChan <- Response{Err: err}
	case 1:
		val, err := el.Store.Get(req.Key)
		req.ResponseChan <- Response{Value: val, Err: err}
	case 2:
		err := el.Store.Delete(req.Key)
		req.ResponseChan <- Response{Err: err}
	}

}
