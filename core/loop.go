package core

type EventLoop struct {
	Store *Store
	Wal   *Wal
	ReqCh <-chan Request
}

type OpType uint8

const (
	OpPut OpType = iota
	OpGet
	OpDelete
)

type Command struct {
	Op    OpType
	Key   string
	Value []byte
}

func (el *EventLoop) Run() {
	for req := range el.ReqCh {
		el.handle(req)
	}
}

func (el *EventLoop) handle(req Request) {
	switch req.Op {

	case OpPut:
		cmd := Command{Op: OpPut, Key: req.Key, Value: req.Value}
		done, err := el.Wal.Append(cmd)
		if err != nil {
			req.ResponseChan <- Response{Err: err}
			return
		}
		<-done
		err = el.Store.Apply(cmd)
		req.ResponseChan <- Response{Err: err}

	case OpDelete:
		cmd := Command{Op: OpDelete, Key: req.Key}
		done, err := el.Wal.Append(cmd)
		if err != nil {
			req.ResponseChan <- Response{Err: err}
			return
		}
		<-done
		err = el.Store.Apply(cmd)
		req.ResponseChan <- Response{Err: err}

	case OpGet:
		val, err := el.Store.Get(req.Key)
		req.ResponseChan <- Response{Value: val, Err: err}
	}
}