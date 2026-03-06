package core

import (
	"sync"
	"time"
)

// Pool for reusing Command slices
var commandPool = sync.Pool{
	New: func() interface{} {
		return make([]Command, 0, 512)
	},
}

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

const batchSize = 32
const batchTimeout = 5 * time.Millisecond

func (el *EventLoop) Run() {
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	var writeBatch []Request

	processBatch := func() {
		if len(writeBatch) == 0 {
			return
		}

		// Get Command slice from pool, reset length
		commands := commandPool.Get().([]Command)[:0]
		for _, r := range writeBatch {
			commands = append(commands, Command{
				Op:    r.Op,
				Key:   r.Key,
				Value: r.Value,
			})
		}

		done, err := el.Wal.Append(commands)
		if err != nil {
			for _, r := range writeBatch {
				r.ResponseChan <- Response{Err: err}
			}
			writeBatch = writeBatch[:0]
			commandPool.Put(commands)
			return
		}

		<-done

		for i, cmd := range commands {
			err := el.Store.Apply(cmd)
			writeBatch[i].ResponseChan <- Response{Err: err}
		}

		writeBatch = writeBatch[:0]
		commandPool.Put(commands)
	}

	for {
		select {
		case req, ok := <-el.ReqCh:
			if !ok {

				processBatch()
				return
			}

			if req.Op == OpGet {
				val, err := el.Store.Get(req.Key)
				req.ResponseChan <- Response{Value: val, Err: err}
			} else {
				//bypass batching for AckAfterEnqueue mode to minimize latency
				if el.Wal.ackMode == AckAfterEnqueue {
					cmd := Command{Op: req.Op, Key: req.Key, Value: req.Value}
					done, err := el.Wal.Append([]Command{cmd})
					if err != nil {
						req.ResponseChan <- Response{Err: err}
						continue
					}
					<-done
					err = el.Store.Apply(cmd)
					req.ResponseChan <- Response{Err: err}
					continue
				}

				// Batch writes for durable ack modes
				writeBatch = append(writeBatch, req)

				if len(writeBatch) >= batchSize {
					processBatch()
					ticker.Reset(batchTimeout)
				}
			}

		case <-ticker.C:

			processBatch()
		}
	}
}
