package core

import (
	
	"time"
)

type EventLoop struct {
	Store *Store
	Wal   *Wal
	ReqCh <-chan Request
}

type pendingAck struct {
	id       uint64
	requests []Request
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


const batchSize = 512
const batchTimeout = 2 * time.Millisecond

func (el *EventLoop) Run() {
	ackCh := make(chan pendingAck, 1024)

	// Reaper Goroutine: Handles gRPC responses asynchronously
	go func() {
		for ack := range ackCh {
			// Wait for WAL durability (instantly returns nil if Enqueue mode)
			diskErr := el.Wal.Wait(ack.id)
			
			// Respond to all clients in this batch
			for _, req := range ack.requests {
				req.ResponseChan <- Response{Err: diskErr}
			}
		}
	}()

	var writeBatch []Request
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	processBatch := func() {
		if len(writeBatch) == 0 {
			return
		}

		cmds := make([]Command, len(writeBatch))
		for i, r := range writeBatch {
			cmds[i] = Command{Op: r.Op, Key: r.Key, Value: r.Value}
			
			el.Store.Apply(cmds[i])
		}
        // still counts as 'WAL' because it won't return until the batch is flushed or fsynced, depending on the ack mode.
		// Non-blocking Append to WAL, 
		id := el.Wal.Append(cmds)

		// Hand off to response routine for async durability acknowledgment
	
		ackCh <- pendingAck{id: id, requests:  append([]Request(nil), writeBatch...)}

		writeBatch = writeBatch[:0]
	}

	for {
		select {
		case req, ok := <-el.ReqCh:
			if !ok {
				processBatch()
				close(ackCh)
				return
			}

			if req.Op == OpGet {
				val, err := el.Store.Get(req.Key)
				req.ResponseChan <- Response{Value: val, Err: err}
			} else {
				// Fast-path bypass for AckAfterEnqueue
				if el.Wal.ackMode == AckAfterEnqueue {
					cmd := Command{Op: req.Op, Key: req.Key, Value: req.Value}
					el.Wal.Append([]Command{cmd})
					err := el.Store.Apply(cmd)
					req.ResponseChan <- Response{Err: err}
					continue
				}

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