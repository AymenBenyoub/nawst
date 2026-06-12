package core

import (
	"time"
)

type EventLoop struct {
	Store *Store
	Wal   *Wal
	ReqCh <-chan Request
	BatchSize int
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

// Command represents a state-changing operation with full context for vnode-aware storage.
// VNodeID: indicates which vnode owns this key (computed from key hash at RPC layer).
// Version: logical timestamp for conflict detection during concurrent replication.
// Enables fast enumeration of owned keys per vnode and version-based reconciliation.
type Command struct {
	Op      OpType
	Key     string
	Value   []byte
	VNodeID uint16 // Vnode owner (0..1023); computed from hash(key)
	Version uint64 // Logical version for conflict resolution; increments per write
}

// const batchSize = 512
const batchTimeout = 2 * time.Millisecond

func (el *EventLoop) Run() {
	ackCh := make(chan pendingAck, 10000)

	// Handles gRPC responses asynchronously
	go func() {
		for ack := range ackCh {
			// Wait for WAL durability (instantly returns nil if Enqueue mode)
			diskErr := el.Wal.Wait(ack.id)

			// Respond to all clients in this batch.
			// BLOCKING send ensures we never drop an ACK and break the inFlight count.
			for _, req := range ack.requests {
				req.ResponseChan <- Response{Op: req.Op, Err: diskErr}
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
			// Build command with vnode and version from request.
			// Vnode was computed by RPC layer; version was incremented at Put/Delete time.
			cmds[i] = Command{
				Op:      r.Op,
				Key:     r.Key,
				Value:   r.Value,
				VNodeID: r.VNodeID,
				Version: r.Version,
			}
			el.Store.Apply(cmds[i])
		}

		// Non-blocking Append to WAL
		id := el.Wal.Append(cmds)

		// Hand off to response routine for async durability acknowledgment
		ackCh <- pendingAck{id: id, requests: append([]Request(nil), writeBatch...)}

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
				val, ver, err := el.Store.GetWithVersion(req.Key)
				// BLOCKING send for GET requests, include version
				req.ResponseChan <- Response{Op: req.Op, Value: val, Err: err, Version: ver}
			} else {
				// Fast-path bypass for AckAfterEnqueue: immediately apply and ACK without batching.
				if el.Wal.ackMode == AckAfterEnqueue {
					cmd := Command{
						Op:      req.Op,
						Key:     req.Key,
						Value:   req.Value,
						VNodeID: req.VNodeID,
						Version: req.Version,
					}
					el.Wal.Append([]Command{cmd})
					err := el.Store.Apply(cmd)

					// BLOCKING send for fast-path
					req.ResponseChan <- Response{Op: req.Op, Err: err}
					continue
				}

				writeBatch = append(writeBatch, req)
				if len(writeBatch) >= el.BatchSize {
					processBatch()
					ticker.Reset(batchTimeout)
				}
			}

		case <-ticker.C:
			processBatch()
		}
	}
}
