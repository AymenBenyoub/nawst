// package cluster

// import (
// 	"context"
// 	"errors"
// 	"fmt"
// 	"io"
// 	"sync"
// 	"time"

// 	pb "github.com/AymenBenyoub/nawst/core/proto"
// )

// const (
// 	replicationBatchSize   = 64
// 	replicationBatchWindow = 2 * time.Millisecond
// )

// type replicationTask struct {
// 	req  *pb.ReplicationRequest
// 	done chan error
// }

// type replicationBatchWorker struct {
// 	nodeID   string
// 	client   pb.KVClient
// 	enqueue  chan replicationTask
// 	stopCh   chan struct{}
// 	stopOnce sync.Once
// }

// type inFlightBatch struct {
// 	tasks []replicationTask
// }

// type streamRecvResult struct {
// 	ack *pb.ReplicationBatchAck
// 	err error
// }

// func newReplicationBatchWorker(nodeID string, client pb.KVClient) *replicationBatchWorker {
// 	worker := &replicationBatchWorker{
// 		nodeID:  nodeID,
// 		client:  client,
// 		enqueue: make(chan replicationTask, 4096),
// 		stopCh:  make(chan struct{}),
// 	}
// 	go worker.run()
// 	return worker
// }

// func (w *replicationBatchWorker) stop() {
// 	w.stopOnce.Do(func() {
// 		close(w.stopCh)
// 	})
// }

// func (w *replicationBatchWorker) submit(task replicationTask) error {
// 	select {
// 	case w.enqueue <- task:
// 		return nil
// 	case <-time.After(500 * time.Millisecond):
// 		return fmt.Errorf("replication queue full for node %s", w.nodeID)
// 	case <-w.stopCh:
// 		return errors.New("replication worker stopped")
// 	}
// }
// func (w *replicationBatchWorker) run() {
// 	ticker := time.NewTicker(replicationBatchWindow)
// 	defer ticker.Stop()

// 	pending := make([]replicationTask, 0, replicationBatchSize)
// 	inFlight := make([]inFlightBatch, 0, 128)

// 	var stream pb.KV_ReplicateStreamClient
// 	var streamCancel context.CancelFunc
// 	var recvCh chan streamRecvResult

// 	finishTasks := func(tasks []replicationTask, err error) {
// 		for _, task := range tasks {
// 			task.done <- err
// 		}
// 	}

// 	failInFlight := func(err error) {
// 		for _, batch := range inFlight {
// 			finishTasks(batch.tasks, err)
// 		}
// 		inFlight = inFlight[:0]
// 	}

// 	closeStream := func() {
// 		if stream != nil {
// 			_ = stream.CloseSend()
// 		}
// 		if streamCancel != nil {
// 			streamCancel()
// 		}
// 		stream = nil
// 		streamCancel = nil
// 		recvCh = nil
// 	}

// 	openStream := func() error {
// 		if stream != nil {
// 			return nil
// 		}
// 		ctx, cancel := context.WithCancel(context.Background())
// 		st, err := w.client.ReplicateStream(ctx)
// 		if err != nil {
// 			cancel()
// 			return err
// 		}
// 		stream = st
// 		streamCancel = cancel
// 		return nil
// 	}

// 	startRecv := func() {
// 		if stream == nil || recvCh != nil || len(inFlight) == 0 {
// 			return
// 		}
// 		ch := make(chan streamRecvResult, 1)
// 		recvCh = ch
// 		go func() {
// 			ack, err := stream.Recv()
// 			ch <- streamRecvResult{ack: ack, err: err}
// 		}()
// 	}

// 	sendBatch := func(tasks []replicationTask) error {
// 		if len(tasks) == 0 {
// 			return nil
// 		}
// 		if err := openStream(); err != nil {
// 			return err
// 		}

// 		reqs := make([]*pb.ReplicationRequest, 0, len(tasks))
// 		for _, task := range tasks {
// 			reqs = append(reqs, task.req)
// 		}

// 		if err := stream.Send(&pb.ReplicationBatchRequest{Requests: reqs}); err != nil {
// 			return err
// 		}
// 		inFlight = append(inFlight, inFlightBatch{tasks: tasks})
// 		startRecv()
// 		return nil
// 	}

// 	flush := func() {
// 		if len(pending) == 0 {
// 			return
// 		}
// 		batch := append([]replicationTask(nil), pending...)
// 		pending = pending[:0]

// 		err := sendBatch(batch)
// 		if err == nil {
// 			return
// 		}

// 		closeStream()
// 		failInFlight(fmt.Errorf("replication stream to %s reset after send failure: %w", w.nodeID, err))

// 		if err2 := sendBatch(batch); err2 != nil {
// 			closeStream()
// 			finishTasks(batch, fmt.Errorf("replication stream send to %s failed: %w", w.nodeID, err2))
// 		}
// 	}

// 	for {
// 		select {
// 		case task := <-w.enqueue:
// 			pending = append(pending, task)
// 			if len(pending) >= replicationBatchSize {
// 				flush()
// 			}
// 		case <-ticker.C:
// 			flush()
// 		case recv := <-recvCh:
// 			recvCh = nil
// 			if recv.err != nil {
// 				if errors.Is(recv.err, io.EOF) {
// 					failInFlight(fmt.Errorf("replication stream to %s closed by remote", w.nodeID))
// 				} else {
// 					failInFlight(fmt.Errorf("replication stream recv from %s failed: %w", w.nodeID, recv.err))
// 				}
// 				closeStream()
// 				continue
// 			}

// 			if len(inFlight) == 0 {
// 				closeStream()
// 				continue
// 			}
// 			batch := inFlight[0]
// 			inFlight = inFlight[1:]

//				if recv.ack != nil && recv.ack.GetError() != "" {
//					finishTasks(batch.tasks, fmt.Errorf("replication stream ack from %s failed: %s", w.nodeID, recv.ack.GetError()))
//				} else {
//					finishTasks(batch.tasks, nil)
//				}
//				startRecv()
//			case <-w.stopCh:
//				closeStream()
//				finishTasks(pending, errors.New("replication worker stopped"))
//				failInFlight(errors.New("replication worker stopped"))
//				return
//			}
//		}
//	}
package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/AymenBenyoub/nawst/core/proto"
)

const (
	replicationBatchSize   = 256
	replicationBatchWindow = 1 * time.Millisecond
	maxInFlightBatches     = 256
)

type replicationTask struct {
	req  *pb.ReplicationRequest
	done chan error
}

type replicationBatchWorker struct {
	nodeID   string
	client   pb.KVClient
	enqueue  chan replicationTask
	stopCh   chan struct{}
	stopOnce sync.Once
}

type inFlightBatch struct {
	tasks []replicationTask
}

func newReplicationBatchWorker(nodeID string, client pb.KVClient) *replicationBatchWorker {
	w := &replicationBatchWorker{
		nodeID:  nodeID,
		client:  client,
		enqueue: make(chan replicationTask, 16384),
		stopCh:  make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *replicationBatchWorker) stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}

func (w *replicationBatchWorker) submit(task replicationTask) error {
	select {
	case w.enqueue <- task:
		return nil
	case <-time.After(500 * time.Millisecond):
		return fmt.Errorf("replication queue full for node %s", w.nodeID)
	case <-w.stopCh:
		return errors.New("worker stopped")
	}
}

func (w *replicationBatchWorker) run() {
	ticker := time.NewTicker(replicationBatchWindow)
	defer ticker.Stop()

	type ackResult struct {
		ack *pb.ReplicationBatchAck
		err error
	}

	var stream pb.KV_ReplicateStreamClient
	var cancel context.CancelFunc
	recvCh := make(chan ackResult, 1)
	recvActive := false

	failTasks := func(tasks []replicationTask, err error) {
		for _, task := range tasks {
			task.done <- err
		}
	}

	failAll := func(err error, pending []replicationTask, inFlight []inFlightBatch) {
		if len(pending) > 0 {
			failTasks(pending, err)
		}
		for _, batch := range inFlight {
			failTasks(batch.tasks, err)
		}
	}

	open := func() error {
		if stream != nil {
			return nil
		}
		ctx, c := context.WithCancel(context.Background())
		s, err := w.client.ReplicateStream(ctx)
		if err != nil {
			c()
			return err
		}
		stream = s
		cancel = c
		recvActive = false
		return nil
	}

	closeStream := func() {
		if stream != nil {
			_ = stream.CloseSend()
		}
		if cancel != nil {
			cancel()
		}
		stream = nil
		cancel = nil
		recvActive = false
	}

	pending := make([]replicationTask, 0, replicationBatchSize)
	inFlight := make([]inFlightBatch, 0, maxInFlightBatches)

	startRecv := func() {
		if stream == nil || recvActive || len(inFlight) == 0 {
			return
		}
		recvActive = true
		current := stream
		go func() {
			ack, err := current.Recv()
			recvCh <- ackResult{ack: ack, err: err}
		}()
	}

	sendBatch := func(tasks []replicationTask) error {
		if len(tasks) == 0 {
			return nil
		}
		if err := open(); err != nil {
			return err
		}

		reqs := make([]*pb.ReplicationRequest, len(tasks))
		for i, t := range tasks {
			reqs[i] = t.req
		}

		if err := stream.Send(&pb.ReplicationBatchRequest{Requests: reqs}); err != nil {
			return err
		}

		inFlight = append(inFlight, inFlightBatch{tasks: tasks})
		startRecv()
		return nil
	}

	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := append([]replicationTask(nil), pending...)
		pending = pending[:0]

		if err := sendBatch(batch); err != nil {
			closeStream()
			failTasks(batch, err)
		}
	}

	for {
		select {
		case t := <-w.enqueue:
			pending = append(pending, t)
			if len(pending) >= replicationBatchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case res := <-recvCh:
			recvActive = false
			if len(inFlight) == 0 {
				closeStream()
				continue
			}

			batch := inFlight[0]
			inFlight = inFlight[1:]

			if res.err != nil {
				closeStream()
				failTasks(batch.tasks, fmt.Errorf("replication stream to %s failed: %w", w.nodeID, res.err))
				failAll(fmt.Errorf("replication stream to %s failed: %w", w.nodeID, res.err), pending, inFlight)
				pending = pending[:0]
				inFlight = inFlight[:0]
				continue
			}

			if res.ack != nil && res.ack.GetError() != "" {
				failTasks(batch.tasks, fmt.Errorf("replication stream ack from %s failed: %s", w.nodeID, res.ack.GetError()))
			} else {
				failTasks(batch.tasks, nil)
			}
			startRecv()

		case <-w.stopCh:
			closeStream()
			failAll(errors.New("worker stopped"), pending, inFlight)
			return
		}
	}
}
func (r *Replicator) getOrCreateReplicationWorker(nodeID string) (*replicationBatchWorker, error) {
	client, err := r.getOrCreateClientByNodeID(nodeID)
	if err != nil {
		return nil, err
	}

	r.replicationMu.Lock()
	defer r.replicationMu.Unlock()

	if w, ok := r.replicationWorkers[nodeID]; ok {
		return w, nil
	}

	worker := newReplicationBatchWorker(nodeID, client)
	r.replicationWorkers[nodeID] = worker
	return worker, nil
}

func (r *Replicator) submitReplicationTask(nodeID string, req *pb.ReplicationRequest) (<-chan error, error) {
	if req == nil {
		return nil, errors.New("nil replication request")
	}

	worker, err := r.getOrCreateReplicationWorker(nodeID)
	if err != nil {
		return nil, err
	}

	done := make(chan error, 1)
	if err := worker.submit(replicationTask{req: req, done: done}); err != nil {
		return nil, err
	}
	return done, nil
}

func (r *Replicator) stopReplicationWorkers() {
	r.replicationMu.Lock()
	defer r.replicationMu.Unlock()
	for nodeID, worker := range r.replicationWorkers {
		worker.stop()
		delete(r.replicationWorkers, nodeID)
	}
}
