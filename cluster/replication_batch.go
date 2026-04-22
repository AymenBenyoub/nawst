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
	replicationBatchSize    = 64
	replicationBatchWindow  = 2 * time.Millisecond
	replicationBatchTimeout = 2 * time.Second
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

func newReplicationBatchWorker(nodeID string, client pb.KVClient) *replicationBatchWorker {
	worker := &replicationBatchWorker{
		nodeID:  nodeID,
		client:  client,
		enqueue: make(chan replicationTask, 4096),
		stopCh:  make(chan struct{}),
	}
	go worker.run()
	return worker
}

func (w *replicationBatchWorker) stop() {
	w.stopOnce.Do(func() {
		close(w.stopCh)
	})
}

func (w *replicationBatchWorker) submit(task replicationTask) error {
	select {
	case w.enqueue <- task:
		return nil
	case <-w.stopCh:
		return errors.New("replication worker stopped")
	}
}

func (w *replicationBatchWorker) run() {
	ticker := time.NewTicker(replicationBatchWindow)
	defer ticker.Stop()

	pending := make([]replicationTask, 0, replicationBatchSize)

	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := append([]replicationTask(nil), pending...)
		pending = pending[:0]
		go w.dispatch(batch)
	}

	for {
		select {
		case task := <-w.enqueue:
			pending = append(pending, task)
			if len(pending) >= replicationBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-w.stopCh:
			flush()
			return
		}
	}
}

func (w *replicationBatchWorker) dispatch(tasks []replicationTask) {
	if len(tasks) == 0 {
		return
	}

	reqs := make([]*pb.ReplicationRequest, 0, len(tasks))
	for _, task := range tasks {
		reqs = append(reqs, task.req)
	}

	ctx, cancel := context.WithTimeout(context.Background(), replicationBatchTimeout)
	defer cancel()

	_, err := w.client.ReplicateBatch(ctx, &pb.ReplicationBatchRequest{Requests: reqs})
	if err != nil {
		err = fmt.Errorf("replicate batch to %s failed: %w", w.nodeID, err)
	}

	for _, task := range tasks {
		task.done <- err
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
