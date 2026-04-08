package core

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type AckMode uint8

const (
	AckAfterEnqueue AckMode = iota // fastest, weakest
	AckAfterFlush                  // flushed to OS
	AckAfterFsync                  // durable
)

type walBatch struct {
	id   uint64
	cmds []Command
}

type Wal struct {
	file     *os.File
	appendCh chan walBatch
	closeCh  chan struct{}
	ackMode  AckMode

	nextID    uint64
	flushedID uint64

	mu   sync.Mutex
	cond *sync.Cond
	err  error // Propagates fatal disk errors
	wg   sync.WaitGroup
}

// NewWal opens the WAL and starts the writer goroutine.
func NewWal(path string, bufferSize int, ackMode AckMode) (*Wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	w := &Wal{
		file:     f,
		appendCh: make(chan walBatch, 1024),
		closeCh:  make(chan struct{}),
		ackMode:  ackMode,
	}
	w.cond = sync.NewCond(&w.mu)

	w.wg.Add(1)
	go w.writerLoop(bufferSize)

	return w, nil
}

// Append enqueues a batch and returns its Sequence ID.
func (w *Wal) Append(cmds []Command) uint64 {
	id := atomic.AddUint64(&w.nextID, uint64(len(cmds)))
	w.appendCh <- walBatch{id: id, cmds: cmds}
	return id
}

// Wait blocks until the sequence ID is safely durable, returning any disk errors.
// It instantly returns nil if in Enqueue mode.
func (w *Wal) Wait(id uint64) error {
	if w.ackMode == AckAfterEnqueue {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for atomic.LoadUint64(&w.flushedID) < id && w.err == nil {
		w.cond.Wait()
	}
	return w.err
}

func (w *Wal) writerLoop(bufferSize int) {
	defer w.wg.Done()

	buf := make([]byte, bufferSize)
	pos := 0

	flush := func() error {
		if pos == 0 {
			return nil
		}
		if _, err := w.file.Write(buf[:pos]); err != nil {
			return err
		}
		if w.ackMode == AckAfterFsync {
			if err := w.file.Sync(); err != nil {
				return err
			}
		}
		pos = 0
		return nil
	}

	for {
		select {
		case batch := <-w.appendCh:
			for _, cmd := range batch.cmds {
				// needed = opType(1) + vnodeID(2) + version(8) + keyLen(uvarint) + key + valLen(uvarint) + value
				// Conservative estimate with max varint sizes.
				needed := 1 + 2 + 8 + binary.MaxVarintLen64*2 + len(cmd.Key) + len(cmd.Value)

				// Flush if full. Grow buffer if a single command is massive.
				if pos+needed > len(buf) {
					if err := flush(); err != nil {
						w.fail(err)
						return
					}
					if needed > len(buf) {
						buf = make([]byte, needed)
					}
				}

				// Encode: Op (1 byte) | VNodeID (2 bytes, big-endian) | Version (8 bytes, big-endian) | keyLen | key | valLen | value
				buf[pos] = byte(cmd.Op)
				pos++

				binary.BigEndian.PutUint16(buf[pos:], cmd.VNodeID)
				pos += 2

				binary.BigEndian.PutUint64(buf[pos:], cmd.Version)
				pos += 8

				n := binary.PutUvarint(buf[pos:], uint64(len(cmd.Key)))
				pos += n
				pos += copy(buf[pos:], cmd.Key)

				n = binary.PutUvarint(buf[pos:], uint64(len(cmd.Value)))
				pos += n
				pos += copy(buf[pos:], cmd.Value)
			}

			// If channel is empty, flush immediately for lower latency
			if len(w.appendCh) == 0 {
				if err := flush(); err != nil {
					w.fail(err)
					return
				}
				atomic.StoreUint64(&w.flushedID, batch.id)
				w.cond.Broadcast()
			}

		case <-w.closeCh:
			flush()
			w.file.Sync()
			return
		}
	}
}

// fail sets a fatal error and wakes up all waiting Reapers
func (w *Wal) fail(err error) {
	w.mu.Lock()
	w.err = err
	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *Wal) Close() error {
	close(w.closeCh)
	w.wg.Wait()
	return w.file.Close()
}

// MUST be called before NewWal starts the writer goroutine.
// Replay format: Op | VNodeID (2 bytes) | Version (8 bytes) | keyLen | key | valLen | value
func ReplayWal(path string, apply func(Command) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	for {
		op, err := r.ReadByte()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// Read vnode ID (2 bytes, big-endian)
		vnodeBytes := make([]byte, 2)
		if _, err := io.ReadFull(r, vnodeBytes); err != nil {
			return err
		}
		vnodeID := binary.BigEndian.Uint16(vnodeBytes)

		// Read version (8 bytes, big-endian)
		versionBytes := make([]byte, 8)
		if _, err := io.ReadFull(r, versionBytes); err != nil {
			return err
		}
		version := binary.BigEndian.Uint64(versionBytes)

		keyLen, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}

		valLen, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(r, val); err != nil {
			return err
		}

		if err := apply(Command{
			Op:      OpType(op),
			Key:     string(key),
			Value:   val,
			VNodeID: vnodeID,
			Version: version,
		}); err != nil {
			return err
		}
	}
}
