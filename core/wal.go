package core

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Pool for reusing []chan struct{} slices
var entryChannelsPool = sync.Pool{
	New: func() interface{} {
		return make([]chan struct{}, 0, 512)
	},
}

type AckMode uint8

const (
	AckAfterEnqueue AckMode = iota // fastest, weakest
	AckAfterFlush                  // flushed to OS
	AckAfterFsync                  // durable
)

type walEntry struct {
	cmd  Command
	done chan struct{} // closed when durability boundary is reached
}

type Wal struct {
	file   *os.File
	writer *bufio.Writer

	appendCh chan walEntry
	closeCh  chan struct{}

	ackMode AckMode

	wg sync.WaitGroup
}

// NewWal opens the WAL and starts the writer goroutine.
// Replay MUST be done before calling this.
func NewWal(path string, bufferSize int, ackMode AckMode) (*Wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	w := &Wal{
		file:     f,
		writer:   bufio.NewWriterSize(f, bufferSize),
		appendCh: make(chan walEntry, 8192),
		closeCh:  make(chan struct{}),
		ackMode:  ackMode,
	}

	w.wg.Add(1)
	go w.writerLoop()

	return w, nil
}

// Append enqueues a command and returns a channel that will be closed
// once the configured durability level is reached.
func (w *Wal) Append(cmd_batch []Command) (<-chan struct{}, error) {
	if w.ackMode == AckAfterEnqueue {
		for _, cmd := range cmd_batch {
			entry := walEntry{
				cmd:  cmd,
				done: nil, // No channel needed
			}
			select {
			case w.appendCh <- entry:
			case <-w.closeCh:
				return nil, errors.New("wal is closed")
			}
		}
		ch := make(chan struct{})
		close(ch)
		return ch, nil
	}
	if len(cmd_batch) == 0 {
		ch := make(chan struct{})
		close(ch)
		return ch, nil
	}

	// per-entry done channels, aggregated into batchDone (from pool)
	entryChans := entryChannelsPool.Get().([]chan struct{})[:0]

	for _, cmd := range cmd_batch {
		echan := make(chan struct{})
		entry := walEntry{
			cmd:  cmd,
			done: echan,
		}

		select {
		case w.appendCh <- entry:
			entryChans = append(entryChans, echan)
		case <-w.closeCh:
			entryChannelsPool.Put(entryChans)
			return nil, errors.New("wal is closed")
		}
	}

	batchDone := make(chan struct{})

	// If ack after enqueue, close all entry channels immediately and the batch
	if w.ackMode == AckAfterEnqueue {
		for _, ch := range entryChans {
			close(ch)
		}
		entryChannelsPool.Put(entryChans)
		close(batchDone)
		return batchDone, nil
	}

	// Otherwise, wait for all per-entry channels to be closed by writerLoop,
	// then close the batchDone channel once.
	go func(chs []chan struct{}, out chan struct{}) {
		for _, c := range chs {
			<-c
		}
		entryChannelsPool.Put(chs)
		close(out)
	}(entryChans, batchDone)

	return batchDone, nil
}

func (w *Wal) writerLoop() {
	defer w.wg.Done()
	const maxBatchSize = 4096
	const flushInterval = 1 * time.Millisecond
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	var pending []walEntry

	flush := func(doSync bool) {
		if len(pending) == 0 {
			return
		}

		if err := w.writer.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "Flush error: %v\n", err)
		}
		if doSync {
			if err := w.file.Sync(); err != nil {
				fmt.Fprintf(os.Stderr, "Fsync error: %v\n", err)
			}
		}

		for _, e := range pending {
			if w.ackMode != AckAfterEnqueue {
				close(e.done)
			}
		}
		pending = pending[:0]
	}

	for {
		select {
		case e := <-w.appendCh:
			if err := w.writeCommand(e.cmd); err != nil {
				// log and mark the entry as failed
				fmt.Fprintf(os.Stderr, "WAL write failed: %v\n", err)
				close(e.done) // still close so the caller doesn't block forever
				continue
			}
			pending = append(pending, e)
			if len(pending) >= maxBatchSize {
				flush(w.ackMode == AckAfterFsync)
			}
		case <-ticker.C:
			for {
				select {
				// in case new entries arrived after the ticker ticked, include them in same batch
				// instead of waiting for the next tick.
				case e := <-w.appendCh:
					w.writeCommand(e.cmd)
					pending = append(pending, e)
				default:
					goto done
				}
			}
		done:
			flush(w.ackMode == AckAfterFsync)

		case <-w.closeCh:
			flush(true)
			return
		}
	}
}

func (w *Wal) writeCommand(cmd Command) error {
	var buf [binary.MaxVarintLen64]byte

	if err := w.writer.WriteByte(byte(cmd.Op)); err != nil {
		return err
	}

	key := []byte(cmd.Key)
	n := binary.PutUvarint(buf[:], uint64(len(key)))
	if _, err := w.writer.Write(buf[:n]); err != nil {
		return err
	}
	if _, err := w.writer.Write(key); err != nil {
		return err
	}

	n = binary.PutUvarint(buf[:], uint64(len(cmd.Value)))
	if _, err := w.writer.Write(buf[:n]); err != nil {
		return err
	}
	if _, err := w.writer.Write(cmd.Value); err != nil {
		return err
	}

	ts := time.Now().UnixNano()
	return binary.Write(w.writer, binary.LittleEndian, ts)
}

// MUST be called before NewWal starts the writer goroutine.
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

		keyLen, err := binary.ReadUvarint(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}

		valLen, err := binary.ReadUvarint(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(r, val); err != nil {
			return err
		}

		var ts int64
		errr := binary.Read(r, binary.LittleEndian, &ts)
		if errr != nil {
			if errr == io.EOF {
				return nil
			}
			return errr
		}

		if err := apply(Command{
			Op:    OpType(op),
			Key:   string(key),
			Value: val,
		}); err != nil {
			return err
		}
	}
}

func (w *Wal) Close() error {
	close(w.closeCh)
	w.wg.Wait()
	_ = w.writer.Flush()
	return w.file.Close()
}
