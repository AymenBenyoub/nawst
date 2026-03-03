package core

import (
	"bufio"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"time"
)

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
		appendCh: make(chan walEntry, 1024),
		closeCh:  make(chan struct{}),
		ackMode:  ackMode,
	}

	w.wg.Add(1)
	go w.writerLoop()

	return w, nil
}

// Append enqueues a command and returns a channel that will be closed
// once the configured durability level is reached.
func (w *Wal) Append(cmd Command) (<-chan struct{}, error) {
	entry := walEntry{
		cmd:  cmd,
		done: make(chan struct{}),
	}

	select {
	case w.appendCh <- entry:
		if w.ackMode == AckAfterEnqueue {
			close(entry.done)
		}
		return entry.done, nil
	case <-w.closeCh:
		return nil, errors.New("wal is closed")
	}
}

func (w *Wal) writerLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var pending []walEntry

	flush := func(doSync bool) {
		if len(pending) == 0 {
			return
		}

		_ = w.writer.Flush()
		if doSync {
			_ = w.file.Sync()
		}

		for _, e := range pending {
			close(e.done)
		}
		pending = pending[:0]
	}

	for {
		select {
		case e := <-w.appendCh:
			if err := w.writeCommand(e.cmd); err != nil {
				panic(err)
			}
			pending = append(pending, e)

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
			flush(w.ackMode == AckAfterFsync || w.ackMode == AckAfterFlush)

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
		if err != nil {
			return nil
		}

		keyLen, _ := binary.ReadUvarint(r)
		key := make([]byte, keyLen)
		if _, err := r.Read(key); err != nil {
			return err
		}

		valLen, _ := binary.ReadUvarint(r)
		val := make([]byte, valLen)
		if _, err := r.Read(val); err != nil {
			return err
		}

		var ts int64
		_ = binary.Read(r, binary.LittleEndian, &ts)

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
