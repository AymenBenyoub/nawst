package core

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)



//  the WAL should :
//   1- own the log file
//   2- serialize commands
//   3- buffer and batch writes
//   4- guarantee write order
//   5- replay commands in order

type Wal struct {
	file   *os.File      
	writer *bufio.Writer // buffered writer for batching

	appendCh chan Command // in-memory queue for log entries
	closeCh  chan struct{} //signals the writer to flush and exit

	wg sync.WaitGroup // waits for writer goroutine to exit
}

// NewWal opens (or creates) a WAL file and starts the writer goroutine.
//
// bufferSize controls how much data is buffered before flushing to disk.
// bigger buffer = higher throughput, lower durability.
func NewWal(path string, bufferSize int) (*Wal, error) {
	f, err := os.OpenFile(
		path,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	w := &Wal{
		file:     f,
		writer:   bufio.NewWriterSize(f, bufferSize),
		appendCh: make(chan Command, 1024), 
		closeCh:  make(chan struct{}), 
	}

	
	w.wg.Add(1)
	go w.writerLoop()

	return w, nil
}

// Append enqueues a command to be written to the WAL.
//
// This does NOT write to disk directly.
// It only guarantees:
//   - ordering
//   - that the command is accepted by the WAL
func (w *Wal) Append(cmd Command) error {
	select {
	case w.appendCh <- cmd:
		return nil
	case <-w.closeCh:
		return fmt.Errorf("wal is closed")
	}
}

// writerLoop is the only goroutine that ever writes to the WAL file.
//
// It:
//   - serializes commands
//   - batches writes via bufio.Writer
//   - flushes periodically or on shutdown
func (w *Wal) writerLoop() {
	defer w.wg.Done()

	// periodic flush timer (time-based batching)
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case cmd := <-w.appendCh:
			if err := w.writeCommand(cmd); err != nil {
				// for now: crash hard, later on MYBE i'll add retry logic or something similar
			
				panic(err)
			}

		case <-ticker.C:
			_ = w.writer.Flush()

		case <-w.closeCh:
			// drain remaining commands before closing
			for {
				select {
				case cmd := <-w.appendCh:
					_ = w.writeCommand(cmd)
				default:
					_ = w.writer.Flush()
					return
				}
			}
		}
	}
}

// writeCommand ; serializes a 'Command' into the WAL format.
//
// 
//  an entry looks like:

//	[op:1]
//	[keyLen:varint][key bytes]
//	[valLen:varint][value bytes]
//	[timestamp:8]
func (w *Wal) writeCommand(cmd Command) error {
	var lengthBuffer [binary.MaxVarintLen64]byte

	// operation
	if err := w.writer.WriteByte(byte(cmd.Op)); err != nil {
		return err
	}

	// key
	keyBytes := []byte(cmd.Key)
	n := binary.PutUvarint(lengthBuffer[:], uint64(len(keyBytes)))
	if _, err := w.writer.Write(lengthBuffer[:n]); err != nil {
		return err
	}
	if _, err := w.writer.Write(keyBytes); err != nil {
		return err
	}

	// value
	n = binary.PutUvarint(lengthBuffer[:], uint64(len(cmd.Value)))
	if _, err := w.writer.Write(lengthBuffer[:n]); err != nil {
		return err
	}
	if _, err := w.writer.Write(cmd.Value); err != nil {
		return err
	}

	// timestamp
	ts := time.Now().UnixNano()
	if err := binary.Write(w.writer, binary.LittleEndian, ts); err != nil {
		return err
	}

	return nil
}

// Replay reads the WAL from the beginning and invokes apply(cmd)
// for each decoded command, in order.
// it doesnt buffer or mutate the data in any way.!

func (w *Wal) Replay(apply func(Command) error) error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	r := bufio.NewReader(w.file)

	for {
		// op
		op, err := r.ReadByte()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// key
		keyLen, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}

		// value
		valLen, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(r, val); err != nil {
			return err
		}

		// timestamp (unused for now)
		var ts int64
		if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
			return err
		}

		cmd := Command{
			Op:    Operation(op),
			Key:   string(key),
			Value: val,
		}

		if err := apply(cmd); err != nil {
			return err
		}
	}
}

// close the WAL cleanly, ensuring all buffered data is flushed.
func (w *Wal) Close() error {
	close(w.closeCh)
	w.wg.Wait()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}
