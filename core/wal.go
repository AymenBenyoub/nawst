package core

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type Wal struct {
	wal   *os.File
	walMu sync.Mutex
}

type WALRecord struct {
	Op    int
	Key   []byte
	Value []byte
	Ts    int64
}

const (
	OpPut    = 0
	OpGet    = 1
	OpUpdate = 2
	OpDelete = 3
)

func (w *Wal) Append(record *WALRecord) error {
	w.walMu.Lock()
	defer w.walMu.Unlock()

	buf := make([]byte, 0, 64+len(record.Key)+len(record.Value))

	// Op
	buf = append(buf, byte(record.Op))

	// varint lengths + kv data
	buf = binary.AppendUvarint(buf, uint64(len(record.Key)))
	buf = append(buf, record.Key...)
	buf = binary.AppendUvarint(buf, uint64(len(record.Value)))
	buf = append(buf, record.Value...)

	// timestamp
	binary.LittleEndian.PutUint64(buf[len(buf):len(buf)+8], uint64(record.Ts))
	buf = buf[:len(buf)+8]

	_, err := w.wal.Write(buf)
	return err
}
func (w *Wal) Recover() error {
	
    // Seek to beginning (just in case)
    if _, err := w.wal.Seek(0, io.SeekStart); err != nil {
        return fmt.Errorf("seek to start failed: %w", err)
    }
    r := bufio.NewReader(w.wal)
    for {
        // read op (1 byte)
        header := make([]byte, 1)
        n, err := io.ReadFull(w.wal, header)
        if err != nil {
            if err == io.EOF {
                // normal end of file --> success
                return nil
            }
            if err == io.ErrUnexpectedEOF {
                return fmt.Errorf("incomplete WAL entry (partial header)")
            }
            return fmt.Errorf("read op failed: %w", err)
        }
        if n != 1 {
            return fmt.Errorf("unexpected short read for op: %d bytes", n)
        }
        op := int(header[0])

        // Read key length (varint)
        keyLen64, err := binary.ReadUvarint(r)
        if err != nil {
            if err == io.EOF || err == io.ErrUnexpectedEOF {
                return fmt.Errorf("unexpected EOF while reading key length")
            }
            return fmt.Errorf("read key length failed: %w", err)
        }
        if keyLen64 > 1<<16 { // reasonable max key size: 64 KiB
            return fmt.Errorf("implausible key length: %d (possible corruption)", keyLen64)
        }
        keyLen := int(keyLen64)

        key := make([]byte, keyLen)
        if _, err := io.ReadFull(w.wal, key); err != nil {
            return fmt.Errorf("read key failed (len=%d): %w", keyLen, err)
        }

        // Read value length (varint)
        valLen64, err := binary.ReadUvarint(r)
        if err != nil {
            if err == io.EOF || err == io.ErrUnexpectedEOF {
                return fmt.Errorf("unexpected EOF while reading value length")
            }
            return fmt.Errorf("read value length failed: %w", err)
        }
        if valLen64 > 10<<20 { // reasonable max value size: 10 MiB
            return fmt.Errorf("implausible value length: %d (possible corruption)", valLen64)
        }
        valLen := int(valLen64)

        val := make([]byte, valLen)
        if _, err := io.ReadFull(w.wal, val); err != nil {
            return fmt.Errorf("read value failed (len=%d): %w", valLen, err)
        }

        // Read timestamp (fixed 8 bytes)
        tsBytes := make([]byte, 8)
        if _, err := io.ReadFull(w.wal, tsBytes); err != nil {
            return fmt.Errorf("read timestamp failed: %w", err)
        }
        ts := int64(binary.LittleEndian.Uint64(tsBytes))

        // Apply the record to the store
        record := WALRecord{
            Op:    op,
            Key:   key,
            Value: val,
            Ts:    ts,
        }

        // apply to store 
		switch record.Op {
		case OpPut:
			
		}
    }
}
