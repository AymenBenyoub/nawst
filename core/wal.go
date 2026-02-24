package core

import (
	"encoding/binary"
	"os"
	"sync"
)

type Wal struct {
	file  *os.File
	walMu sync.Mutex
}

type WALRecord struct {
	Op    int    // 1 byte
	Key   []byte // varint length prefix + key bytes
	Value []byte //var int length prefix + value bytes
	Ts    int64  // 8 bytes for the timestamp
}

/*
a single entry in the WAL will look like:

	[Op (1 byte)]
	[key length (varint)]
	[key bytes]
	[value length (varint)]
	[value bytes]
	[timestamp (8 bytes)]
*/

const (
	OpPut    = 0
	OpGet    = 1
	OpUpdate = 2
	OpDelete = 3
)

func NewWal(filepath string) (*Wal, error) {
	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &Wal{file: f, walMu: sync.Mutex{}}, nil
}



func (w *Wal) Append(record *WALRecord) error {
	w.walMu.Lock()
	defer w.walMu.Unlock()

	buf := make([]byte, 0, 64+len(record.Key)+len(record.Value))

	// Op
	buf = append(buf, byte(record.Op))

	// varint lengths + kv data
	buf = binary.AppendUvarint(buf, uint64(len(record.Key)))   // write key length
	buf = append(buf, record.Key...)                           // then the key bytes
	buf = binary.AppendUvarint(buf, uint64(len(record.Value))) // write value length
	buf = append(buf, record.Value...)                         // then the value bytes

	// timestamp (8 bytes)
	binary.LittleEndian.PutUint64(buf[len(buf):len(buf)+8], uint64(record.Ts))
	buf = buf[:len(buf)+8]

	_, err := w.file.Write(buf)
	return err
}
