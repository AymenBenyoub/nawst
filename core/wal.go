package core

import (
	"encoding/binary"
	"os"
)

type Wal struct {
	file *os.File
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
	OpPut = 0

	OpDelete = 1
)

func NewWal(filepath string) (*Wal, error) {
	f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Wal{file: f}, nil
}

func (w *Wal) Append(record *WALRecord) error {

	buf := make([]byte, 0, 64+len(record.Key)+len(record.Value))

	// operation (1 byte)
	buf = append(buf, byte(record.Op))

	// varint lengths + kv data
	buf = binary.AppendUvarint(buf, uint64(len(record.Key)))   // write key length
	buf = append(buf, record.Key...)                           // then the key bytes
	buf = binary.AppendUvarint(buf, uint64(len(record.Value))) // write value length
	buf = append(buf, record.Value...)                         // then the value bytes

	// timestamp (8 bytes)
	tsBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBuf, uint64(record.Ts))
	buf = append(buf, tsBuf...)

	_, err := w.file.Write(buf)
	//no fsync for now, less durable but much faster, can be added later if needed
	return err
}

func (w *Wal) Close() error {
	return w.file.Close()
}
