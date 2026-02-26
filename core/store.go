package core

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"slices"

	"time"
)

type Store struct {
	storage map[string][]byte
	wal     *Wal
}

type Operation int

const (
	OpPut Operation = iota
	OpGet
	OpDelete
)

func NewStore(wal *Wal) *Store {
	return &Store{
		storage: make(map[string][]byte),
		wal:     wal,
	}
}

// writes to the WAL are sequential for now for simplicity & to get things going
// a better implementation would be batching multiple entries together and fsyncing for each batch
// might add later

func (s *Store) Put(key string, value []byte) error {

	record := &WALRecord{
		Op:    OpPut,
		Key:   []byte(key),
		Value: value,
		Ts:    time.Now().UnixNano(),
	}
	if err := s.wal.Append(record); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}
	s.storage[key] = slices.Clone(value)
	return nil

}
func (s *Store) Get(key string) ([]byte, error) {
	val, exists := s.storage[key]
	if !exists {
		return nil, ErrKeyNotFound
	}
	return slices.Clone(val), nil
}

func (s *Store) Delete(key string) error {

	record := &WALRecord{
		Op:    OpDelete,
		Key:   []byte(key),
		Value: nil,
		Ts:    time.Now().UnixNano(),
	}
	if err := s.wal.Append(record); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}
	delete(s.storage, key)
	return nil
}

func (s *Store) applyRecord(record *WALRecord) error {

	switch record.Op {
	case OpPut:
		s.storage[string(record.Key)] = slices.Clone(record.Value)

	case OpDelete:
		delete(s.storage, string(record.Key))

	default:
		return ErrInvalidOperation
	}
	return nil
}

func (s *Store) RecoverFromWAL() error {
	s.storage = make(map[string][]byte) // clear in memory data before replaying WAL

	// start from beginning of the WAL file, read each entry and apply to the store.
	if _, err := s.wal.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start failed: %w", err)
	}
	r := bufio.NewReader(s.wal.file)
	for {
		// read op (1 byte)
		header := [1]byte{}
		n, err := io.ReadFull(r, header[:])
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
		op := Operation(header[0])

		// read key length (varint)
		keyLen64, err := binary.ReadUvarint(r)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return fmt.Errorf("unexpected EOF while reading key length")
			}
			return fmt.Errorf("read key length failed: %w", err)
		}
		if keyLen64 > 1<<16 { // reasonable max key size: 64kb
			return fmt.Errorf("implausible key length: %d (possible corruption)", keyLen64)
		}
		keyLen := int(keyLen64)

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return fmt.Errorf("read key failed (len=%d): %w", keyLen, err)
		}

		//read value length (varint)
		valLen64, err := binary.ReadUvarint(r)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return fmt.Errorf("unexpected EOF while reading value length")
			}
			return fmt.Errorf("read value length failed: %w", err)
		}
		if valLen64 > 10<<20 { // reasonable max value size: 10mb
			return fmt.Errorf("implausible value length: %d (possible corruption)", valLen64)
		}
		valLen := int(valLen64)

		val := make([]byte, valLen)
		if _, err := io.ReadFull(r, val); err != nil {
			return fmt.Errorf("read value failed (len=%d): %w", valLen, err)
		}

		//rread timestamp (fixed 8 bytes)
		tsBytes := [8]byte{}
		if _, err := io.ReadFull(r, tsBytes[:]); err != nil {
			return fmt.Errorf("read timestamp failed: %w", err)
		}
		ts := int64(binary.LittleEndian.Uint64(tsBytes[:]))

		record := &WALRecord{
			Op:    op,
			Key:   key,
			Value: val,
			Ts:    ts,
		}

		if err := s.applyRecord(record); err != nil {
			return fmt.Errorf("failed to apply WAL record: %w", err)
		}

	}
}
