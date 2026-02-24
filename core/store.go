package core

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"
)

type Store struct {
	Storage map[string][]byte
	WAL     *Wal
	RWLock  sync.RWMutex
}

func newStore(wal *Wal) *Store {
	return &Store{
		Storage: make(map[string][]byte),
		WAL:     wal,
	}
}

func (s *Store) Put(key string, value []byte) error {
	s.RWLock.Lock()
	defer s.RWLock.Unlock()
	record := &WALRecord{
		Op:    OpPut,
		Key:   []byte(key),
		Value: value,
		Ts:    time.Now().UnixNano(),
	}
	if err := s.WAL.Append(record); err != nil {
		return fmt.Errorf("%w: %v", ErrWalWriteFailed, err)
	}
	return nil

}
func (s *Store) Get(key string) ([]byte, error) {
	s.RWLock.RLock()
	defer s.RWLock.Unlock()
	val, exists := s.Storage[key]
	if !exists {
		return nil, ErrKeyNotFound
	}
	return slices.Clone(val), nil
}

func (s *Store) Update(key string, value []byte) error {
	s.RWLock.Lock()
	defer s.RWLock.Unlock()

	record := &WALRecord{
		Op:    OpUpdate,
		Key:   []byte(key),
		Value: value,
		Ts:    time.Now().UnixNano(),
	}
	if err := s.WAL.Append(record); err != nil {
		return fmt.Errorf("%w: %v", ErrWalWriteFailed, err)
	}
	s.Storage[key] = value
	return nil
}

func (s *Store) Delete(key string) error {
	s.RWLock.Lock()
	defer s.RWLock.Unlock()
	record := &WALRecord{
		Op:    OpDelete,
		Key:   []byte(key),
		Value: nil,
		Ts:    time.Now().UnixNano(),
	}
	if err := s.WAL.Append(record); err != nil {
		return fmt.Errorf("%w: %v", ErrWalWriteFailed, err)
	}
	delete(s.Storage, key)
	return nil
}

func (s *Store) applyRecord(record *WALRecord) error {
	switch record.Op {
	case OpPut:
		s.Storage[string(record.Key)] = append([]byte(nil), record.Value...)
		break
	case OpDelete:
		delete(s.Storage, string(record.Key))
		break
	case OpUpdate:
		s.Storage[string(record.Key)] = append([]byte(nil), record.Value...)
		break
	default:
		return ErrInvalidOperation
	}
	return nil
}

func (s *Store) RecoverFromWAL() error {

	// start from beginning of the WAL file, read each entry and apply to the store.
	if _, err := s.WAL.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start failed: %w", err)
	}
	r := bufio.NewReader(s.WAL.file)
	for {
		// read op (1 byte)
		header := make([]byte, 1)
		n, err := io.ReadFull(r, header)
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
		tsBytes := make([]byte, 8)
		if _, err := io.ReadFull(r, tsBytes); err != nil {
			return fmt.Errorf("read timestamp failed: %w", err)
		}
		ts := int64(binary.LittleEndian.Uint64(tsBytes))

		record := &WALRecord{
			Op:    op,
			Key:   key,
			Value: val,
			Ts:    ts,
		}
		if err := s.applyRecord(record); err != nil {
			return fmt.Errorf("%w:%w", ErrWalWriteFailed, err)
		}

	}
}
