package core

import (
	"slices"
)

type Store struct {
	storage map[string][]byte
}

func NewStore() *Store {
	return &Store{
		storage: make(map[string][]byte),
	}
}

func (s *Store) Apply(cmd Command) error {

	switch cmd.Op {
	case OpPut:
		s.storage[cmd.Key] = slices.Clone(cmd.Value)
	case OpDelete:
		delete(s.storage, cmd.Key)
	default:
		return ErrInvalidOperation
	}
	return nil
}

func (s *Store) Get(key string) ([]byte, error) {
	val, exists := s.storage[key]
	if !exists {
		return nil, ErrKeyNotFound
	}
	return slices.Clone(val), nil
}
