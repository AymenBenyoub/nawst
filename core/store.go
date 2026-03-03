package core

import (
	"fmt"
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
		fmt.Println("Apply PUT len: , value: ", len(cmd.Value), string(cmd.Value))
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
	fmt.Println("Apply GET len: , value: ", len(val), string(val))
	return slices.Clone(val), nil
}
