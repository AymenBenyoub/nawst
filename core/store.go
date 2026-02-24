package core

type Store struct {
	Storage map[string][]byte
	WAL     *Wal
}

func newStore() *Store {
	return &Store{
		Storage: make(map[string][]byte),
	}
}

func (s *Store) Put(key string, value []byte) error {

	return nil

}
func (s *Store) Get(key string) ([]byte, error) {
	return s.Storage[key], nil
}

func (s *Store) Update(key string, value []byte) error {
	s.Storage[key] = value
	return nil
}

func (s *Store) Delete(key string) error {
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
