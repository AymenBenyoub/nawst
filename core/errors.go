package core

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyExists   = errors.New("key already exists")
	ErrInvalidKey  = errors.New("invalid key")
	ErrInvalidOperation = errors.New("invalid operation")
	ErrWalWriteFailed = errors.New("WAL write failed")
)
