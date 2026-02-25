package core

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")

	ErrInvalidKey       = errors.New("invalid key")
	ErrInvalidOperation = errors.New("invalid operation")
)
