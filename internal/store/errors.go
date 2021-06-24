package store

import "errors"

var (
	ErrMemoryNotAllowed = errors.New(":memory: path not available")
	ErrUnsupported      = errors.New("unsupported")
	ErrInit             = errors.New("failed to init")
	ErrEmptyKey         = errors.New("key cannot be empty")
)
