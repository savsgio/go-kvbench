package store

import "errors"

var (
	ErrMemoryNotAllowed = errors.New(":memory: path not available")
	ErrUnsupported      = errors.New("unsupported")
)
