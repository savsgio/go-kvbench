package store

import "github.com/savsgio/kvbench/internal/common"

type DB interface {
	Set(key, value []byte) error
	SetString(key string, value []byte) error
	SetBulk(kvs ...common.KV) error
	Get(key []byte) ([]byte, error)
	GetString(key string) ([]byte, error)
	GetBulk(keys ...[]byte) ([]common.KV, error)
	Del(key []byte) error
	DelString(key string) error
	DelBulk(key ...[]byte) error
	Iter(fn common.IterFunc) error
	Flush() error
	// Reset() error
	Close() error
}
