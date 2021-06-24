package pogreb

import (
	"errors"
	"os"
	"sync"

	"github.com/akrylysov/pogreb"
	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/common"
	"github.com/savsgio/kvbench/internal/store"
)

type DB struct {
	path  string
	fsync bool
	db    *pogreb.DB
	mu    sync.RWMutex
}

func New(path string, fsync bool) (store.DB, error) {
	db := &DB{
		path:  path,
		fsync: fsync,
	}

	if err := db.init(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) init() error {
	if db.path == ":memory:" {
		return store.ErrMemoryNotAllowed
	}

	opts := new(pogreb.Options)
	if db.fsync {
		opts.BackgroundSyncInterval = -1
	}

	pdb, err := pogreb.Open(db.path, opts)
	if err != nil {
		return err
	}

	db.db = pdb

	return nil
}

func (db *DB) set(key, value []byte) error {
	return db.db.Put(key, value)
}

func (db *DB) Set(key, value []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.set(key, value)
}

func (db *DB) SetString(key string, value []byte) error {
	return db.Set(strconv.S2B(key), value)
}

func (db *DB) SetBulk(kvs ...common.KV) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for i := range kvs {
		kv := kvs[i]

		if err := db.set(kv.Key, kv.Value); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) get(key []byte) ([]byte, error) {
	return db.db.Get(key)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.get(key)
}

func (db *DB) GetString(key string) (val []byte, err error) {
	return db.Get(strconv.S2B(key))
}

func (db *DB) GetBulk(keys ...[]byte) ([]common.KV, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	kvs := make([]common.KV, len(keys))

	for i := range keys {
		key := keys[i]

		value, err := db.get(key)
		if err != nil {
			return nil, err
		}

		kv := &kvs[i]
		kv.Key = append(kv.Key, key...)
		kv.Value = append(kv.Value, value...)
	}

	return kvs, nil
}

func (db *DB) del(key []byte) error {
	return db.db.Delete(key)
}

func (db *DB) Del(key []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.del(key)
}

func (db *DB) DelString(key string) error {
	return db.Del(strconv.S2B(key))
}

func (db *DB) DelBulk(keys ...[]byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for i := range keys {
		if err := db.del(keys[i]); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Iter(fn common.IterFunc) error {
	it := db.db.Items()

	for {
		key, value, err := it.Next()

		switch {
		case err != nil && errors.Is(err, pogreb.ErrIterationDone):
			return nil
		case err != nil:
			return err
		}

		if err := fn(key, value); err != nil {
			return err
		}
	}
}

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.close(); err != nil {
		return err
	}

	os.RemoveAll(db.path)

	return db.init()
}

func (db *DB) close() error {
	return db.db.Close()
}

func (db *DB) Close() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.close()
}
