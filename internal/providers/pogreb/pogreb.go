package pogreb

import (
	"os"
	"sync"

	"github.com/akrylysov/pogreb"
	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/store"
)

type DB struct {
	db    *pogreb.DB
	path  string
	fsync bool
	mu    sync.RWMutex
}

func New(path string, fsync bool) (store.Store, error) {
	if path == ":memory:" {
		return nil, store.ErrMemoryNotAllowed
	}

	db, err := newDB(path, fsync)
	if err != nil {
		return nil, err
	}

	return &DB{
		db:    db,
		path:  path,
		fsync: fsync,
	}, nil
}

func newDB(path string, fsync bool) (*pogreb.DB, error) {
	opts := new(pogreb.Options)
	if fsync {
		opts.BackgroundSyncInterval = -1
	}

	db, err := pogreb.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return db, nil
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

func (db *DB) SetBulk(kvs []store.KV) error {
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

func (db *DB) GetBulk(keys [][]byte) ([]store.KV, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	kvs := make([]store.KV, len(keys))

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

func (db *DB) DelBulk(keys [][]byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for i := range keys {
		if err := db.del(keys[i]); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Keys(pattern []byte, limit int, withvals bool) ([]store.KV, error) {
	return nil, store.ErrUnsupported
}

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.close(); err != nil {
		return err
	}

	os.RemoveAll(db.path)

	pdb, err := newDB(db.path, db.fsync)
	if err != nil {
		return err
	}

	db.db = pdb

	return nil
}

func (db *DB) close() error {
	return db.db.Close()
}

func (db *DB) Close() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.close()
}
