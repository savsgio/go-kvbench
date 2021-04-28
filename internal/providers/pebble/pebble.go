package pebble

import (
	"bytes"
	"errors"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/store"
)

type DB struct {
	db        *pebble.DB
	wo        *pebble.WriteOptions
	batchPool sync.Pool
}

func New(path string, fsync bool) (store.Store, error) {
	if path == ":memory:" {
		return nil, store.ErrMemoryNotAllowed
	}

	opts := &pebble.Options{}
	if !fsync {
		opts.DisableWAL = true
	}

	wo := &pebble.WriteOptions{}
	wo.Sync = fsync

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &DB{
		db: db,
		wo: wo,
		batchPool: sync.Pool{
			New: func() interface{} {
				return db.NewBatch()
			},
		},
	}, nil
}

func (db *DB) acquireBatch() *pebble.Batch {
	return db.batchPool.Get().(*pebble.Batch)
}

func (db *DB) releaseBatch(batch *pebble.Batch) {
	batch.Reset()
	db.batchPool.Put(batch)
}

func (db *DB) Set(key, value []byte) error {
	return db.db.Set(key, value, db.wo)
}

func (db *DB) SetString(key string, value []byte) error {
	return db.Set(strconv.S2B(key), value)
}

func (db *DB) SetBulk(kvs []store.KV) error {
	batch := db.acquireBatch()
	defer db.releaseBatch(batch)

	for i := range kvs {
		kv := kvs[i]

		if err := batch.Set(kv.Key, kv.Value, db.wo); err != nil {
			return err
		}
	}

	return batch.Commit(db.wo)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	v, closer, err := db.db.Get(key)

	switch {
	case errors.Is(err, pebble.ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	default:
		closer.Close()
	}

	return v, nil
}

func (db *DB) GetString(key string) (val []byte, err error) {
	return db.Get(strconv.S2B(key))
}

func (db *DB) GetBulk(keys [][]byte) ([]store.KV, error) {
	kvs := make([]store.KV, len(keys))

	for i := range keys {
		key := keys[i]

		value, err := db.Get(key)
		if err != nil {
			return nil, err
		}

		kv := &kvs[i]
		kv.Key = append(kv.Key, key...)
		kv.Value = append(kv.Value, value...)
	}

	return kvs, nil
}

func (db *DB) Del(key []byte) error {
	return db.db.Delete(key, db.wo)
}

func (db *DB) DelString(key string) error {
	return db.Del(strconv.S2B(key))
}

func (db *DB) DelBulk(keys [][]byte) error {
	for i := range keys {
		if err := db.Del(keys[i]); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Keys(pattern []byte, limit int, withvals bool) ([]store.KV, error) {
	var kvs []store.KV

	it := db.db.NewIter(&pebble.IterOptions{})
	defer it.Close()

	it.SeekGE(pattern)

	for it.Valid() && it.Next() {
		if limit > -1 && len(kvs) >= limit {
			break
		}

		key := it.Key()
		if !bytes.HasPrefix(key, pattern) {
			continue
		}

		kv := store.KV{}
		kv.Key = append(kv.Key, key...)

		if withvals {
			value := it.Value()
			kv.Value = append(kv.Value, value...)
		}

		kvs = append(kvs, kv)
	}

	return kvs, nil
}

func (db *DB) Flush() error {
	return db.db.Flush()
}

func (db *DB) Close() error {
	return db.db.Close()
}
