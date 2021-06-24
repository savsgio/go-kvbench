package leveldb

import (
	"errors"
	"os"
	"sync"

	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/common"
	"github.com/savsgio/kvbench/internal/store"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type DB struct {
	path      string
	fsync     bool
	db        *leveldb.DB
	wo        opt.WriteOptions
	mu        sync.RWMutex
	batchPool sync.Pool
}

func New(path string, fsync bool) (store.DB, error) {
	db := &DB{
		path:  path,
		fsync: fsync,
		wo:    opt.WriteOptions{Sync: fsync},
		batchPool: sync.Pool{
			New: func() interface{} {
				return new(leveldb.Batch)
			},
		},
	}

	if err := db.init(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) init() error {
	opts := &opt.Options{NoSync: !db.fsync}

	ldb, err := leveldb.OpenFile(db.path, opts)
	if err != nil {
		return err
	}

	db.db = ldb

	return nil
}

func (db *DB) acquireBatch() *leveldb.Batch {
	return db.batchPool.Get().(*leveldb.Batch)
}

func (db *DB) releaseBatch(batch *leveldb.Batch) {
	batch.Reset()
	db.batchPool.Put(batch)
}

func (db *DB) Set(key, value []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.Put(key, value, &db.wo)
}

func (db *DB) SetString(key string, value []byte) error {
	return db.Set(strconv.S2B(key), value)
}

func (db *DB) SetBulk(kvs ...common.KV) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	batch := db.acquireBatch()
	defer db.releaseBatch(batch)

	for i := range kvs {
		kv := kvs[i]

		batch.Put(kv.Key, kv.Value)
	}

	return db.db.Write(batch, &db.wo)
}

func (db *DB) get(key []byte) ([]byte, error) {
	value, err := db.db.Get(key, nil)

	switch {
	case err != nil && errors.Is(err, leveldb.ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	}

	return value, nil
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
	ok, err := db.db.Has(key, nil)
	if !ok || err != nil {
		return err
	}

	err = db.db.Delete(key, &db.wo)
	if err != nil {
		return err
	}

	return nil
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

	batch := db.acquireBatch()
	defer db.releaseBatch(batch)

	for i := range keys {
		batch.Delete(keys[i])
	}

	return db.db.Write(batch, &db.wo)
}

func (db *DB) Iter(fn common.IterFunc) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	it := db.db.NewIterator(nil, nil)
	defer it.Release()

	for it.First(); it.Next(); {
		if err := fn(it.Key(), it.Value()); err != nil {
			return err
		}
	}

	return it.Error()
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
