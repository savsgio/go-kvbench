package leveldb

import (
	"errors"
	"os"
	"sync"

	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/store"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tidwall/match"
)

type DB struct {
	db        *leveldb.DB
	wo        opt.WriteOptions
	path      string
	fsync     bool
	mu        sync.RWMutex
	batchPool sync.Pool
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
		wo:    opt.WriteOptions{Sync: fsync},
		path:  path,
		fsync: fsync,
		batchPool: sync.Pool{
			New: func() interface{} {
				return new(leveldb.Batch)
			},
		},
	}, nil
}

func newDB(path string, fsync bool) (*leveldb.DB, error) {
	opts := &opt.Options{NoSync: !fsync}
	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		return nil, err
	}

	return db, nil
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

func (db *DB) SetBulk(kvs []store.KV) error {
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
	case errors.Is(err, leveldb.ErrNotFound):
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

func (db *DB) DelBulk(keys [][]byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	batch := db.acquireBatch()
	defer db.releaseBatch(batch)

	for i := range keys {
		batch.Delete(keys[i])
	}

	return db.db.Write(batch, &db.wo)
}

func (db *DB) Keys(pattern []byte, limit int, withvals bool) ([]store.KV, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var kvs []store.KV

	spattern := strconv.B2S(pattern)
	min, max := match.Allowable(spattern)
	bmin := strconv.S2B(min)
	useMax := !(len(spattern) > 0 && spattern[0] == '*')
	iter := db.db.NewIterator(nil, nil)

	for ok := iter.Seek(bmin); ok; ok = iter.Next() {
		if limit > -1 && len(kvs) >= limit {
			break
		}

		key := iter.Key()
		value := iter.Value()

		strKey := strconv.B2S(key)
		if useMax && strKey >= max {
			break
		}

		if match.Match(strKey, spattern) {
			kv := store.KV{}
			kv.Key = append(kv.Key, key...)

			if withvals {
				kv.Value = append(kv.Value, value...)
			}

			kvs = append(kvs, kv)
		}
	}

	iter.Release()

	if err := iter.Error(); err != nil {
		return nil, err
	}

	return kvs, nil
}

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.close(); err != nil {
		return err
	}

	os.RemoveAll(db.path)

	ldb, err := newDB(db.path, db.fsync)
	if err != nil {
		return err
	}

	db.db = ldb

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
