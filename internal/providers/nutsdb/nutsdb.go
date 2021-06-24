package nutsdb

import (
	"errors"
	"sync"

	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/common"
	"github.com/savsgio/kvbench/internal/store"
	"github.com/xujiajun/nutsdb"
)

const (
	bucket  = "r2d2"
	keyInit = "init"
)

type DB struct {
	path  string
	fsync bool
	db    *nutsdb.DB
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
	opt := nutsdb.DefaultOptions
	opt.Dir = db.path
	opt.EntryIdxMode = nutsdb.HintBPTSparseIdxMode
	opt.SyncEnable = db.fsync

	ndb, err := nutsdb.Open(opt)
	if err != nil {
		return err
	}

	db.db = ndb

	if err := db.SetString(keyInit, nil); err != nil {
		return store.ErrInit
	}

	if err := db.DelString(keyInit); err != nil {
		return store.ErrInit
	}

	return nil
}

func (db *DB) Set(key, value []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(bucket, key, value, 0)
	})
}

func (db *DB) SetString(key string, value []byte) error {
	return db.Set(strconv.S2B(key), value)
}

func (db *DB) SetBulk(kvs ...common.KV) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.Update(func(tx *nutsdb.Tx) error {
		for i := range kvs {
			kv := kvs[i]

			if err := tx.Put(bucket, kv.Key, kv.Value, 0); err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	err = db.db.View(func(tx *nutsdb.Tx) error {
		e, err := tx.Get(bucket, key)

		switch {
		case err != nil && errors.Is(err, nutsdb.ErrKeyNotFound), errors.Is(err, nutsdb.ErrNotFoundKey):
			return nil
		case err != nil:
			return err
		}

		value = e.Value

		return nil
	})

	return value, err
}

func (db *DB) GetString(key string) ([]byte, error) {
	return db.Get(strconv.S2B(key))
}

func (db *DB) GetBulk(keys ...[]byte) ([]common.KV, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	kvs := make([]common.KV, len(keys))

	err := db.db.View(func(tx *nutsdb.Tx) error {
		for i := range keys {
			key := keys[i]

			e, err := tx.Get(bucket, key)
			if err != nil && !(errors.Is(err, nutsdb.ErrKeyNotFound) || errors.Is(err, nutsdb.ErrNotFoundKey)) {
				return err
			}

			kv := &kvs[i]
			kv.Key = append(kv.Key, key...)
			kv.Value = append(kv.Value, e.Value...)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return kvs, nil
}

func (db *DB) Del(key []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Delete(bucket, key)
	})
}

func (db *DB) DelString(key string) error {
	return db.Del(strconv.S2B(key))
}

func (db *DB) DelBulk(keys ...[]byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.Update(func(tx *nutsdb.Tx) error {
		for i := range keys {
			key := keys[i]

			if err := tx.Delete(bucket, key); err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *DB) Iter(fn common.IterFunc) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.View(func(tx *nutsdb.Tx) error {
		entries, err := tx.GetAll(bucket)

		switch {
		case err != nil && errors.Is(err, nutsdb.ErrBucketEmpty):
			return nil
		case err != nil:
			return err
		}

		for i := range entries {
			entry := entries[i]

			if err := fn(entry.Key, entry.Value); err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *DB) Flush() error {
	return db.db.ActiveFile.Sync()
}

func (db *DB) close() error {
	return db.db.Close()
}

func (db *DB) Close() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.close()
}
