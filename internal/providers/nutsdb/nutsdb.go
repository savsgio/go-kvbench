package nutsdb

import (
	"os"
	"sync"

	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/store"
	"github.com/xujiajun/nutsdb"
)

const bucket = "kvbench"

type DB struct {
	db    *nutsdb.DB
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

func newDB(path string, fsync bool) (*nutsdb.DB, error) {
	opt := nutsdb.DefaultOptions
	opt.SyncEnable = fsync
	opt.Dir = path

	db, err := nutsdb.Open(opt)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Set(key, value []byte) error {
	return db.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(bucket, key, value, 0)
	})
}

func (db *DB) SetString(key string, value []byte) error {
	return db.Set(strconv.S2B(key), value)
}

func (db *DB) SetBulk(kvs []store.KV) error {
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
		if err != nil {
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

func (db *DB) GetBulk(keys [][]byte) ([]store.KV, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	kvs := make([]store.KV, len(keys))

	err := db.db.View(func(tx *nutsdb.Tx) error {
		for i := range keys {
			key := keys[i]

			e, err := tx.Get(bucket, key)
			if err != nil {
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

func (db *DB) DelBulk(keys [][]byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.Update(func(tx *nutsdb.Tx) error {
		for i := range keys {
			if err := tx.Delete(bucket, keys[i]); err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *DB) Keys(pattern []byte, limit int, withvals bool) ([]store.KV, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var kvs []store.KV

	err := db.db.View(func(tx *nutsdb.Tx) error {
		entries, _, err := tx.PrefixScan(bucket, pattern, 0, nutsdb.ScanNoLimit)
		if err != nil {
			return err
		}

		for i := range entries {
			entry := entries[i]

			kv := store.KV{}
			kv.Key = append(kv.Key, entry.Key...)

			if withvals {
				kv.Value = append(kv.Value, entry.Value...)
			}

			kvs = append(kvs, kv)
		}

		return nil
	})

	if err != nil {
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

	ndb, err := newDB(db.path, db.fsync)
	if err != nil {
		return err
	}

	db.db = ndb

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
