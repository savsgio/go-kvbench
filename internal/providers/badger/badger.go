package badger

import (
	"errors"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/common"
	"github.com/savsgio/kvbench/internal/store"
)

type DB struct {
	path  string
	fsync bool
	db    *badger.DB
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
	opts := badger.DefaultOptions(db.path)
	opts.SyncWrites = db.fsync

	if db.path == ":memory:" {
		opts.InMemory = true
	}

	bdb, err := badger.Open(opts)
	if err != nil {
		return err
	}

	db.db = bdb

	return nil
}

func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return store.ErrEmptyKey
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.Update(func(tx *badger.Txn) error {
		return tx.Set(key, value)
	})
}

func (db *DB) SetString(key string, value []byte) error {
	return db.Set(strconv.S2B(key), value)
}

func (db *DB) SetBulk(kvs ...common.KV) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	wb := db.db.NewWriteBatch()

	for i := range kvs {
		kv := kvs[i]

		if len(kv.Key) == 0 {
			wb.Cancel()

			return store.ErrEmptyKey
		}

		if err := wb.Set(kv.Key, kv.Value); err != nil {
			wb.Cancel()

			return err
		}
	}

	return wb.Flush()
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	if len(key) == 0 {
		return nil, store.ErrEmptyKey
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	if err = db.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(key)

		switch {
		case err != nil && errors.Is(err, badger.ErrKeyNotFound):
			return nil
		case err != nil:
			return err
		}

		value, err = item.ValueCopy(value)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return value, nil
}

func (db *DB) GetString(key string) ([]byte, error) {
	return db.Get(strconv.S2B(key))
}

func (db *DB) GetBulk(keys ...[]byte) ([]common.KV, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	kvs := make([]common.KV, len(keys))

	if err := db.db.View(func(tx *badger.Txn) error {
		for i := range keys {
			key := keys[i]

			if len(key) == 0 {
				return store.ErrEmptyKey
			}

			kv := &kvs[i]
			kv.Key = append(kv.Key, key...)

			item, err := tx.Get(key)

			switch {
			case err != nil && errors.Is(err, badger.ErrKeyNotFound):
				continue
			case err != nil:
				return err
			}

			kv.Value, err = item.ValueCopy(kv.Value)
			if err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return kvs, nil
}

func (db *DB) Del(key []byte) error {
	if len(key) == 0 {
		return store.ErrEmptyKey
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.Update(func(tx *badger.Txn) error {
		return tx.Delete(key)
	})
}

func (db *DB) DelString(key string) error {
	return db.Del(strconv.S2B(key))
}

func (db *DB) DelBulk(keys ...[]byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.Update(func(tx *badger.Txn) error {
		for i := range keys {
			key := keys[i]

			if len(key) == 0 {
				return store.ErrEmptyKey
			}

			if err := tx.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *DB) Iter(fn common.IterFunc) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			if err := fn(key, value); err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.db.Sync()
}

func (db *DB) Reset() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.db.DropAll()
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.db.Close()
}
