package badger

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/common"
	"github.com/savsgio/kvbench/internal/store"
)

type DB struct {
	path  string
	fsync bool
	db    *badger.DB
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
	if db.path == ":memory:" {
		opts.InMemory = true
	}

	opts.SyncWrites = db.fsync
	bdb, err := badger.Open(opts)
	if err != nil {
		return err
	}

	db.db = bdb

	return nil
}

func (db *DB) Set(key, value []byte) error {
	return db.db.Update(func(tx *badger.Txn) error {
		return tx.Set(key, value)
	})
}

func (db *DB) SetString(key string, value []byte) error {
	return db.Set(strconv.S2B(key), value)
}

func (db *DB) SetBulk(kvs ...common.KV) error {
	wb := db.db.NewWriteBatch()

	for i := range kvs {
		kv := kvs[i]

		if err := wb.Set(kv.Key, kv.Value); err != nil {
			return err
		}
	}

	return wb.Flush()
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	err = db.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(key)
		if err != nil {
			return err
		}

		value, err = item.ValueCopy(value)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (db *DB) GetString(key string) ([]byte, error) {
	return db.Get(strconv.S2B(key))
}

func (db *DB) GetBulk(keys ...[]byte) ([]common.KV, error) {
	kvs := make([]common.KV, len(keys))

	err := db.db.View(func(tx *badger.Txn) error {
		for i := range keys {
			key := keys[i]

			item, err := tx.Get(key)
			if err != nil {
				return err
			}

			kv := &kvs[i]
			kv.Key = append(kv.Key, key...)

			kv.Value, err = item.ValueCopy(kv.Value)
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return kvs, nil
}

func (db *DB) Del(key []byte) error {
	return db.db.Update(func(tx *badger.Txn) error {
		return tx.Delete(key)
	})
}

func (db *DB) DelString(key string) error {
	return db.Del(strconv.S2B(key))
}

func (db *DB) DelBulk(keys ...[]byte) error {
	return db.db.Update(func(tx *badger.Txn) error {
		for i := range keys {
			if err := tx.Delete(keys[i]); err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *DB) Iter(fn common.IterFunc) error {
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
	return db.db.DropAll()
}

func (db *DB) Close() error {
	return db.db.Close()
}
