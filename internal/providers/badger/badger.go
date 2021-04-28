package badger

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/store"
)

type DB struct {
	db *badger.DB
}

func New(path string, fsync bool) (store.Store, error) {
	opts := badger.DefaultOptions(path)
	if path == ":memory:" {
		opts.InMemory = true
	}

	opts.SyncWrites = fsync
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &DB{
		db: db,
	}, nil
}

func (db *DB) Set(key, value []byte) error {
	return db.db.Update(func(tx *badger.Txn) error {
		return tx.Set(key, value)
	})
}

func (db *DB) SetString(key string, value []byte) error {
	return db.Set(strconv.S2B(key), value)
}

func (db *DB) SetBulk(kvs []store.KV) error {
	wb := db.db.NewWriteBatch()

	for i := range kvs {
		kv := kvs[i]

		err := wb.Set(kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}

	return wb.Flush()
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	err = db.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(key)
		if err == nil {
			err = item.Value(func(v []byte) error {
				value = append(value, v...)

				return nil
			})
		}

		return err
	})

	return value, err
}

func (db *DB) GetString(key string) ([]byte, error) {
	return db.Get(strconv.S2B(key))
}

func (db *DB) GetBulk(keys [][]byte) ([]store.KV, error) {
	kvs := make([]store.KV, len(keys))

	err := db.db.View(func(tx *badger.Txn) error {
		for i := range keys {
			key := keys[i]

			item, err := tx.Get(key)
			if err != nil {
				return err
			}

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			kv := &kvs[i]
			kv.Key = append(kv.Key, key...)
			kv.Value = append(kv.Value, value...)
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

func (db *DB) DelBulk(keys [][]byte) error {
	return db.db.Update(func(tx *badger.Txn) error {
		for i := range keys {
			if err := tx.Delete(keys[i]); err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *DB) Keys(pattern []byte, limit int, withvals bool) ([]store.KV, error) {
	var kvs []store.KV

	err := db.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(pattern); it.ValidForPrefix(pattern); it.Next() {
			if limit > -1 && len(kvs) >= limit {
				break
			}

			item := it.Item()

			kv := store.KV{}
			kv.Key = append(kv.Key, item.Key()...)

			if withvals {
				value, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}

				kv.Value = append(kv.Value, value...)
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
	return db.db.DropAll()
}

func (db *DB) Close() error {
	return db.db.Close()
}
