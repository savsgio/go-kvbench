package buntdb

import (
	"errors"

	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/store"
	"github.com/tidwall/buntdb"
)

type DB struct {
	db *buntdb.DB
}

func New(path string, fsync bool) (store.Store, error) {
	if path == ":memory:" {
		return nil, store.ErrMemoryNotAllowed
	}

	opts := buntdb.Config{}
	if fsync {
		opts.SyncPolicy = buntdb.Always
	}

	db, err := buntdb.Open(path)
	if err != nil {
		return nil, err
	}

	if err := db.SetConfig(opts); err != nil {
		return nil, err
	}

	return &DB{
		db: db,
	}, nil
}

func (db *DB) Set(key, value []byte) error {
	return db.SetString(strconv.B2S(key), value)
}

func (db *DB) SetString(key string, value []byte) error {
	return db.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, strconv.B2S(value), nil)

		return err
	})
}

func (db *DB) SetBulk(kvs []store.KV) error {
	return db.db.Update(func(tx *buntdb.Tx) error {
		for i := range kvs {
			kv := kvs[i]

			if _, _, err := tx.Set(strconv.B2S(kv.Key), strconv.B2S(kv.Value), nil); err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *DB) Get(key []byte) (val []byte, err error) {
	return db.GetString(strconv.B2S(key))
}

func (db *DB) GetString(key string) (value []byte, err error) {
	err = db.db.View(func(tx *buntdb.Tx) error {
		v, err := tx.Get(key)

		switch {
		case errors.Is(err, buntdb.ErrNotFound):
			return nil
		case err != nil:
			return err
		}

		value = []byte(v) // Copy??

		return nil
	})

	return value, err
}

func (db *DB) GetBulk(keys [][]byte) ([]store.KV, error) {
	kvs := make([]store.KV, len(keys))

	err := db.db.View(func(tx *buntdb.Tx) error {
		for i := range keys {
			key := keys[i]

			value, err := tx.Get(string(key))
			if err != nil && !errors.Is(err, buntdb.ErrNotFound) {
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
	return db.DelString(strconv.B2S(key))
}

func (db *DB) DelString(key string) error {
	return db.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(key)

		if err != nil && !errors.Is(err, buntdb.ErrNotFound) {
			return err
		}

		return nil
	})
}

func (db *DB) DelBulk(keys [][]byte) error {
	return db.db.Update(func(tx *buntdb.Tx) error {
		for i := range keys {
			_, err := tx.Delete(strconv.B2S(keys[i]))

			switch {
			case errors.Is(err, buntdb.ErrNotFound):
				continue
			case err != nil:
				return err
			}
		}

		return nil
	})
}

func (db *DB) Keys(pattern []byte, limit int, withvals bool) ([]store.KV, error) {
	var kvs []store.KV

	err := db.db.View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(string(pattern), func(key, value string) bool {
			kv := store.KV{}
			kv.Key = append(kv.Key, key...)

			if withvals {
				value := []byte(value) // Copy??
				kv.Value = append(kv.Value, value...)
			}

			kvs = append(kvs, kv)

			return true
		})
	})

	if err != nil {
		return nil, err
	}

	return kvs, nil
}

func (db *DB) Flush() error {
	return db.db.Update(func(tx *buntdb.Tx) error {
		return tx.DeleteAll()
	})
}

func (db *DB) Close() error {
	return db.db.Close()
}
