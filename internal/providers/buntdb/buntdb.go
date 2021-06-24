package buntdb

import (
	"errors"

	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/common"
	"github.com/savsgio/kvbench/internal/store"
	"github.com/tidwall/buntdb"
)

type DB struct {
	path  string
	fsync bool
	db    *buntdb.DB
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
	if db.path == ":memory:" {
		return store.ErrMemoryNotAllowed
	}

	opts := buntdb.Config{}
	if db.fsync {
		opts.SyncPolicy = buntdb.Always
	}

	bdb, err := buntdb.Open(db.path)
	if err != nil {
		return err
	}

	if err := bdb.SetConfig(opts); err != nil {
		return err
	}

	db.db = bdb

	return nil
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

func (db *DB) SetBulk(kvs ...common.KV) error {
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
		case err != nil && errors.Is(err, buntdb.ErrNotFound):
			return nil
		case err != nil:
			return err
		}

		value = []byte(v)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (db *DB) GetBulk(keys ...[]byte) ([]common.KV, error) {
	kvs := make([]common.KV, len(keys))

	err := db.db.View(func(tx *buntdb.Tx) error {
		for i := range keys {
			key := keys[i]

			value, err := tx.Get(strconv.B2S(key))
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

func (db *DB) DelBulk(keys ...[]byte) error {
	return db.db.Update(func(tx *buntdb.Tx) error {
		for i := range keys {
			_, err := tx.Delete(strconv.B2S(keys[i]))

			switch {
			case err != nil && errors.Is(err, buntdb.ErrNotFound):
				continue
			case err != nil:
				return err
			}
		}

		return nil
	})
}

func (db *DB) Iter(fn common.IterFunc) (err error) {
	err2 := db.db.View(func(tx *buntdb.Tx) error {
		return tx.Ascend("", func(key, value string) bool {
			err = fn(strconv.S2B(key), strconv.S2B(value))

			return err == nil
		})
	})

	if err2 != nil {
		return err2
	}

	return err
}

func (db *DB) Flush() error {
	return db.db.Update(func(tx *buntdb.Tx) error {
		return tx.DeleteAll()
	})
}

func (db *DB) Close() error {
	return db.db.Close()
}
