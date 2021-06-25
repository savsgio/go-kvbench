package pebble

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/savsgio/gotils/strconv"
	"github.com/savsgio/kvbench/internal/common"
	"github.com/savsgio/kvbench/internal/store"
)

type DB struct {
	path  string
	fsync bool
	db    *pebble.DB
	wo    *pebble.WriteOptions
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
	opts := &pebble.Options{}
	// if !db.fsync {
	// 	opts.DisableWAL = true
	// }

	db.wo = &pebble.WriteOptions{
		Sync: db.fsync,
	}

	pdb, err := pebble.Open(db.path, opts)
	if err != nil {
		return err
	}

	db.db = pdb

	return nil
}

func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return store.ErrEmptyKey
	}

	return db.db.Set(key, value, db.wo)
}

func (db *DB) SetString(key string, value []byte) error {
	return db.Set(strconv.S2B(key), value)
}

func (db *DB) SetBulk(kvs ...common.KV) error {
	batch := db.db.NewBatch()
	defer batch.Close()

	for i := range kvs {
		kv := kvs[i]

		if len(kv.Key) == 0 {
			return store.ErrEmptyKey
		}

		if err := batch.Set(kv.Key, kv.Value, db.wo); err != nil {
			return err
		}
	}

	return batch.Commit(db.wo)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, store.ErrEmptyKey
	}

	v, closer, err := db.db.Get(key)

	switch {
	case err != nil && errors.Is(err, pebble.ErrNotFound):
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

func (db *DB) GetBulk(keys ...[]byte) ([]common.KV, error) {
	kvs := make([]common.KV, len(keys))

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
	if len(key) == 0 {
		return store.ErrEmptyKey
	}

	return db.db.SingleDelete(key, db.wo)
}

func (db *DB) DelString(key string) error {
	return db.Del(strconv.S2B(key))
}

func (db *DB) DelBulk(keys ...[]byte) error {
	batch := db.db.NewBatch()
	defer batch.Close()

	for i := range keys {
		key := keys[i]

		if len(key) == 0 {
			return store.ErrEmptyKey
		}

		if err := batch.Delete(key, db.wo); err != nil {
			return err
		}
	}

	return batch.Commit(db.wo)
}

func (db *DB) Iter(fn common.IterFunc) error {
	snapshot := db.db.NewSnapshot()
	defer snapshot.Close()

	it := snapshot.NewIter(&pebble.IterOptions{})
	defer it.Close()

	it.First()

	for it.Next() {
		if err := fn(it.Key(), it.Value()); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Flush() error {
	return db.db.Flush()
}

func (db *DB) Close() error {
	return db.db.Close()
}
