package providers

import (
	"encoding/binary"
	"flag"
	"os"
	"testing"

	"github.com/savsgio/kvbench/internal/providers/badger"
	"github.com/savsgio/kvbench/internal/providers/buntdb"
	"github.com/savsgio/kvbench/internal/providers/leveldb"
	"github.com/savsgio/kvbench/internal/providers/nutsdb"
	"github.com/savsgio/kvbench/internal/providers/pebble"
	"github.com/savsgio/kvbench/internal/providers/pogreb"
	"github.com/savsgio/kvbench/internal/store"
)

var count = flag.Int("count", 1000, "item count for test")

var stores = []struct {
	Name    string
	Path    string
	Factory func(path string, fsync bool) (store.DB, error)
}{
	{"badger", "badger.db", badger.New},
	{"leveldb", "leveldb.db", leveldb.New},
	{"buntdb", "buntdb.db", buntdb.New},
	{"pebble", "pebble.db", pebble.New},
	{"pogreb", "pogreb.db", pogreb.New},
	{"nutsdb", "nutsdb.db", nutsdb.New},
}

func prefixKey(i int) []byte {
	r := make([]byte, 8)
	binary.BigEndian.PutUint64(r, uint64(i))

	return r
}

func wrapfsync(fn func(*testing.T, store.DB, bool), s store.DB, fsync bool) func(*testing.T) {
	return func(t *testing.T) {
		fn(t, s, fsync)
	}
}
func TestStore_fsync(t *testing.T) {
	for _, s := range stores {
		store, err := s.Factory(s.Path, true)
		if err != nil {
			os.RemoveAll(s.Path)
			t.Fatal(err)
		}
		t.Run(s.Name, wrapfsync(testStore, store, true))
		os.RemoveAll(s.Path)
	}
}

func TestStore_nofsync(t *testing.T) {
	for _, s := range stores {
		store, err := s.Factory(s.Path, false)
		if err != nil {
			os.RemoveAll(s.Path)
			t.Fatal(err)
		}
		t.Run(s.Name, wrapfsync(testStore, store, false))
		os.RemoveAll(s.Path)
	}
}

func testStore(t *testing.T, s store.DB, fsync bool) {
	v := make([]byte, 256)

	defer s.Close()

	t.Run("set", func(tt *testing.T) {
		for i := 0; i < *count; i++ {
			err := s.Set(prefixKey(i), v)
			if err != nil {
				tt.Fatalf("failed to set key %d: %v", i, err)
			}
		}
	})

	t.Run("get", func(tt *testing.T) {
		for i := 0; i < *count; i++ {
			value, err := s.Get(prefixKey(i))
			if err != nil {
				tt.Fatalf("failed to get key %d: %v", i, err)
			}
			if len(value) == 0 {
				tt.Fatalf("the key %d does not exist", i)
			}
		}
	})
}
