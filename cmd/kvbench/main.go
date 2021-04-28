package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/savsgio/kvbench/internal/providers/badger"
	"github.com/savsgio/kvbench/internal/providers/buntdb"
	"github.com/savsgio/kvbench/internal/providers/leveldb"
	"github.com/savsgio/kvbench/internal/providers/nutsdb"
	"github.com/savsgio/kvbench/internal/providers/pebble"
	"github.com/savsgio/kvbench/internal/providers/pogreb"
	"github.com/savsgio/kvbench/internal/store"
)

var (
	duration = flag.Duration("d", time.Minute, "test duration for each case")
	c        = flag.Int("c", runtime.NumCPU(), "concurrent goroutines")
	size     = flag.Int("size", 256, "data size")
	fsync    = flag.Bool("fsync", false, "fsync")
	s        = flag.String("s", "map", "store type")

	data = make([]byte, *size)
)

func main() {
	flag.Parse()

	fmt.Printf("duration=%v, c=%d size=%d\n", *duration, *c, *size)

	var memory bool
	var path string

	if strings.HasSuffix(*s, "/memory") {
		memory = true
		path = ":memory:"
		*s = strings.TrimSuffix(*s, "/memory")
	}

	st, path, err := getStore(*s, *fsync, path)
	if err != nil {
		panic(err)
	}

	if !memory {
		defer os.RemoveAll(path)
	}

	defer st.Close()

	name := *s
	if memory {
		name = name + "/memory"
	}

	if *fsync {
		name = name + "/fsync"
	} else {
		name = name + "/nofsync"
	}

	testBatchWrite(name, st)
	testSet(name, st)
	testGet(name, st)
	testGetSet(name, st)
	testDelete(name, st)
}

func genKey(i uint64) []byte {
	r := make([]byte, 9)
	r[0] = 'k'
	binary.BigEndian.PutUint64(r[1:], i)

	return r
}

// test batch writes
func testBatchWrite(name string, s store.Store) {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	total := uint64(0)
	start := time.Now()

	for i := 0; i < *c; i++ {
		wg.Add(1)

		go func(proc int) {
			defer wg.Done()

			batchSize := uint64(1000)
			var kvs []store.KV

			for i := uint64(0); i < batchSize; i++ {
				kvs = append(kvs, store.KV{
					Key:   genKey(i),
					Value: make([]byte, *size),
				})
			}

			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Fill random keys and values.
					for i := range kvs {
						kv := kvs[i]

						rand.Read(kv.Key)
						rand.Read(kv.Value)
					}

					err := s.SetBulk(kvs)
					if err != nil {
						panic(err)
					}

					atomic.AddUint64(&total, uint64(len(kvs)))
				}
			}
		}(i)
	}

	wg.Wait()

	fmt.Printf(
		"%s batch write test inserted: %d entries; took: %s s\n",
		name, total, time.Since(start),
	)
}

// test get
func testGet(name string, s store.Store) {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	counts := make([]int, *c)
	start := time.Now()

	for j := 0; j < *c; j++ {
		wg.Add(1)

		index := uint64(j)
		go func() {
			defer wg.Done()

			i := index

			for {
				select {
				case <-ctx.Done():
					return
				default:
					v, _ := s.Get(genKey(i))
					if len(v) == 0 {
						i = index
					}

					i += uint64(*c)
					counts[index]++
				}
			}
		}()
	}

	wg.Wait()

	dur := time.Since(start)
	d := int64(dur)

	var n int
	for _, count := range counts {
		n += count
	}

	fmt.Printf(
		"%s get rate: %d op/s, mean: %d ns, took: %d s\n",
		name, int64(n)*1e6/(d/1e3), d/int64((n)*(*c)), int(dur.Seconds()),
	)
}

// test multiple get/one set
func testGetSet(name string, s store.Store) {
	var wg sync.WaitGroup

	ch := make(chan struct{})
	setCount := 0

	go func() {
		i := uint64(0)

		for {
			select {
			case <-ch:
				return
			default:
				if err := s.Set(genKey(i), data); err != nil {
					panic(err)
				}

				setCount++
				i++
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	counts := make([]int, *c)
	start := time.Now()

	for j := 0; j < *c; j++ {
		wg.Add(1)

		index := uint64(j)
		go func() {
			defer wg.Done()

			i := index

			for {
				select {
				case <-ctx.Done():
					return
				default:
					v, _ := s.Get(genKey(i))
					if len(v) == 0 {
						i = index
					}

					i += uint64(*c)
					counts[index]++
				}
			}
		}()
	}

	wg.Wait()

	close(ch)

	dur := time.Since(start)
	d := int64(dur)

	var n int
	for _, count := range counts {
		n += count
	}

	if setCount == 0 {
		fmt.Printf("%s setmixed rate: -1 op/s, mean: -1 ns, took: %d s\n", name, int(dur.Seconds()))
	} else {
		fmt.Printf(
			"%s setmixed rate: %d op/s, mean: %d ns, took: %d s\n",
			name, int64(setCount)*1e6/(d/1e3), d/int64(setCount), int(dur.Seconds()),
		)
	}

	fmt.Printf(
		"%s getmixed rate: %d op/s, mean: %d ns, took: %d s\n",
		name, int64(n)*1e6/(d/1e3), d/int64((n)*(*c)), int(dur.Seconds()),
	)
}

func testSet(name string, s store.Store) {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	counts := make([]int, *c)
	start := time.Now()

	for j := 0; j < *c; j++ {
		wg.Add(1)

		index := uint64(j)
		go func() {
			defer wg.Done()

			i := index

			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := s.Set(genKey(i), data); err != nil {
						panic(err)
					}

					i += uint64(*c)
					counts[index]++
				}
			}
		}()
	}

	wg.Wait()

	dur := time.Since(start)
	d := int64(dur)

	var n int
	for _, count := range counts {
		n += count
	}

	fmt.Printf(
		"%s set rate: %d op/s, mean: %d ns, took: %d s\n",
		name, int64(n)*1e6/(d/1e3), d/int64((n)*(*c)), int(dur.Seconds()),
	)
}

func testDelete(name string, s store.Store) {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	counts := make([]int, *c)
	start := time.Now()

	for j := 0; j < *c; j++ {
		wg.Add(1)

		index := uint64(j)
		go func() {
			defer wg.Done()

			i := index

			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := s.Del(genKey(i)); err != nil {
						panic(err)
					}

					i += uint64(*c)
					counts[index]++
				}
			}
		}()
	}

	wg.Wait()

	dur := time.Since(start)
	d := int64(dur)

	var n int
	for _, count := range counts {
		n += count
	}

	fmt.Printf(
		"%s del rate: %d op/s, mean: %d ns, took: %d s\n",
		name, int64(n)*1e6/(d/1e3), d/int64((n)*(*c)), int(dur.Seconds()),
	)
}

func getStore(s string, fsync bool, path string) (store.Store, string, error) {
	var st store.Store
	var err error

	switch s {
	default:
		err = fmt.Errorf("unknown store type: %v", s)
	case "badger":
		if path == "" {
			path = "badger.db"
		}
		st, err = badger.New(path, fsync)
	case "buntdb":
		if path == "" {
			path = "buntdb.db"
		}
		st, err = buntdb.New(path, fsync)
	case "leveldb":
		if path == "" {
			path = "leveldb.db"
		}
		st, err = leveldb.New(path, fsync)
	case "nutsdb":
		if path == "" {
			path = "nutsdb.db"
		}
		st, err = nutsdb.New(path, fsync)
	case "pebble":
		if path == "" {
			path = "pebble.db"
		}
		st, err = pebble.New(path, fsync)
	case "pogreb":
		if path == "" {
			path = "pogreb.db"
		}
		st, err = pogreb.New(path, fsync)
	}

	return st, path, err
}
