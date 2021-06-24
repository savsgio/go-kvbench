package common

type KV struct {
	Key   []byte
	Value []byte
}

type IterFunc func(key, value []byte) error
