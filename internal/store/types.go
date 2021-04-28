package store

type KV struct {
	Key   []byte
	Value []byte
}

type Store interface {
	Set(key, value []byte) error
	SetString(key string, value []byte) error
	SetBulk(kvs []KV) error
	Get(key []byte) ([]byte, error)
	GetString(key string) ([]byte, error)
	GetBulk(keys [][]byte) ([]KV, error)
	Del(key []byte) error
	DelString(key string) error
	DelBulk(key [][]byte) error
	Keys(pattern []byte, limit int, withvalues bool) ([]KV, error)
	Flush() error
	Close() error
}
