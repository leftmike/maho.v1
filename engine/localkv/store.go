package localkv

type Store interface {
	Begin(writable bool) (Tx, error)
}

type Tx interface {
	Map(mid uint64) (Mapper, error)
	Commit() error
	Rollback() error
}

type Mapper interface {
	Get(key []byte, vf func(val []byte) error) error
	Set(key, val []byte) error
	Walk(prefix []byte) Walker
}

type Walker interface {
	Close()
	Delete() error
	Next() ([]byte, bool)
	Rewind() ([]byte, bool)
	Seek(seek []byte) ([]byte, bool)
	Value(vf func(val []byte) error) error
}
