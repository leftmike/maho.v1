package kv

import (
	"errors"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

type Engine interface {
	Open(path string) (DB, error)
}

type DB interface {
	ReadTx() (ReadTx, error)
	WriteTx() (WriteTx, error)
	Close() error
}

type ReadTx interface {
	Commit() error
	Get(key1 []byte, key2 []byte, key3 []byte, vf func(val []byte) error) error
	Iterate(key1 []byte, key2 []byte) (Iterator, error)
}

type WriteTx interface {
	ReadTx
	Delete(key1 []byte, key2 []byte, key3 []byte) error
	DeleteAll(key1 []byte, key2 []byte) error
	Set(key1 []byte, key2 []byte, key3 []byte, val []byte) error
	Rollback() error
}

type Iterator interface {
	Close()
	Key() []byte
	Next()
	Rewind()
	Seek(key []byte)
	Valid() bool
	Value(vf func(val []byte) error) error
}
