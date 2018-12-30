package kv

import (
	"errors"
	"strings"
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
	Discard()
	Get(key1 string, key2 string, key3 []byte, vf func(val []byte) error) error
	GetValue(key1 string, key2 string, key3 []byte) ([]byte, error)
	Iterate(key1 string, key2 string) (Iterator, error)
}

func Get(tx ReadTx, path string, key []byte) ([]byte, error) {
	keys := strings.SplitN(path, "/", 2)
	return tx.GetValue(keys[0], keys[1], key)
}

func Set(tx WriteTx, path string, key []byte, val []byte) error {
	keys := strings.SplitN(path, "/", 2)
	return tx.Set(keys[0], keys[1], key, val)
}

type WriteTx interface {
	ReadTx
	Commit() error
	Delete(key1 string, key2 string, key3 []byte) error
	DeleteAll(key1 string, key2 string) error
	Set(key1 string, key2 string, key3 []byte, val []byte) error
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
