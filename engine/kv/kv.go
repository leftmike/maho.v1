package kv

import (
	"errors"
	"fmt"
	"path/filepath"
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
	Get(key []byte, vf func(val []byte) error) error
	GetValue(key []byte) ([]byte, error)
	Iterate(prefix []byte) Iterator
}

type WriteTx interface {
	ReadTx
	Commit() error
	Delete(key []byte) error
	Set(key []byte, val []byte) error
}

type Iterator interface {
	Close()
	Key() []byte
	KeyCopy() []byte
	Next()
	Rewind()
	Seek(key []byte)
	Valid() bool
	Value(vf func(val []byte) error) error
}

func FixPath(path, ext, who string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("%s: missing filename", who)
	}
	if filepath.Ext(path) == "" {
		return path + ext, nil
	}
	return path, nil
}
