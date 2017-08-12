package store

import (
	"fmt"
	"sync"

	"maho/row"
	"maho/sql"
)

type Store interface {
	Open(name string) (Database, error)
}

type Database interface {
	Name() sql.Identifier
	Type() sql.Identifier
	CreateTable(name sql.Identifier, cols []row.Column) error
	DropTable(name sql.Identifier) error
	Table(name sql.Identifier) (Table, error)
	Tables() ([]sql.Identifier, [][]row.Column)
}

type ColumnMap map[sql.Identifier]int

type Table interface {
	Name() sql.Identifier
	Columns() []row.Column
	ColumnMap() ColumnMap
	Rows() (Rows, error)
	Insert(row []sql.Value) error
}

type Rows interface {
	Columns() []row.Column
	Close() error
	Next(dest []sql.Value) error
}

var (
	storesMutex sync.RWMutex
	stores      = make(map[string]Store)
)

func Register(typ string, store Store) {
	storesMutex.Lock()
	defer storesMutex.Unlock()
	if store == nil {
		panic("store: register store is nil")
	}
	if _, dup := stores[typ]; dup {
		panic("store: register called twice for store: " + typ)
	}
	stores[typ] = store
}

func Open(typ string, name string) (Database, error) {
	storesMutex.RLock()
	defer storesMutex.RUnlock()
	if store, ok := stores[typ]; ok {
		return store.Open(name)
	}
	return nil, fmt.Errorf("store %s not found", typ)
}

func Stores() []string {
	storesMutex.RLock()
	defer storesMutex.RUnlock()
	var ret []string
	for name := range stores {
		ret = append(ret, name)
	}
	return ret
}
