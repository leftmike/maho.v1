package store

import (
	"fmt"
	"sync"

	"github.com/leftmike/maho/db"
)

type Store interface {
	Open(name string) (db.Database, error)
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

func Open(typ string, name string) (db.Database, error) {
	storesMutex.RLock()
	defer storesMutex.RUnlock()
	if store, ok := stores[typ]; ok {
		return store.Open(name)
	}
	return nil, fmt.Errorf("store: \"%s\" not found", typ)
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
