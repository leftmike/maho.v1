package engine

import (
	"sync"
	"sync/atomic"
)

type Page struct {
	mutex   sync.RWMutex
	pageNum PageNum
	cache   *PageCache
	pin     int32
	dirty   bool
	Bytes   []byte
}

func (pg *Page) RUnlock() {
	pg.mutex.RUnlock()
	atomic.AddInt32(&pg.pin, -1)
}

func (pg *Page) Unlock(dirty bool) error {
	var err error

	if dirty {
		pg.dirty = true
	}
	if pg.dirty {
		err = pg.cache.writePage(pg)
		if err == nil {
			pg.dirty = false
		}
	}
	pg.mutex.Unlock()
	atomic.AddInt32(&pg.pin, -1)
	return err
}
