package engine

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

type pageIO interface {
	ReadAt(b []byte, off int64) (int, error)
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	WriteAt(b []byte, off int64) (int, error)
}

type PageCache struct {
	pagesMutex sync.Mutex
	pages      map[PageNum]*Page
	ioMutex    sync.Mutex
	io         pageIO
	pageSize   int64
}

func NewPageCache(io pageIO, psz int64) *PageCache {
	return &PageCache{
		pages:    map[PageNum]*Page{},
		io:       io,
		pageSize: psz,
	}
}

func (pc *PageCache) lockPage(pn PageNum, rlock bool) (*Page, error) {
	pc.pagesMutex.Lock()
	pg, ok := pc.pages[pn]
	if ok {
		atomic.AddInt32(&pg.pin, 1)
		pc.pagesMutex.Unlock()

		if rlock {
			pg.mutex.RLock()
		} else {
			pg.mutex.Lock()
		}
		return pg, nil
	}

	pg = &Page{pageNum: pn, cache: pc, pin: 1, Bytes: make([]byte, pc.pageSize)}
	pg.mutex.Lock()
	pc.pages[pn] = pg
	pc.pagesMutex.Unlock()

	err := pc.readPage(pg)
	if err != nil {
		pg.mutex.Unlock()
		return nil, err
	}
	if rlock {
		pg.mutex.Unlock()
		pg.mutex.RLock()
	}
	return pg, nil
}

func (pc *PageCache) LockPage(pn PageNum) (*Page, error) {
	return pc.lockPage(pn, false)
}

func (pc *PageCache) RLockPage(pn PageNum) (*Page, error) {
	return pc.lockPage(pn, true)
}

func (pc *PageCache) readPage(pg *Page) error {
	pc.ioMutex.Lock()
	defer pc.ioMutex.Unlock()

	fi, err := pc.io.Stat()
	if err != nil {
		return err
	}
	if fi.Size() < (int64(pg.pageNum)+1)*pc.pageSize {
		return nil // must be a new page
	}
	br, err := pc.io.ReadAt(pg.Bytes, int64(pg.pageNum)*pc.pageSize)
	if err != nil {
		return err
	} else if int64(br) != pc.pageSize {
		return fmt.Errorf("partial read: got %d, want %d", br, pc.pageSize)
	}
	return nil
}

func (pc *PageCache) writePage(pg *Page) error {
	pc.ioMutex.Lock()
	defer pc.ioMutex.Unlock()

	fi, err := pc.io.Stat()
	if err != nil {
		return err
	}
	sz := (int64(pg.pageNum) + 1) * pc.pageSize
	if fi.Size() < sz {
		err = pc.io.Truncate(sz)
		if err != nil {
			return err
		}
	}
	bw, err := pc.io.WriteAt(pg.Bytes, int64(pg.pageNum)*pc.pageSize)
	if err != nil {
		return err
	} else if int64(bw) != pc.pageSize {
		return fmt.Errorf("partial write: got %d, want %d", bw, pc.pageSize)
	}
	return pc.io.Sync()
}
