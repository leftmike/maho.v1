package mvcc

import (
	"fmt"
	"os"

	"github.com/leftmike/maho/engine"
)

const (
	pageSize = 1024 * 16
)

type database struct {
	pageCache  *engine.PageCache
	dir        engine.PageNum
	startCount uint32
	/*
		nextID engine.TableID
		tables map[sql.Identifier]*basicTable
	*/
}

func (me *mvcc) createDatabase(fullname string) (*database, error) {
	f, err := os.OpenFile(fullname, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}

	pc := engine.NewPageCache(f, pageSize)

	spg, err := pc.LockPage(0)
	if err != nil {
		return nil, err
	}
	sp := SummaryPage(spg.Bytes)
	sp.SetSignature(Signature)
	sp.SetFileVersion(FileVersion)
	sp.SetPageSize(pageSize)
	sp.SetDirectory(2)
	sp.SetStartCount(1)
	err = spg.Unlock(true)
	if err != nil {
		return nil, err
	}

	ipg, err := pc.LockPage(1)
	if err != nil {
		return nil, err
	}
	ip := InventoryPage(ipg.Bytes)
	ip.SetPageType(InventoryPageType)
	// XXX: set that pg 1 and pg 2 are allocated
	err = ipg.Unlock(true)
	if err != nil {
		return nil, err
	}

	dpg, err := pc.LockPage(2)
	if err != nil {
		return nil, err
	}
	dp := DirectoryPage(dpg.Bytes)
	dp.SetPageType(DirectoryPageType)
	err = dpg.Unlock(true)
	if err != nil {
		return nil, err
	}

	return &database{
		pageCache: pc,
	}, nil
}

func (me *mvcc) loadDatabase(fullname string) (*database, error) {
	f, err := os.OpenFile(fullname, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	sb := make([]byte, 512)
	br, err := f.Read(sb)
	if err != nil {
		return nil, err
	} else if br != 512 {
		return nil, fmt.Errorf("mvcc: database %s: too small", fullname)
	}
	if SummaryPage(sb).Signature() != Signature {
		return nil, fmt.Errorf("mvcc: database %s: bad signature: %v", fullname,
			SummaryPage(sb).Signature())
	}
	if SummaryPage(sb).FileVersion() != FileVersion {
		return nil, fmt.Errorf("mvcc: database %s: bad file version: %d", fullname,
			SummaryPage(sb).FileVersion())
	}
	psz := SummaryPage(sb).PageSize()
	if psz < 1024 || psz%512 != 0 {
		return nil, fmt.Errorf("mvcc: database: %s: bad page size: %d", fullname, psz)
	}

	pc := engine.NewPageCache(f, int64(psz))

	spg, err := pc.LockPage(0)
	if err != nil {
		return nil, err
	}
	sp := SummaryPage(spg.Bytes)
	dir := sp.Directory()
	sc := sp.StartCount()
	sc += 1
	sp.SetStartCount(sc)
	err = spg.Unlock(true)
	if err != nil {
		return nil, err
	}

	return &database{
		pageCache:  pc,
		dir:        engine.PageNum(dir),
		startCount: sc,
	}, nil
}
