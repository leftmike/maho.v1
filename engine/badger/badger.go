package badger

import (
	"fmt"
	"path/filepath"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/localkv"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

type badgerEngine struct {
	kvrows.KVRows
}

func NewEngine(dataDir string) (engine.Engine, error) {
	path := filepath.Join(dataDir, "mahobadger")
	st, err := OpenStore(path)
	if err != nil {
		return nil, fmt.Errorf("badger: creating engine at %s failed: %s", path, err)
	}
	be := &badgerEngine{}
	err = be.KVRows.Startup(localkv.NewStore(st))
	if err != nil {
		return nil, err
	}
	ve := virtual.NewEngine(be)
	return ve, nil
}

func (_ *badgerEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("badger: use virtual engine with badger engine")
}

func (_ *badgerEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("badger: use virtual engine with badger engine")
}
