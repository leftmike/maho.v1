package memkv

import (
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/localkv"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

type memKVEngine struct {
	kvrows.KVRows
}

func NewEngine(dataDir string) (engine.Engine, error) {
	me := &memKVEngine{}
	err := me.KVRows.Startup(localkv.NewStore(OpenStore()))
	if err != nil {
		return nil, err
	}
	ve := virtual.NewEngine(me)
	return ve, nil
}

func (_ *memKVEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("badger: use virtual engine with memkv engine")
}

func (_ *memKVEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("badger: use virtual engine with memkv engine")
}

func (_ *memKVEngine) IsTransactional() bool {
	return true
}
