package storage

//go:generate protoc --go_opt=paths=source_relative --go_out=. layoutmd.proto

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type TableLayout struct {
	tt      *engine.TableType
	nextIID int64
	IIDs    []int64
}

func makeTableLayout(tt *engine.TableType) *TableLayout {
	tl := TableLayout{
		tt:      tt,
		nextIID: int64(PrimaryIID) + 1,
	}

	tl.IIDs = make([]int64, 0, len(tt.Indexes()))
	for _ = range tt.Indexes() {
		tl.IIDs = append(tl.IIDs, tl.nextIID)
		tl.nextIID += 1
	}
	return &tl
}

func (tl *TableLayout) Columns() []sql.Identifier {
	return tl.tt.Columns()
}

func (tl *TableLayout) PrimaryKey() []sql.ColumnKey {
	return tl.tt.PrimaryKey()
}

func (tl *TableLayout) PrimaryUpdated(updates []sql.ColumnUpdate) bool {
	primary := tl.tt.PrimaryKey()
	for _, update := range updates {
		for _, ck := range primary {
			if ck.Number() == update.Index {
				return true
			}
		}
	}

	return false
}

func (tl *TableLayout) encode() ([]byte, error) {
	return proto.Marshal(&TableLayoutMetadata{
		NextIID: tl.nextIID,
		IIDs:    tl.IIDs,
	})
}

func (st *Store) decodeTableLayout(tn sql.TableName, tt *engine.TableType,
	buf []byte) (*TableLayout, error) {

	var md TableLayoutMetadata
	err := proto.Unmarshal(buf, &md)
	if err != nil {
		return nil, fmt.Errorf("%s: table %s: %s", st.name, tn, err)
	}
	if len(md.IIDs) != len(tt.Indexes()) {
		return nil, fmt.Errorf("%s: table %s: corrupt metadata", st.name, tn)
	}

	return &TableLayout{
		tt:      tt,
		nextIID: md.NextIID,
		IIDs:    md.IIDs,
	}, nil
}
