package storage

//go:generate protoc --go_opt=paths=source_relative --go_out=. layoutmd.proto

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type IndexLayout struct {
	IID     int64
	Key     []sql.ColumnKey
	Columns []int
	Unique  bool
}

type TableLayout struct {
	tt      *engine.TableType
	nextIID int64
	indexes []IndexLayout
}

func makeIndexLayout(iid int64, it sql.IndexType) IndexLayout {
	key := make([]sql.ColumnKey, 0, len(it.Key))
	for _, ck := range it.Key {
		num := ck.Number()
		for idx, col := range it.Columns {
			if num == col {
				key = append(key, sql.MakeColumnKey(idx, ck.Reverse()))
				break
			}
		}
	}

	if len(key) != len(it.Key) {
		panic(fmt.Sprintf("store: failed converting key %v to index key %v", it.Key, key))
	}

	return IndexLayout{
		IID:     iid,
		Key:     key,
		Columns: it.Columns,
		Unique:  it.Unique,
	}
}

func makeTableLayout(tt *engine.TableType) *TableLayout {
	tl := TableLayout{
		tt:      tt,
		nextIID: int64(PrimaryIID) + 1,
	}

	tl.indexes = make([]IndexLayout, 0, len(tt.Indexes()))
	for _, it := range tt.Indexes() {
		tl.indexes = append(tl.indexes, makeIndexLayout(tl.nextIID, it))
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

func (tl *TableLayout) Indexes() []IndexLayout {
	return tl.indexes
}

func (tl *TableLayout) IndexesUpdate(updates []sql.ColumnUpdate) ([]IndexLayout, []bool) {
	// XXX: also need to know if the key was updated as well
	return nil, nil
}

func (il *IndexLayout) RowToIndexRow(row, idxRow []sql.Value) {
	for idx, rdx := range il.Columns {
		idxRow[idx] = row[rdx]
	}
}

func (il *IndexLayout) IndexRowToRow(idxRow, row []sql.Value) {
	for idx, rdx := range il.Columns {
		row[rdx] = idxRow[idx]
	}
}

func encodeIndexKey(key []sql.ColumnKey) []*IndexKey {
	mdk := make([]*IndexKey, 0, len(key))
	for _, k := range key {
		mdk = append(mdk,
			&IndexKey{
				Number:  int32(k.Number()),
				Reverse: k.Reverse(),
			})
	}
	return mdk
}

func (tl *TableLayout) encode() ([]byte, error) {
	md := TableLayoutMetadata{
		NextIID: tl.nextIID,
	}

	md.Indexes = make([]*IndexLayoutMetadata, 0, len(tl.indexes))
	for _, il := range tl.indexes {
		cols := make([]int64, 0, len(il.Columns))
		for _, col := range il.Columns {
			cols = append(cols, int64(col))
		}

		md.Indexes = append(md.Indexes,
			&IndexLayoutMetadata{
				IID:     il.IID,
				Key:     encodeIndexKey(il.Key),
				Columns: cols,
				Unique:  il.Unique,
			})
	}

	return proto.Marshal(&md)
}

func decodeIndexKey(mdk []*IndexKey) []sql.ColumnKey {
	key := make([]sql.ColumnKey, 0, len(mdk))
	for _, k := range mdk {
		key = append(key, sql.MakeColumnKey(int(k.Number), k.Reverse))
	}
	return key
}

func (st *Store) decodeTableLayout(tn sql.TableName, tt *engine.TableType,
	buf []byte) (*TableLayout, error) {

	var md TableLayoutMetadata
	err := proto.Unmarshal(buf, &md)
	if err != nil {
		return nil, fmt.Errorf("%s: table %s: %s", st.name, tn, err)
	}
	if len(md.Indexes) != len(tt.Indexes()) {
		return nil, fmt.Errorf("%s: table %s: corrupt metadata", st.name, tn)
	}

	tl := TableLayout{
		tt:      tt,
		nextIID: md.NextIID,
		indexes: make([]IndexLayout, 0, len(md.Indexes)),
	}

	for _, imd := range md.Indexes {
		cols := make([]int, 0, len(imd.Columns))
		for _, col := range imd.Columns {
			cols = append(cols, int(col))
		}

		tl.indexes = append(tl.indexes,
			IndexLayout{
				IID:     imd.IID,
				Key:     decodeIndexKey(imd.Key),
				Columns: cols,
				Unique:  imd.Unique,
			})
	}

	return &tl, nil
}
