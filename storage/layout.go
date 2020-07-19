package storage

//go:generate protoc --go_opt=paths=source_relative --go_out=. layoutmd.proto

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type IndexLayout struct {
	IID int64
	// Map from index row columns to primary row columns: indexRow[i] = primaryRow[Columns[i]]
	Columns []int
	// Key for this index in index row columns, NOT primary row columns
	Key    []sql.ColumnKey
	Unique bool
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
			if ck.Number() == update.Column {
				return true
			}
		}
	}

	return false
}

func (tl *TableLayout) Indexes() []IndexLayout {
	return tl.indexes
}

func (tl *TableLayout) IndexName(idx int) sql.Identifier {
	return tl.tt.Indexes()[idx].Name
}

func columnUpdated(cols []int, updates []sql.ColumnUpdate) bool {
	for _, col := range cols {
		for _, upd := range updates {
			if col == upd.Column {
				return true
			}
		}
	}

	return false
}

func (il IndexLayout) keyUpdated(updates []sql.ColumnUpdate) bool {
	for _, update := range updates {
		for _, ck := range il.Key {
			if il.Columns[ck.Number()] == update.Column {
				return true
			}
		}
	}

	return false
}

func (tl *TableLayout) IndexesUpdated(updates []sql.ColumnUpdate) ([]IndexLayout, []bool) {
	var indexes []IndexLayout
	var indexUpdated []bool

	for _, il := range tl.indexes {
		if columnUpdated(il.Columns, updates) {
			indexes = append(indexes, il)
			indexUpdated = append(indexUpdated, il.keyUpdated(updates))
		}
	}

	return indexes, indexUpdated
}

func (il IndexLayout) RowToIndexRow(row []sql.Value) []sql.Value {
	idxRow := make([]sql.Value, len(il.Columns))
	for idx, rdx := range il.Columns {
		idxRow[idx] = row[rdx]
	}

	return idxRow
}

func (il IndexLayout) IndexRowToRow(idxRow, row []sql.Value) {
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
