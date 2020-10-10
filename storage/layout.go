package storage

//go:generate protoc --go_opt=paths=source_relative --go_out=. layoutmd.proto

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/encode"
)

type IndexLayout struct {
	IID int64
	// Map from index row columns to primary row columns: indexRow[i] = primaryRow[Columns[i]]
	Columns []int
	// Key for this index in index row columns, NOT primary row columns
	Key []sql.ColumnKey
	// Optional key to append if any of the index key columns are NULL
	NullKey []sql.ColumnKey
}

type TableLayout struct {
	tt      *engine.TableType
	nextIID int64
	indexes []IndexLayout
}

func maybeNullColumns(key []sql.ColumnKey, colTypes []sql.ColumnType) bool {
	for _, ck := range key {
		if !colTypes[ck.Column()].NotNull {
			return true
		}
	}

	return false
}

func asIndexKey(key []sql.ColumnKey, cols []int) []sql.ColumnKey {
	idxKey := make([]sql.ColumnKey, 0, len(key))
	for _, ck := range key {
		keyCol := ck.Column()
		for cdx, col := range cols {
			if keyCol == col {
				idxKey = append(idxKey, sql.MakeColumnKey(cdx, ck.Reverse()))
				break
			}
		}
	}

	if len(key) != len(idxKey) {
		panic(fmt.Sprintf("store: failed converting key %v to index key %v", key, idxKey))
	}
	return idxKey
}

func (tl *TableLayout) addIndexLayout(it sql.IndexType) {
	var nullKey []sql.ColumnKey
	if it.Unique && maybeNullColumns(it.Key, tl.tt.ColumnTypes()) {
		for _, ck := range tl.tt.PrimaryKey() {
			if !sql.ColumnInKey(it.Key, ck) {
				nullKey = append(nullKey, ck)
			}
		}
		if nullKey != nil {
			nullKey = asIndexKey(nullKey, it.Columns)
		}
	}

	tl.indexes = append(tl.indexes,
		IndexLayout{
			IID:     tl.nextIID,
			Columns: it.Columns,
			Key:     asIndexKey(it.Key, it.Columns),
			NullKey: nullKey,
		})
	tl.nextIID += 1
}

func makeTableLayout(tt *engine.TableType) *TableLayout {
	tl := TableLayout{
		tt:      tt,
		nextIID: int64(PrimaryIID) + 1,
	}

	tl.indexes = make([]IndexLayout, 0, len(tt.Indexes()))
	for _, it := range tt.Indexes() {
		tl.addIndexLayout(it)
	}
	return &tl
}

func (tl *TableLayout) NumColumns() int {
	return len(tl.tt.Columns())
}

func (tl *TableLayout) PrimaryKey() []sql.ColumnKey {
	return tl.tt.PrimaryKey()
}

func (tl *TableLayout) PrimaryUpdated(updatedCols []int) bool {
	primary := tl.tt.PrimaryKey()
	for _, col := range updatedCols {
		for _, ck := range primary {
			if ck.Column() == col {
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

func columnUpdated(cols []int, updatedCols []int) bool {
	for _, col := range cols {
		for _, upd := range updatedCols {
			if col == upd {
				return true
			}
		}
	}

	return false
}

func (il IndexLayout) keyUpdated(updatedCols []int) bool {
	for _, upd := range updatedCols {
		for _, ck := range il.Key {
			if il.Columns[ck.Column()] == upd {
				return true
			}
		}
	}

	return false
}

func (tl *TableLayout) IndexesUpdated(updatedCols []int) ([]IndexLayout, []bool) {
	var indexes []IndexLayout
	var indexUpdated []bool

	for _, il := range tl.indexes {
		if columnUpdated(il.Columns, updatedCols) {
			indexes = append(indexes, il)
			indexUpdated = append(indexUpdated, il.keyUpdated(updatedCols))
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

func hasNullKeyColumn(key []sql.ColumnKey, row []sql.Value) bool {
	for _, ck := range key {
		if row[ck.Column()] == nil {
			return true
		}
	}

	return false
}

func (il IndexLayout) MakeKey(key []byte, row []sql.Value) []byte {
	if row != nil && il.NullKey != nil && hasNullKeyColumn(il.Key, row) {
		return append(key, encode.MakeKey(il.NullKey, row)...)
	}
	return key
}

func encodeIndexKey(key []sql.ColumnKey) []*IndexKey {
	mdk := make([]*IndexKey, 0, len(key))
	for _, k := range key {
		mdk = append(mdk,
			&IndexKey{
				Number:  int32(k.Column()),
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
				Columns: cols,
				Key:     encodeIndexKey(il.Key),
				NullKey: encodeIndexKey(il.NullKey),
			})
	}

	return proto.Marshal(&md)
}

func decodeIndexKey(mdk []*IndexKey) []sql.ColumnKey {
	if len(mdk) == 0 {
		return nil
	}

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
				Columns: cols,
				Key:     decodeIndexKey(imd.Key),
				NullKey: decodeIndexKey(imd.NullKey),
			})
	}

	return &tl, nil
}
