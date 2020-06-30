package engine

import (
	"bytes"
	"encoding/gob"

	"github.com/leftmike/maho/sql"
)

type TableType struct {
	ver      uint
	cols     []sql.Identifier
	colTypes []sql.ColumnType
	primary  []sql.ColumnKey
}

func MakeTableType(cols []sql.Identifier, colTypes []sql.ColumnType,
	primary []sql.ColumnKey) *TableType {

	return &TableType{
		ver:      1,
		cols:     cols,
		colTypes: colTypes,
		primary:  primary,
	}
}

func (tt *TableType) Columns() []sql.Identifier {
	return tt.cols
}

func (tt *TableType) ColumnTypes() []sql.ColumnType {
	return tt.colTypes
}

func (tt *TableType) PrimaryKey() []sql.ColumnKey {
	return tt.primary
}

func (tt *TableType) Version() uint {
	return tt.ver
}

func (tt *TableType) AddColumns(cols []sql.Identifier, colTypes []sql.ColumnType) *TableType {
	return &TableType{
		ver:      tt.ver + 1,
		cols:     append(tt.cols, cols...),
		colTypes: append(tt.colTypes, colTypes...),
		primary:  tt.primary,
	}
}

func (tt *TableType) SetPrimaryKey(primary []sql.ColumnKey) *TableType {
	if len(tt.primary) > 0 {
		panic("metadata: primary key may not be changed")
	}

	return &TableType{
		ver:      tt.ver + 1,
		cols:     tt.cols,
		colTypes: tt.colTypes,
		primary:  primary,
	}
}

type tableMetadata struct {
	Columns     []sql.Identifier
	ColumnTypes []sql.ColumnType
	Primary     []sql.ColumnKey
}

func (tt *TableType) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(&tableMetadata{
		Columns:     tt.cols,
		ColumnTypes: tt.colTypes,
		Primary:     tt.primary,
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeTableType(buf []byte) (*TableType, error) {
	dec := gob.NewDecoder(bytes.NewBuffer(buf))
	var tm tableMetadata
	err := dec.Decode(&tm)
	if err != nil {
		return nil, err
	}
	return &TableType{
		cols:     tm.Columns,
		colTypes: tm.ColumnTypes,
		primary:  tm.Primary,
	}, nil
}
