package engine

import (
	"bytes"
	"encoding/gob"

	"github.com/leftmike/maho/sql"
)

type tableType struct {
	ver      uint
	cols     []sql.Identifier
	colTypes []sql.ColumnType
	primary  []sql.ColumnKey
}

func MakeTableType(cols []sql.Identifier, colTypes []sql.ColumnType,
	primary []sql.ColumnKey) sql.TableType {

	return &tableType{
		ver:      1,
		cols:     cols,
		colTypes: colTypes,
		primary:  primary,
	}
}

func (tt *tableType) Columns() []sql.Identifier {
	return tt.cols
}

func (tt *tableType) ColumnTypes() []sql.ColumnType {
	return tt.colTypes
}

func (tt *tableType) PrimaryKey() []sql.ColumnKey {
	return tt.primary
}

func (tt *tableType) Version() uint {
	return tt.ver
}

func (tt *tableType) AddColumns(cols []sql.Identifier, colTypes []sql.ColumnType) sql.TableType {
	return &tableType{
		ver:      tt.ver + 1,
		cols:     append(tt.cols, cols...),
		colTypes: append(tt.colTypes, colTypes...),
		primary:  tt.primary,
	}
}

func (tt *tableType) SetPrimaryKey(primary []sql.ColumnKey) sql.TableType {
	if len(tt.primary) > 0 {
		panic("metadata: primary key may not be changed")
	}

	return &tableType{
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

func (tt *tableType) Encode() ([]byte, error) {
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

func DecodeTableType(buf []byte) (sql.TableType, error) {
	dec := gob.NewDecoder(bytes.NewBuffer(buf))
	var tm tableMetadata
	err := dec.Decode(&tm)
	if err != nil {
		return nil, err
	}
	return &tableType{
		cols:     tm.Columns,
		colTypes: tm.ColumnTypes,
		primary:  tm.Primary,
	}, nil
}
