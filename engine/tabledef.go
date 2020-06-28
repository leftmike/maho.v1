package engine

import (
	"errors"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

type tableDef struct {
	cols     []sql.Identifier
	colTypes []sql.ColumnType
	primary  []sql.ColumnKey
}

func (td *tableDef) Columns() []sql.Identifier {
	return td.cols
}

func (td *tableDef) ColumnTypes() []sql.ColumnType {
	return td.colTypes
}

func (td *tableDef) PrimaryKey() []sql.ColumnKey {
	return td.primary
}

func (e *Engine) EncodeTableDef(td storage.TableDef) ([]byte, error) {
	// XXX
	return nil, errors.New("not implemented")
}

func (e *Engine) DecodeTableDef(buf []byte) (storage.TableDef, error) {
	// XXX
	return nil, errors.New("not implemented")
}

func (e *Engine) MakeTableDef(cols []sql.Identifier, colTypes []sql.ColumnType,
	primary []sql.ColumnKey) (storage.TableDef, error) {

	return &tableDef{
		cols:     cols,
		colTypes: colTypes,
		primary:  primary,
	}, nil
}
