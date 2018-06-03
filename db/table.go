package db

import (
	"github.com/leftmike/maho/sql"
)

type Table interface {
	Columns(ses Session) []sql.Identifier
	ColumnTypes(ses Session) []ColumnType
	Rows(ses Session) (Rows, error)
	Insert(ses Session, row []sql.Value) error
}
