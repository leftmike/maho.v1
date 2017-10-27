package db

import (
	"maho/sql"
)

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(dest []sql.Value) error
}
