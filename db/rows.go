package db

import (
	"github.com/leftmike/maho/sql"
)

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(dest []sql.Value) error
}
