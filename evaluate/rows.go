package evaluate

import (
	"github.com/leftmike/maho/sql"
)

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(ses Session, dest []sql.Value) error
	Delete(ses Session) error
	Update(ses Session, updates []sql.ColumnUpdate) error
}
