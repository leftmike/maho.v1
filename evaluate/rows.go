package evaluate

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/sql"
)

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(ses *Session, dest []sql.Value) error
	Delete(ses *Session) error
	Update(ses *Session, updates []sql.ColumnUpdate) error
}

type Values struct {
	Cols  []sql.Identifier
	Rows  [][]sql.Value
	index int
}

func (v *Values) Columns() []sql.Identifier {
	return v.Cols
}

func (v *Values) Close() error {
	v.index = len(v.Rows)
	return nil
}

func (v *Values) Next(ses *Session, dest []sql.Value) error {
	if v.index == len(v.Rows) {
		return io.EOF
	}
	copy(dest, v.Rows[v.index])
	v.index += 1
	return nil
}

func (_ *Values) Delete(ses *Session) error {
	return fmt.Errorf("values: rows may not be deleted")
}

func (_ *Values) Update(ses *Session, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("values: rows may not be updated")
}
