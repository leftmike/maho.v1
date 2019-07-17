package evaluate

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/sql"
)

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

func (v *Values) Next(ctx context.Context, dest []sql.Value) error {
	if v.index == len(v.Rows) {
		return io.EOF
	}
	copy(dest, v.Rows[v.index])
	v.index += 1
	return nil
}

func (_ *Values) Delete(ctx context.Context) error {
	return fmt.Errorf("values: rows may not be deleted")
}

func (_ *Values) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("values: rows may not be updated")
}
