package evaluate

import (
	"context"
	"io"

	"github.com/leftmike/maho/sql"
)

// AllRows returns all of the rows from a Rows as slices of values.
func AllRows(ctx context.Context, rows sql.Rows) ([][]sql.Value, error) {
	all := [][]sql.Value{}
	l := rows.NumColumns()
	for {
		dest := make([]sql.Value, l)
		err := rows.Next(ctx, dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		all = append(all, dest)
	}
	err := rows.Close()
	if err != nil {
		return nil, err
	}
	return all, nil
}
