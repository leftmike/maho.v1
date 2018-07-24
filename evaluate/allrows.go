package evaluate

import (
	"io"

	"github.com/leftmike/maho/sql"
)

// AllRows returns all of the rows from a Rows as slices of values.
func AllRows(ses Session, rows Rows) ([][]sql.Value, error) {
	all := [][]sql.Value{}
	l := len(rows.Columns())
	for {
		dest := make([]sql.Value, l)
		err := rows.Next(ses, dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		all = append(all, dest)
	}
	return all, nil
}
