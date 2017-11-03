package test

import (
	"bytes"
	"fmt"
	"io"
	"maho/db"
	"maho/engine"
	"maho/sql"
	"maho/store"
	_ "maho/store/test"
	"sort"
)

// StartEngine creates a test database and starts an engine; it is intended for use by testing.
func StartEngine(tbl string) (*engine.Engine, db.Database, error) {
	dbase, err := store.Open("test", tbl)
	if err != nil {
		return nil, nil, err
	}
	e, err := engine.Start(dbase)
	if err != nil {
		return nil, nil, err
	}

	return e, dbase, nil
}

// AllRows returns all of the rows from a db.Rows as slices of values.
func AllRows(rows db.Rows) ([][]sql.Value, error) {
	all := [][]sql.Value{}
	l := len(rows.Columns())
	for {
		dest := make([]sql.Value, l)
		err := rows.Next(dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		all = append(all, dest)
	}
	return all, nil
}

// RowsToStmt converts rows into a SQL statement which when evaluated results in an identical set
// of rows.
//
// For rows with at least one row of values the following statement will be returned.
// SELECT * FROM (VALUES ...) AS rows (c1, ...)
//
// For rows with no values the following statement will be returned.
// SELECT * FROM (VALUES NULL ...) AS rows (c1, ...) WHERE FALSE
func RowsToStmt(rows db.Rows) (string, error) {
	all, err := AllRows(rows)
	if err != nil {
		return "", err
	}
	cols := rows.Columns()

	var b bytes.Buffer
	fmt.Fprint(&b, "SELECT * FROM (VALUES ")

	if len(all) == 0 {
		fmt.Fprint(&b, "(")
		for i := range cols {
			if i > 0 {
				fmt.Fprint(&b, ", ")
			}
			fmt.Fprint(&b, "NULL")
		}
		fmt.Fprint(&b, ")")
	} else {
		for i, row := range all {
			if i > 0 {
				fmt.Fprint(&b, ", (")
			} else {
				fmt.Fprint(&b, "(")
			}

			for i, val := range row {
				if i > 0 {
					fmt.Fprint(&b, ", ")
				}
				fmt.Fprint(&b, sql.Format(val))
			}

			fmt.Fprint(&b, ")")
		}
	}

	fmt.Fprint(&b, ") AS rows (")
	for i, c := range cols {
		if i > 0 {
			fmt.Fprint(&b, ", ")
		}
		fmt.Fprint(&b, c.String())
	}
	fmt.Fprint(&b, ")")

	if len(all) == 0 {
		fmt.Fprint(&b, " WHERE false")
	}

	return b.String(), nil
}

type allRows [][]sql.Value

func (ar allRows) Len() int {
	return len(ar)
}

func (ar allRows) Swap(i, j int) {
	ar[i], ar[j] = ar[j], ar[i]
}

func (ar allRows) Less(i, j int) bool {
	return sql.Less(ar[i][0], ar[j][0])
}

// RowsEqual checks to see if the two sets of rows are equal including columns (number, name, and
// order), and the same rows of values ignoring order. The rows of values are sorted before they
// are compared.
func RowsEqual(rows1, rows2 db.Rows) bool {
	if !DeepEqual(rows1.Columns(), rows2.Columns()) {
		return false
	}
	all1, err := AllRows(rows1)
	if err != nil {
		return false
	}
	all2, err := AllRows(rows2)
	if err != nil {
		return false
	}
	sort.Sort(allRows(all1))
	sort.Sort(allRows(all2))
	return DeepEqual(all1, all2)
}

// RowsIdentical checks to see if the two sets of rows are identical including columns (number,
// name, and order), and the same rows of values in the same order. The rows of values are not
// sorted before they are compared.
func RowsIdentical(rows1, rows2 db.Rows) bool {
	if !DeepEqual(rows1.Columns(), rows2.Columns()) {
		return false
	}
	all1, err := AllRows(rows1)
	if err != nil {
		return false
	}
	all2, err := AllRows(rows2)
	if err != nil {
		return false
	}
	return DeepEqual(all1, all2)
}

type rows struct {
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
}

// MakeRows creates rows based on a slice of columns and slices of values; the result can be used
// as db.Rows.
func MakeRows(cols []sql.Identifier, vals [][]sql.Value) db.Rows {
	return &rows{columns: cols, rows: vals}
}

func (r *rows) Columns() []sql.Identifier {
	return r.columns
}

func (r *rows) Close() error {
	r.index = len(r.rows)
	return nil
}

func (r *rows) Next(dest []sql.Value) error {
	if r.index == len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index += 1
	return nil
}
