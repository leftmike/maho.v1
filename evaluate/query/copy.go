package query

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/copy"
	"github.com/leftmike/maho/sql"
)

type Copy struct {
	Table     sql.TableName
	Columns   []sql.Identifier
	From      io.RuneReader
	FromLine  int
	Delimiter rune
}

func (stmt *Copy) String() string {
	s := fmt.Sprintf("COPY %s (", stmt.Table)
	for i, col := range stmt.Columns {
		if i > 0 {
			s += ", "
		}
		s += col.String()
	}
	s += ") "

	s += "FROM STDIN"

	if stmt.Delimiter != '\t' {
		s += fmt.Sprintf(" DELIMITER '%c'", stmt.Delimiter)
	}
	return s
}

func (stmt *Copy) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	tbl, err := ses.Engine.LookupTable(ses.Context(), tx, ses.ResolveTableName(stmt.Table))
	if err != nil {
		return nil, err
	}

	cols := tbl.Columns(ses.Context())

	if len(cols) != len(stmt.Columns) {
		return nil, fmt.Errorf("engine: %s: all table columns must be specified for COPY",
			stmt.Table)
	}

	cmap := map[sql.Identifier]int{}
	for cdx, cn := range cols {
		cmap[cn] = cdx
	}

	fromToRow := make([]int, len(stmt.Columns))
	for fdx, cn := range stmt.Columns {
		cdx, ok := cmap[cn]
		if !ok {
			return nil, fmt.Errorf("engine: %s: column not found: %s", stmt.Table, cn)
		}
		fromToRow[fdx] = cdx
	}

	return &copyPlan{
		table:     stmt.Table,
		tbl:       tbl,
		cols:      cols,
		colTypes:  tbl.ColumnTypes(ses.Context()),
		from:      copy.NewReader("stdin", stmt.From, stmt.FromLine),
		fromToRow: fromToRow,
		delimiter: stmt.Delimiter,
	}, nil
}

type copyPlan struct {
	table     sql.TableName
	tbl       engine.Table
	cols      []sql.Identifier
	colTypes  []sql.ColumnType
	from      *copy.Reader
	fromToRow []int
	delimiter rune
}

func (plan *copyPlan) Execute(ctx context.Context, eng engine.Engine,
	tx engine.Transaction) (int64, error) {

	var cnt int64

	err := copy.CopyFromText(plan.from, len(plan.fromToRow), plan.delimiter,
		func(vals []sql.Value) error {
			fmt.Printf("%d\n", cnt)
			row := make([]sql.Value, len(plan.cols))

			for fdx, val := range vals {
				cdx := plan.fromToRow[fdx]
				ct := plan.colTypes[cdx]

				var err error
				row[cdx], err = ct.ConvertValue(plan.cols[cdx], val)
				if err != nil {
					return fmt.Errorf("engine: %s: %s", plan.table, err)
				}
			}

			cnt += 1
			return plan.tbl.Insert(ctx, row)
		})
	if err != nil {
		return -1, err
	}
	return cnt, nil
}