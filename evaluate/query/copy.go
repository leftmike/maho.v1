package query

import (
	"context"
	"fmt"
	"io"

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

func (stmt *Copy) Plan(ses *evaluate.Session, tx sql.Transaction) (interface{}, error) {
	tbl, tt, err := ses.Engine.LookupTable(ses.Context(), tx, ses.ResolveTableName(stmt.Table))
	if err != nil {
		return nil, err
	}

	cols := tt.Columns()
	colTypes := tt.ColumnTypes()

	defaultRow := make([]sql.CExpr, len(cols))
	cmap := map[sql.Identifier]int{}
	for cdx, cn := range cols {
		cmap[cn] = cdx
		defaultRow[cdx] = colTypes[cdx].Default
	}

	fromToRow := make([]int, len(stmt.Columns))
	for fdx, cn := range stmt.Columns {
		cdx, ok := cmap[cn]
		if !ok {
			return nil, fmt.Errorf("engine: %s: column not found: %s", stmt.Table, cn)
		}
		fromToRow[fdx] = cdx
		defaultRow[cdx] = nil
	}

	allNil := true
	for _, ce := range defaultRow {
		if ce != nil {
			allNil = false
			break
		}
	}
	if allNil {
		defaultRow = nil
	}

	return &copyPlan{
		tbl:        tbl,
		cols:       cols,
		from:       copy.NewReader("stdin", stmt.From, stmt.FromLine),
		fromToRow:  fromToRow,
		defaultRow: defaultRow,
		delimiter:  stmt.Delimiter,
	}, nil
}

type copyPlan struct {
	tbl        sql.Table
	cols       []sql.Identifier
	from       *copy.Reader
	fromToRow  []int
	defaultRow []sql.CExpr
	delimiter  rune
}

func (plan *copyPlan) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	var cnt int64

	err := copy.CopyFromText(plan.from, len(plan.fromToRow), plan.delimiter,
		func(vals []sql.Value) error {
			row := make([]sql.Value, len(plan.cols))
			for cdx, ce := range plan.defaultRow {
				if ce == nil {
					continue
				}

				var err error
				row[cdx], err = ce.Eval(ctx, nil)
				if err != nil {
					return err
				}
			}

			for fdx, val := range vals {
				row[plan.fromToRow[fdx]] = val
			}
			cnt += 1
			return plan.tbl.Insert(ctx, row)
		})
	if err != nil {
		return -1, err
	}
	return cnt, nil
}
