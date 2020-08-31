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

func (stmt *Copy) Resolve(ses *evaluate.Session) {
	stmt.Table = ses.ResolveTableName(stmt.Table)
}

func (stmt *Copy) Plan(ctx context.Context, pctx evaluate.PlanContext) (evaluate.Plan, error) {
	tt, err := pctx.Transaction().LookupTableType(ctx, stmt.Table)
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
		tn:         stmt.Table,
		cols:       cols,
		from:       copy.NewReader("stdin", stmt.From, stmt.FromLine),
		fromToRow:  fromToRow,
		defaultRow: defaultRow,
		delimiter:  stmt.Delimiter,
	}, nil
}

type copyPlan struct {
	tn         sql.TableName
	cols       []sql.Identifier
	from       *copy.Reader
	fromToRow  []int
	defaultRow []sql.CExpr
	delimiter  rune
}

func (plan *copyPlan) Explain() string {
	// XXX: copyPlan.Explain
	return ""
}

func (plan *copyPlan) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	tbl, _, err := tx.LookupTable(ctx, plan.tn)
	if err != nil {
		return -1, err
	}

	var cnt int64
	err = copy.CopyFromText(plan.from, len(plan.fromToRow), plan.delimiter,
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
			return tbl.Insert(ctx, row)
		})
	if err != nil {
		return -1, err
	}
	return cnt, nil
}
