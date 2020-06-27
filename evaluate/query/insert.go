package query

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type InsertValues struct {
	Table   sql.TableName
	Columns []sql.Identifier
	Rows    [][]sql.Expr
}

func (stmt *InsertValues) String() string {
	s := fmt.Sprintf("INSERT INTO %s ", stmt.Table)
	if stmt.Columns != nil {
		s += "("
		for i, col := range stmt.Columns {
			if i > 0 {
				s += ", "
			}
			s += col.String()
		}
		s += ") "
	}

	s += "VALUES"

	for i, r := range stmt.Rows {
		if i > 0 {
			s += ", ("
		} else {
			s += " ("
		}

		for j, v := range r {
			if j > 0 {
				s += ", "
			}
			if v == nil {
				s += "NULL"
			} else {
				s += v.String()
			}
		}

		s += ")"
	}

	return s
}

func (stmt *InsertValues) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	tbl, err := ses.Engine.LookupTable(ses.Context(), tx, ses.ResolveTableName(stmt.Table))
	if err != nil {
		return nil, err
	}

	cols := tbl.Columns(ses.Context())
	colTypes := tbl.ColumnTypes(ses.Context())
	mv := len(cols)
	c2v := make([]int, mv) // column number to value number
	if stmt.Columns == nil {
		for c := range c2v {
			c2v[c] = c
		}
	} else {
		for c := range c2v {
			c2v[c] = len(c2v)
		}

		var cmap = make(map[sql.Identifier]int)
		for i, cn := range cols {
			cmap[cn] = i
		}

		mv = len(stmt.Columns)
		for v, nam := range stmt.Columns {
			c, ok := cmap[nam]
			if !ok {
				return nil, fmt.Errorf("engine: %s: column not found: %s", stmt.Table, nam)
			}
			c2v[c] = v
		}
	}

	var rows [][]expr.CExpr
	for _, r := range stmt.Rows {
		if len(r) > mv {
			return nil, fmt.Errorf("engine: %s: too many values", stmt.Table)
		}
		row := make([]expr.CExpr, len(cols))
		for i, c := range colTypes {
			e := c.Default
			if c2v[i] < len(r) {
				e = r[c2v[i]]
				if e == nil {
					e = c.Default
				}
			}
			var ce expr.CExpr
			if e != nil {
				ce, err = expr.Compile(ses, tx, nil, e, false)
				if err != nil {
					return nil, err
				}
			}
			row[i] = ce
		}

		rows = append(rows, row)
	}

	return &insertValuesPlan{stmt.Table, tbl, cols, colTypes, rows}, nil
}

type insertValuesPlan struct {
	table    sql.TableName
	tbl      engine.Table
	cols     []sql.Identifier
	colTypes []sql.ColumnType
	rows     [][]expr.CExpr
}

func (plan *insertValuesPlan) Execute(ctx context.Context, e *engine.Engine,
	tx engine.Transaction) (int64, error) {

	var err error

	for _, r := range plan.rows {
		row := make([]sql.Value, len(plan.cols))

		for i, c := range plan.colTypes {
			var v sql.Value

			ce := r[i]
			if ce != nil {
				v, err = ce.Eval(ctx, nil)
				if err != nil {
					return -1, err
				}
			}

			row[i], err = c.ConvertValue(plan.cols[i], v)
			if err != nil {
				return -1, fmt.Errorf("engine: table %s: %s", plan.table, err)
			}
		}

		err := plan.tbl.Insert(ctx, row)
		if err != nil {
			return -1, err
		}
	}

	return int64(len(plan.rows)), nil
}
