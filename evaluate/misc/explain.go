package misc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Explain struct {
	Stmt    evaluate.Stmt
	Verbose bool
}

type explainable interface {
	Explain() evaluate.ExplainTree
}

func (stmt Explain) String() string {
	return fmt.Sprintf("EXPLAIN %s", stmt.Stmt)
}

var (
	emptyValue = sql.StringValue("")
)

/*
sort
 |
 +-- join
      |
      +-- scan
      |
      |
      +-- scan

*/

func explain(tree evaluate.ExplainTree, rows [][]sql.Value, depth int,
	verbose bool) [][]sql.Value {

	var indent int
	var name string
	if depth == 0 {
		name = tree.Name()
	} else {
		name = strings.Repeat(" ", (depth-1)*5) + " +-- " + tree.Name()
		indent = depth * 5
	}
	row := []sql.Value{sql.StringValue(name), emptyValue, emptyValue}
	if verbose {
		var cols string
		for _, col := range tree.Columns() {
			if cols != "" {
				cols += ", "
			}
			cols += col
		}
		row = append(row, sql.StringValue("("+cols+")"))
	}
	rows = append(rows, row)

	indent += 1
	for _, fd := range tree.Fields() {
		row := []sql.Value{sql.StringValue(strings.Repeat(" ", indent) + "|"),
			sql.StringValue(fd.Field), sql.StringValue(fd.Description)}
		if verbose {
			row = append(row, emptyValue)
		}
		rows = append(rows, row)
	}

	for _, child := range tree.Children() {
		rows = explain(child, rows, depth+1, verbose)
	}
	return rows
}

func (stmt Explain) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	plan, err := stmt.Stmt.Plan(ctx, pctx, tx)
	if err != nil {
		return nil, err
	}
	expl, ok := plan.(explainable)
	if !ok {
		return nil, fmt.Errorf("explain: statement can't be explained: %s", stmt.Stmt)
	}

	var cols []sql.Identifier
	var colTypes []sql.ColumnType
	if stmt.Verbose {
		cols = []sql.Identifier{sql.TREE, sql.FIELD, sql.DESCRIPTION, sql.COLUMNS}
		colTypes = []sql.ColumnType{sql.StringColType, sql.StringColType, sql.StringColType,
			sql.StringColType}
	} else {
		cols = []sql.Identifier{sql.TREE, sql.FIELD, sql.DESCRIPTION}
		colTypes = []sql.ColumnType{sql.StringColType, sql.StringColType, sql.StringColType}
	}
	return &explainRows{
		cols:     cols,
		colTypes: colTypes,
		rows:     explain(expl.Explain(), nil, 0, stmt.Verbose),
	}, nil
}

type explainRows struct {
	cols     []sql.Identifier
	colTypes []sql.ColumnType
	rows     [][]sql.Value
}

func (_ *explainRows) Tag() string {
	return "EXPLAIN"
}

func (er *explainRows) Columns() []sql.Identifier {
	return er.cols
}

func (er *explainRows) ColumnTypes() []sql.ColumnType {
	return er.colTypes
}

func (er *explainRows) NumColumns() int {
	return len(er.cols)
}

func (er *explainRows) Close() error {
	er.rows = nil
	return nil
}

func (er *explainRows) Next(ctx context.Context, dest []sql.Value) error {
	if len(er.rows) == 0 {
		return io.EOF
	}

	copy(dest, er.rows[0])
	er.rows = er.rows[1:]
	return nil
}

func (er *explainRows) Delete(ctx context.Context) error {
	return errors.New("explain: rows may not be deleted")
}

func (er *explainRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return errors.New("explain: rows may not be updated")
}

func (er *explainRows) Rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	return er, nil
}
