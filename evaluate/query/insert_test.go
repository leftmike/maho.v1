package query_test

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/evaluate/query"
	"github.com/leftmike/maho/evaluate/test"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

type insertCase struct {
	stmt string
	fail bool
	rows [][]sql.Value
}

var (
	insertColumns1     = []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")}
	insertColumnTypes1 = []sql.ColumnType{
		{Type: sql.BooleanType, Size: 1},
		{Type: sql.StringType, Size: 128},
		{Type: sql.FloatType, Size: 8},
		{Type: sql.IntegerType, Size: 4},
	}
	insertCases1 = []insertCase{
		{
			stmt: "insert into t values (DEFAULT)",
			rows: [][]sql.Value{{nil, nil, nil, nil}},
		},
		{
			stmt: "insert into t values (NULL, NULL, NULL, NULL)",
			rows: [][]sql.Value{{nil, nil, nil, nil}},
		},
		{
			stmt: "insert into t values (true, 'abcd', 123.456, 789)",
			rows: [][]sql.Value{{sql.BoolValue(true), sql.StringValue("abcd"),
				sql.Float64Value(123.456), sql.Int64Value(789)}},
		},
		{
			stmt: "insert into t (c4, c1) values (123, false), (456)",
			rows: [][]sql.Value{
				{sql.BoolValue(false), nil, nil, sql.Int64Value(123)},
				{nil, nil, nil, sql.Int64Value(456)},
			},
		},
		{
			stmt: "insert into t (c3, c2, c1, c4) values (987.654, 'efghi', false, 321)",
			rows: [][]sql.Value{{sql.BoolValue(false), sql.StringValue("efghi"),
				sql.Float64Value(987.654), sql.Int64Value(321)}},
		},
		{
			stmt: "insert into t (c1, c4) values (true, 123, 123)",
			fail: true,
		},
		{
			stmt: "insert into t values (true, 'abcd', 123.456, 789, false)",
			fail: true,
		},
		{
			stmt: "insert into t (c1, c2, c3, c4, c5) values (true)",
			fail: true,
		},
		{
			stmt: "insert into t (c4, c3, c2, c4) values (123)",
			fail: true,
		},
		{
			stmt: "insert into t (c1) values ('abcd')",
			fail: true,
		},
		{
			stmt: "insert into t (c1) values (123)",
			fail: true,
		},
		{
			stmt: "insert into t (c1) values (45.67)",
			fail: true,
		},
		{
			stmt: "insert into t (c2) values (true)",
			fail: true,
		},
		{
			stmt: "insert into t (c2) values (123)",
			rows: [][]sql.Value{{nil, sql.StringValue("123"), nil, nil}},
		},
		{
			stmt: "insert into t (c2) values (123.456)",
			rows: [][]sql.Value{{nil, sql.StringValue("123.456"), nil, nil}},
		},
		{
			stmt: "insert into t (c3) values (true)",
			fail: true,
		},
		{
			stmt: "insert into t (c3) values ('   123   ')",
			rows: [][]sql.Value{{nil, nil, sql.Float64Value(123), nil}},
		},
		{
			stmt: "insert into t (c3) values ('123.456')",
			rows: [][]sql.Value{{nil, nil, sql.Float64Value(123.456), nil}},
		},
		{
			stmt: "insert into t (c3) values ('123.456b')",
			fail: true,
		},
		{
			stmt: "insert into t (c4) values (true)",
			fail: true,
		},
		{
			stmt: "insert into t (c4) values ('   123   ')",
			rows: [][]sql.Value{{nil, nil, nil, sql.Int64Value(123)}},
		},
		{
			stmt: "insert into t (c4) values (123.456)",
			rows: [][]sql.Value{{nil, nil, nil, sql.Int64Value(123)}},
		},
		{
			stmt: "insert into t (c4) values ('123b')",
			fail: true,
		},
	}

	insertColumns2 = []sql.Identifier{sql.ID("b1"), sql.ID("b2"), sql.ID("b3"), sql.ID("b4"),
		sql.ID("b5"), sql.ID("b6")}
	insertColumnTypes2 = []sql.ColumnType{
		{Type: sql.BooleanType, Size: 1},
		{Type: sql.BooleanType, Size: 1},
		{Type: sql.BooleanType, Size: 1},
		{Type: sql.BooleanType, Size: 1},
		{Type: sql.BooleanType, Size: 1},
		{Type: sql.BooleanType, Size: 1},
	}
	insertCases2 = []insertCase{
		{
			stmt: "insert into t2 values ('t', 'true', 'y', 'yes', 'on', '1')",
			rows: [][]sql.Value{{sql.BoolValue(true), sql.BoolValue(true), sql.BoolValue(true),
				sql.BoolValue(true), sql.BoolValue(true), sql.BoolValue(true)}},
		},
		{
			stmt: "insert into t2 values ('f', 'false', 'n', 'no', 'off', '0')",
			rows: [][]sql.Value{{sql.BoolValue(false), sql.BoolValue(false), sql.BoolValue(false),
				sql.BoolValue(false), sql.BoolValue(false), sql.BoolValue(false)}},
		},
	}

	insertColumns3     = []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")}
	insertColumnTypes3 = []sql.ColumnType{
		{Type: sql.IntegerType, Size: 4, Default: expr.Int64Literal(1)},
		{Type: sql.IntegerType, Size: 4, NotNull: true},
		{Type: sql.IntegerType, Size: 4, Default: expr.Int64Literal(3),
			NotNull: true},
	}
	insertCases3 = []insertCase{
		{
			stmt: "insert into t3 values (DEFAULT)",
			fail: true,
		},
		{
			stmt: "insert into t3 (c2) values (2)",
			rows: [][]sql.Value{{sql.Int64Value(1), sql.Int64Value(2), sql.Int64Value(3)}},
		},
		{
			stmt: "insert into t3 (c1, c2) values (NULL, 2)",
			rows: [][]sql.Value{{nil, sql.Int64Value(2), sql.Int64Value(3)}},
		},
		{
			stmt: "insert into t3 (c1, c2, c3) values (1, 2, NULL)",
			fail: true,
		},
	}
)

func TestInsert(t *testing.T) {
	s := query.InsertValues{
		Table: sql.TableName{
			Database: sql.ID("left"),
			Schema:   sql.ID("middle"),
			Table:    sql.ID("right"),
		},
	}
	r := "INSERT INTO left.middle.right VALUES"
	if s.String() != r {
		t.Errorf("InsertValues{}.String() got %s want %s", s.String(), r)
	}

	e, ses := test.StartSession(t)
	testInsert(t, e, ses, sql.TableName{sql.ID("test"), sql.PUBLIC, sql.ID("t")}, insertColumns1,
		insertColumnTypes1, insertCases1)
	testInsert(t, e, ses, sql.TableName{sql.ID("test"), sql.PUBLIC, sql.ID("t2")}, insertColumns2,
		insertColumnTypes2, insertCases2)
	testInsert(t, e, ses, sql.TableName{sql.ID("test"), sql.PUBLIC, sql.ID("t3")}, insertColumns3,
		insertColumnTypes3, insertCases3)
}

func statement(ctx context.Context, ses *evaluate.Session, e sql.Engine, tx sql.Transaction,
	s string) error {

	p := parser.NewParser(strings.NewReader(s), "statement")
	stmt, err := p.Parse()
	if err != nil {
		return err
	}

	stmt.Resolve(ses)
	plan, err := stmt.Plan(ctx, evaluate.MakePlanContext(e, tx))
	if err != nil {
		return err
	}
	stmtPlan := plan.(evaluate.StmtPlan)
	_, err = stmtPlan.Execute(ctx, tx)
	return err
}

func allRows(ctx context.Context, rows sql.Rows, numCols int) ([][]sql.Value, error) {
	all := [][]sql.Value{}
	l := len(rows.Columns())
	for {
		dest := make([]sql.Value, l)
		err := rows.Next(ctx, dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		all = append(all, dest[:numCols])
	}
	return all, nil
}

func testInsert(t *testing.T, e sql.Engine, ses *evaluate.Session, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, cases []insertCase) {

	ctx := context.Background()
	for _, c := range cases {
		tx := e.Begin(0)
		err := tx.CreateTable(ctx, tn, cols, colTypes, nil, false)
		if err != nil {
			t.Error(err)
			return
		}

		err = statement(ctx, ses, e, tx, c.stmt)
		if c.fail {
			if err == nil {
				t.Errorf("Parse(\"%s\").Execute() did not fail", c.stmt)
			}
		} else if err != nil {
			t.Errorf("Parse(\"%s\").Execute() failed with %s", c.stmt, err.Error())
		} else {
			var tbl sql.Table
			tbl, _, err = tx.LookupTable(ctx, tn)
			if err != nil {
				t.Error(err)
				continue
			}
			var rows sql.Rows
			rows, err = tbl.Rows(ctx, nil, nil)
			if err != nil {
				t.Errorf("(%s).Rows() failed with %s", tn, err)
				continue
			}
			var all [][]sql.Value
			all, err = allRows(ctx, rows, len(cols))
			if err != nil {
				t.Errorf("(%s).Rows().Next() failed with %s", tn, err)
				continue
			}
			if !reflect.DeepEqual(all, c.rows) {
				t.Errorf("(%s).Rows() got %v want %v", tn, all, c.rows)
			}
		}

		err = tx.DropTable(ctx, tn, false)
		if err != nil {
			t.Error(err)
			return
		}

		err = tx.Commit(ctx)
		if err != nil {
			t.Error(err)
			return
		}
	}
}
