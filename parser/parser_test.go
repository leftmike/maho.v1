package parser_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"maho/db"
	"maho/expr"
	"maho/parser"
	"maho/sql"
	"maho/stmt"
)

func TestParse(t *testing.T) {
	failed := []string{
		"create foobar",
		"create temp index",
		"create unique table",
		"create table if not my-table",
		"create table (my-table)",
		"create table .my-table",
		"create table my-schema.",
	}

	for i, f := range failed {
		p := parser.NewParser(strings.NewReader(f), fmt.Sprintf("failed[%d]", i))
		stmt, err := p.Parse()
		if stmt != nil || err == nil {
			t.Errorf("Parse(%q) did not fail", f)
		}
	}
}

func TestCreateTable(t *testing.T) {
	cases := []struct {
		sql  string
		stmt stmt.CreateTable
		fail bool
	}{
		{sql: "create temp table t (c int)", fail: true},
		{sql: "create temporary table t (c int)", fail: true},
		{sql: "create table if not exists t (c int)", fail: true},
		{sql: "create table test ()", fail: true},
		{sql: "create table test (c)", fail: true},
		{sql: "create table (c int)", fail: true},
		{sql: "create table t (c int, c bool)", fail: true},
		{sql: "create table t (c int, d bool, c char(1))", fail: true},
		{sql: "create table t (c int) default", fail: true},
		{sql: "create table . (c int)", fail: true},
		{sql: "create table .t (c int)", fail: true},
		{sql: "create table d. (c int)", fail: true},
		{sql: "create table t (c int, )", fail: true},
		{sql: "create table t (c bool())", fail: true},
		{sql: "create table t (c bool(1))", fail: true},
		{sql: "create table t (c double())", fail: true},
		{sql: "create table t (c double(1,2,3))", fail: true},
		{sql: "create table t (c double(0))", fail: true},
		{sql: "create table t (c double(256))", fail: true},
		{sql: "create table t (c double(0,15))", fail: true},
		{sql: "create table t (c double(256,15))", fail: true},
		{sql: "create table t (c double(123,-1))", fail: true},
		{sql: "create table t (c double(123,31))", fail: true},
		{sql: "create table t (c int())", fail: true},
		{sql: "create table t (c int(1,2))", fail: true},
		{sql: "create table t (c int(0))", fail: true},
		{sql: "create table t (c int(256))", fail: true},
		{sql: "create table t (c varbinary)", fail: true},
		{sql: "create table t (c varchar)", fail: true},
		{sql: "create table t (c char(1,2))", fail: true},
		{sql: "create table t (c char(-1))", fail: true},
		{sql: "create table t (c blob binary)", fail: true},
		{sql: "create table t (c int binary)", fail: true},
		{sql: "create table t (c bool binary)", fail: true},
		{sql: "create table t (c char binary(123))", fail: true},
		{sql: "create table t (c double binary)", fail: true},
		{sql: "create table t (c char null)", fail: true},
		{sql: "create table t (c char null, d int)", fail: true},
		{sql: "create table t (c char not null not null)", fail: true},
		{sql: "create table t (c char default)", fail: true},
		{sql: "create table t (c char default, d int)", fail: true},
		{sql: "create table t (c int default 0 default 1)", fail: true},
		{
			sql: "create table t (c1 tinyint, c2 smallint, c3 mediumint, c4 integer, c5 bigint)",
			stmt: stmt.CreateTable{
				Table: stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4"),
					sql.ID("c5")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.IntegerType, Size: 1, Width: 255},
					{Type: sql.IntegerType, Size: 2, Width: 255},
					{Type: sql.IntegerType, Size: 3, Width: 255},
					{Type: sql.IntegerType, Size: 4, Width: 255},
					{Type: sql.IntegerType, Size: 8, Width: 255},
				},
			},
		},
		{
			sql: "create table t (c1 tinyint(1), c2 smallint(2), c3 mediumint(3), c4 integer(4))",
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.IntegerType, Size: 1, Width: 1},
					{Type: sql.IntegerType, Size: 2, Width: 2},
					{Type: sql.IntegerType, Size: 3, Width: 3},
					{Type: sql.IntegerType, Size: 4, Width: 4},
				},
			},
		},
		{
			sql: "create table t (b1 bool, b2 boolean, d1 double, d2 double)",
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("b1"), sql.ID("b2"), sql.ID("d1"), sql.ID("d2")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.BooleanType, Size: 1},
					{Type: sql.BooleanType, Size: 1},
					{Type: sql.DoubleType, Size: 8, Width: 255, Fraction: 30},
					{Type: sql.DoubleType, Size: 8, Width: 255, Fraction: 30},
				},
			},
		},
		{
			sql: "create table t (d1 double(123,4), d2 double(12,3))",
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("d1"), sql.ID("d2")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.DoubleType, Size: 8, Width: 123, Fraction: 4},
					{Type: sql.DoubleType, Size: 8, Width: 12, Fraction: 3},
				},
			},
		},
		{
			sql: "create table t (b1 binary, b2 varbinary(123), b3 blob)",
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("b1"), sql.ID("b2"), sql.ID("b3")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.CharacterType, Fixed: true, Binary: true, Size: 1},
					{Type: sql.CharacterType, Fixed: false, Binary: true, Size: 123},
					{Type: sql.CharacterType, Fixed: false, Binary: true, Size: db.MaxColumnSize},
				},
			},
		},
		{
			sql: "create table t (b1 binary(123), b2 varbinary(456), b3 blob(789))",
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("b1"), sql.ID("b2"), sql.ID("b3")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.CharacterType, Fixed: true, Binary: true, Size: 123},
					{Type: sql.CharacterType, Fixed: false, Binary: true, Size: 456},
					{Type: sql.CharacterType, Fixed: false, Binary: true, Size: 789},
				},
			},
		},
		{
			sql: "create table t (c1 char, c2 varchar(123), c3 text)",
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.CharacterType, Fixed: true, Size: 1},
					{Type: sql.CharacterType, Fixed: false, Size: 123},
					{Type: sql.CharacterType, Fixed: false, Size: db.MaxColumnSize},
				},
			},
		},
		{
			sql: "create table t (c1 char(123), c2 varchar(456), c3 text(789))",
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.CharacterType, Fixed: true, Size: 123},
					{Type: sql.CharacterType, Fixed: false, Size: 456},
					{Type: sql.CharacterType, Fixed: false, Size: 789},
				},
			},
		},
		{
			sql: "create table t (b1 char binary, b2 varchar(123) binary, b3 text binary)",
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("b1"), sql.ID("b2"), sql.ID("b3")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.CharacterType, Fixed: true, Binary: true, Size: 1},
					{Type: sql.CharacterType, Fixed: false, Binary: true, Size: 123},
					{Type: sql.CharacterType, Fixed: false, Binary: true, Size: db.MaxColumnSize},
				},
			},
		},
		{
			sql: "create table t (c1 varchar(64) default 'abcd', c2 int default 123)",
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.CharacterType, Fixed: false, Size: 64,
						Default: &expr.Literal{"abcd"}},
					{Type: sql.IntegerType, Size: 4, Width: 255,
						Default: &expr.Literal{int64(123)}},
				},
			},
		},
		{
			sql: "create table t (c1 boolean default true, c2 boolean not null)",
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.BooleanType, Size: 1, Default: &expr.Literal{true}},
					{Type: sql.BooleanType, Size: 1, NotNull: true},
				},
			},
		},
		{
			sql: `create table t (c1 boolean default true not null,
c2 boolean not null default true)`,
			stmt: stmt.CreateTable{
				Table:   stmt.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []db.ColumnType{
					{Type: sql.BooleanType, Size: 1, Default: &expr.Literal{true}, NotNull: true},
					{Type: sql.BooleanType, Size: 1, NotNull: true,
						Default: &expr.Literal{true}},
				},
			},
		},
	}

	for i, c := range cases {
		p := parser.NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		stmt, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.sql)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if !createTableEqual(c.stmt, stmt) {
				t.Errorf("Parse(%q) got %s want %s", c.sql, stmt.String(), c.stmt.String())
			}
		}
	}
}

func createTableEqual(stmt1 stmt.CreateTable, s2 stmt.Stmt) bool {
	stmt2, ok := s2.(*stmt.CreateTable)
	if !ok {
		return false
	}
	if stmt1.Table.Database != stmt2.Table.Database || stmt1.Table.Table != stmt2.Table.Table ||
		len(stmt1.Columns) != len(stmt2.Columns) ||
		len(stmt1.ColumnTypes) != len(stmt2.ColumnTypes) {
		return false
	}
	for i, c1 := range stmt1.Columns {
		if c1 != stmt2.Columns[i] {
			return false
		}
	}
	for i, c1 := range stmt1.ColumnTypes {
		if !reflect.DeepEqual(c1, stmt2.ColumnTypes[i]) {
			return false
		}
	}
	return true
}

func TestInsertValues(t *testing.T) {
	cases := []struct {
		sql  string
		stmt stmt.InsertValues
		fail bool
	}{
		{sql: "insert into t", fail: true},
		{sql: "insert t values (1)", fail: true},
		{sql: "insert into t (1)", fail: true},
		{sql: "insert into t values (1", fail: true},
		{sql: "insert into t values 1)", fail: true},
		{sql: "insert into t values (1, )", fail: true},
		{sql: "insert into t values (1, 2),", fail: true},
		{sql: "insert into t values (1, 2) (3)", fail: true},
		{sql: "insert into t () values (1, 2)", fail: true},
		{sql: "insert into t (a values (1, 2)", fail: true},
		{sql: "insert into t (a, ) values (1, 2)", fail: true},
		{sql: "insert into t (a, a) values (1, 2)", fail: true},
		{sql: "insert into t (a, b, a) values (1, 2)", fail: true},
		{
			sql: "insert into t values (1, 'abc', true)",
			stmt: stmt.InsertValues{
				Table: stmt.TableName{Table: sql.ID("t")},
				Rows: [][]expr.Expr{
					{&expr.Literal{int64(1)}, &expr.Literal{"abc"}, &expr.Literal{true}},
				},
			},
		},
		{
			sql: "insert into t values (1, 'abc', true), (2, 'def', false)",
			stmt: stmt.InsertValues{
				Table: stmt.TableName{Table: sql.ID("t")},
				Rows: [][]expr.Expr{
					{&expr.Literal{int64(1)}, &expr.Literal{"abc"}, &expr.Literal{true}},
					{&expr.Literal{int64(2)}, &expr.Literal{"def"}, &expr.Literal{false}},
				},
			},
		},
		{
			sql: "insert into t values (NULL, 'abc', NULL)",
			stmt: stmt.InsertValues{
				Table: stmt.TableName{Table: sql.ID("t")},
				Rows: [][]expr.Expr{
					{&expr.Literal{nil}, &expr.Literal{"abc"}, &expr.Literal{nil}},
				},
			},
		},
	}

	for i, c := range cases {
		p := parser.NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		stmt, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.sql)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if !insertValuesEqual(c.stmt, stmt) {
				t.Errorf("Parse(%q) got %s want %s", c.sql, stmt.String(), c.stmt.String())
			}
		}
	}
}

func insertValuesEqual(stmt1 stmt.InsertValues, s2 stmt.Stmt) bool {
	stmt2, ok := s2.(*stmt.InsertValues)
	if !ok {
		return false
	}
	if stmt1.Table.Database != stmt2.Table.Database || stmt1.Table.Table != stmt2.Table.Table ||
		len(stmt1.Columns) != len(stmt2.Columns) {
		return false
	}
	for i, c1 := range stmt1.Columns {
		if c1 != stmt2.Columns[i] {
			return false
		}
	}

	if len(stmt1.Rows) != len(stmt2.Rows) {
		return false
	}
	for i, r1 := range stmt1.Rows {
		r2 := stmt2.Rows[i]
		if len(r1) != len(r2) {
			return false
		}
		for j, v1 := range r1 {
			if !reflect.DeepEqual(v1, r2[j]) {
				return false
			}
		}
	}
	return true
}

func TestParseExpr(t *testing.T) {
	cases := []struct {
		sql  string
		expr string
	}{
		{"1 * 2 - 3 = 4", "((1 * (2 - 3)) == 4)"},
		{"1 * 2 * 3 - 5", "((1 * (2 * 3)) - 5)"},
		{"1 - 2 * (3 + 4) + 5", "(1 - ((2 * (3 + 4)) + 5))"},
		{"1 + 2 = 3 * 4 - 5 * 6", "((1 + 2) == ((3 * 4) - (5 * 6)))"},
		{"NOT 12 AND 1 OR 3", "(((NOT 12) AND 1) OR 3)"},
		{"- 1 * 2 + 3", "(((- 1) * 2) + 3)"},
		{"- 1 * 2", "((- 1) * 2)"},
		{"12 % 34 + 56", "((12 % 34) + 56)"},
		{"12 == 34 OR 56 != 78", "((12 == 34) OR (56 != 78))"},
		{"12 + 34 << 56 + 78", "((12 + 34) << (56 + 78))"},
		{"abc", "abc"},
		{"abc.def", "abc.def"},
		{"abc. def . ghi .jkl", "abc.def.ghi.jkl"},
		{"abc(1 + 2)", "abc((1 + 2))"},
		{"abc()", "abc()"},
		{"abc(1 + 2, def() * 3)", "abc((1 + 2), (def() * 3))"},
	}

	for i, c := range cases {
		p := parser.NewParser(strings.NewReader(c.sql), fmt.Sprintf("cases[%d]", i))
		e, err := p.ParseExpr()
		if err != nil {
			t.Errorf("ParseExpr(%q) failed with %s", c.sql, err)
		} else if c.expr != e.String() {
			t.Errorf("ParseExpr(%q) got %s want %s", c.sql, e, c.expr)
		}
	}

	fails := []string{
		"1 *",
		"(1 * 2",
		"(*)",
		"abc.123",
		"((1 + 2) * 3",
		"abc(123,",
	}

	for i, f := range fails {
		p := parser.NewParser(strings.NewReader(f), fmt.Sprintf("fails[%d]", i))
		e, err := p.ParseExpr()
		if err == nil {
			t.Errorf("ParseExpr(%q) did not fail, got %s", f, e)
		}
	}
}