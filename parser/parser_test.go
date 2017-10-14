package parser

import (
	"fmt"
	"strings"
	"testing"

	"maho/db"
	"maho/expr"
	"maho/join"
	"maho/parser/token"
	"maho/sql"
	"maho/stmt"
	"maho/test"
)

func TestScan(t *testing.T) {
	s := `create foobar * 123 (,) 'string' "identifier" 456.789`
	tokens := []rune{token.Reserved, token.Identifier, token.Star, token.Integer, token.LParen,
		token.Comma, token.RParen, token.String, token.Identifier, token.Double, token.EOF}
	p := newParser(strings.NewReader(s), "scan")
	for _, e := range tokens {
		r := p.scan()
		if e != r {
			t.Errorf("scan(%q) got %s want %s", s, token.Format(r), token.Format(e))
		}
	}

	p = newParser(strings.NewReader(s), "scan")
	for i := 0; i < len(tokens); i++ {
		if i >= lookBackAmount {
			for j := 0; j < lookBackAmount; j++ {
				p.unscan()
			}
			for j := lookBackAmount; j > 0; j-- {
				r := p.scan()
				if tokens[i-j] != r {
					t.Errorf("scan(%q) got %s want %s", s, token.Format(r),
						token.Format(tokens[i-j]))
				}
			}
		}

		r := p.scan()
		if tokens[i] != r {
			t.Errorf("scan(%q) got %s want %s", s, token.Format(r), token.Format(tokens[i]))
		}
	}
}

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
		p := NewParser(strings.NewReader(f), fmt.Sprintf("failed[%d]", i))
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
		p := NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		cs, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.sql)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if cs, ok := cs.(*stmt.CreateTable); !ok || !test.DeepEqual(&c.stmt, cs) {
				t.Errorf("Parse(%q) got %s want %s", c.sql, cs.String(), c.stmt.String())
			}
		}
	}
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
		p := NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		is, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.sql)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if is, ok := is.(*stmt.InsertValues); !ok || !test.DeepEqual(&c.stmt, is) {
				t.Errorf("Parse(%q) got %s want %s", c.sql, is.String(), c.stmt.String())
			}
		}
	}
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
		p := NewParser(strings.NewReader(c.sql), fmt.Sprintf("cases[%d]", i))
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
		p := NewParser(strings.NewReader(f), fmt.Sprintf("fails[%d]", i))
		e, err := p.ParseExpr()
		if err == nil {
			t.Errorf("ParseExpr(%q) did not fail, got %s", f, e)
		}
	}
}

func TestSelect(t *testing.T) {
	cases := []struct {
		sql  string
		stmt stmt.Select
		fail bool
	}{
		{sql: "select", fail: true},
		{sql: "select *, * from t", fail: true},
		{sql: "select c, * from t", fail: true},
		{sql: "select c, from t", fail: true},
		{sql: "select t.c, c, * from t", fail: true},
		{
			sql:  "select *",
			stmt: stmt.Select{},
		},
		{
			sql: "select * from t",
			stmt: stmt.Select{
				From: join.FromTableAlias{
					TableName: stmt.TableName{Table: sql.ID("t")},
					Alias:     sql.ID("t"),
				},
			},
		},
		{
			sql: "select * from t where x > 1",
			stmt: stmt.Select{
				From: join.FromTableAlias{
					TableName: stmt.TableName{Table: sql.ID("t")},
					Alias:     sql.ID("t"),
				},
				Where: &expr.Binary{expr.GreaterThanOp, expr.Ref{sql.ID("x")},
					&expr.Literal{int64(1)}},
			},
		},
		{
			sql: "select c from t",
			stmt: stmt.Select{
				From: join.FromTableAlias{
					TableName: stmt.TableName{Table: sql.ID("t")},
					Alias:     sql.ID("t"),
				},
				Results: []stmt.SelectResult{
					stmt.TableColumnResult{Column: sql.ID("c")},
				},
			},
		},
		{
			sql: "select c1, c2, t.c3 from t",
			stmt: stmt.Select{
				From: join.FromTableAlias{
					TableName: stmt.TableName{Table: sql.ID("t")},
					Alias:     sql.ID("t"),
				},
				Results: []stmt.SelectResult{
					stmt.TableColumnResult{Column: sql.ID("c1")},
					stmt.TableColumnResult{Column: sql.ID("c2")},
					stmt.TableColumnResult{
						Table:  sql.ID("t"),
						Column: sql.ID("c3"),
					},
				},
			},
		},
		{
			sql: "select t.*, c1, c2 from t",
			stmt: stmt.Select{
				From: join.FromTableAlias{
					TableName: stmt.TableName{Table: sql.ID("t")},
					Alias:     sql.ID("t"),
				},
				Results: []stmt.SelectResult{
					stmt.TableResult{sql.ID("t")},
					stmt.TableColumnResult{Column: sql.ID("c1")},
					stmt.TableColumnResult{Column: sql.ID("c2")},
				},
			},
		},
		{
			sql: "select c1, t.*, c2 from t",
			stmt: stmt.Select{
				From: join.FromTableAlias{
					TableName: stmt.TableName{Table: sql.ID("t")},
					Alias:     sql.ID("t"),
				},
				Results: []stmt.SelectResult{
					stmt.TableColumnResult{Column: sql.ID("c1")},
					stmt.TableResult{sql.ID("t")},
					stmt.TableColumnResult{Column: sql.ID("c2")},
				},
			},
		},
		{
			sql: "select c1, c2, t.* from t",
			stmt: stmt.Select{
				From: join.FromTableAlias{
					TableName: stmt.TableName{Table: sql.ID("t")},
					Alias:     sql.ID("t"),
				},
				Results: []stmt.SelectResult{
					stmt.TableColumnResult{Column: sql.ID("c1")},
					stmt.TableColumnResult{Column: sql.ID("c2")},
					stmt.TableResult{sql.ID("t")},
				},
			},
		},
		{
			sql: "select t2.c1 as a1, c2 as a2 from t",
			stmt: stmt.Select{
				From: join.FromTableAlias{
					TableName: stmt.TableName{Table: sql.ID("t")},
					Alias:     sql.ID("t"),
				},
				Results: []stmt.SelectResult{
					stmt.TableColumnResult{
						Column: sql.ID("c1"),
						Table:  sql.ID("t2"),
						Alias:  sql.ID("a1"),
					},
					stmt.TableColumnResult{Column: sql.ID("c2"), Alias: sql.ID("a2")},
				},
			},
		},
		{
			sql: "select t2.c1 a1, c2 a2 from t",
			stmt: stmt.Select{
				From: join.FromTableAlias{
					TableName: stmt.TableName{Table: sql.ID("t")},
					Alias:     sql.ID("t"),
				},
				Results: []stmt.SelectResult{
					stmt.TableColumnResult{
						Column: sql.ID("c1"),
						Table:  sql.ID("t2"),
						Alias:  sql.ID("a1"),
					},
					stmt.TableColumnResult{Column: sql.ID("c2"), Alias: sql.ID("a2")},
				},
			},
		},
		{
			sql: "select c1 + c2 as a from t",
			stmt: stmt.Select{
				From: join.FromTableAlias{
					TableName: stmt.TableName{Table: sql.ID("t")},
					Alias:     sql.ID("t"),
				},
				Results: []stmt.SelectResult{
					stmt.ExprResult{
						Expr: &expr.Binary{expr.AddOp,
							expr.Ref{sql.ID("c1")}, expr.Ref{sql.ID("c2")}},
						Alias: sql.ID("a"),
					},
				},
			},
		},
		{
			sql: "select t1.c1, t2.c2 from t1, t2",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t1")}, Alias: sql.ID("t1")},
					Right: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t2")}, Alias: sql.ID("t2")},
					Type: join.CrossJoin,
				},
				Results: []stmt.SelectResult{
					stmt.TableColumnResult{Column: sql.ID("c1"), Table: sql.ID("t1")},
					stmt.TableColumnResult{Column: sql.ID("c2"), Table: sql.ID("t2")},
				},
			},
		},
		{
			sql: "select * from t1, t2, t3",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromJoin{
						Left: join.FromTableAlias{
							TableName: stmt.TableName{Table: sql.ID("t1")}, Alias: sql.ID("t1")},
						Right: join.FromTableAlias{
							TableName: stmt.TableName{Table: sql.ID("t2")}, Alias: sql.ID("t2")},
						Type: join.CrossJoin,
					},
					Right: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t3")}, Alias: sql.ID("t3")},
					Type: join.CrossJoin,
				},
			},
		},
		{
			sql: "select * from t1 join t2, t3",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromJoin{
						Left: join.FromTableAlias{
							TableName: stmt.TableName{Table: sql.ID("t1")}, Alias: sql.ID("t1")},
						Right: join.FromTableAlias{
							TableName: stmt.TableName{Table: sql.ID("t2")}, Alias: sql.ID("t2")},
						Type: join.Join,
					},
					Right: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t3")}, Alias: sql.ID("t3")},
					Type: join.CrossJoin,
				},
			},
		},
		{
			sql: "select * from t1, t2 natural full outer join t3",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t1")}, Alias: sql.ID("t1")},
					Right: join.FromJoin{
						Left: join.FromTableAlias{
							TableName: stmt.TableName{Table: sql.ID("t2")}, Alias: sql.ID("t2")},
						Right: join.FromTableAlias{
							TableName: stmt.TableName{Table: sql.ID("t3")}, Alias: sql.ID("t3")},
						Natural: true,
						Type:    join.FullOuterJoin,
					},
					Type: join.CrossJoin,
				},
			},
		},
		{
			sql: "select * from (t1, t2) right join t3",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromJoin{
						Left: join.FromTableAlias{
							TableName: stmt.TableName{Table: sql.ID("t1")}, Alias: sql.ID("t1")},
						Right: join.FromTableAlias{
							TableName: stmt.TableName{Table: sql.ID("t2")}, Alias: sql.ID("t2")},
						Type: join.CrossJoin,
					},
					Right: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t3")}, Alias: sql.ID("t3")},
					Type: join.RightJoin,
				},
			},
		},
		{
			sql: "select * from t1 natural inner join t2",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t1")}, Alias: sql.ID("t1")},
					Right: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t2")}, Alias: sql.ID("t2")},
					Natural: true,
					Type:    join.InnerJoin,
				},
			},
		},
		{
			sql: "select * from t1 inner join t2 on c1 > 5",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t1")}, Alias: sql.ID("t1")},
					Right: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t2")}, Alias: sql.ID("t2")},
					Type: join.InnerJoin,
					On: &expr.Binary{expr.GreaterThanOp,
						expr.Ref{sql.ID("c1")}, &expr.Literal{int64(5)}},
				},
			},
		},
		{
			sql: "select * from t1 inner join t2 using (c1, c2, c3)",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t1")}, Alias: sql.ID("t1")},
					Right: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t2")}, Alias: sql.ID("t2")},
					Type:  join.InnerJoin,
					Using: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
				},
			},
		},
		{sql: "select * from t1 inner join t2", fail: true},
		{sql: "select * from t1 natural inner join t2 on c1 > 5", fail: true},
		{sql: "select * from t1 natural inner join t2 using (c1, c2)", fail: true},
		{sql: "select * from t1 inner join t2 on c1 > 5 using (c1, c2)", fail: true},
		{sql: "select * from t1 natural cross join t2", fail: true},
		{sql: "select * from t1 cross join t2 on c1 > 5", fail: true},
		{sql: "select * from t1 cross join t2 using (c1, c2)", fail: true},
		{sql: "select * from t1 inner join t2 using ()", fail: true},
		{sql: "select * from t1 inner join t2 using (c1, c1)", fail: true},
		{
			sql: "select * from (select * from t1) join t2",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromSelect{
						Select: &stmt.Select{
							From: join.FromTableAlias{
								TableName: stmt.TableName{Table: sql.ID("t1")},
								Alias:     sql.ID("t1"),
							},
						},
					},
					Right: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t2")},
						Alias:     sql.ID("t2"),
					},
					Type: join.Join,
				},
			},
		},
		{
			sql: "select * from t2 join (values (1, 'abc', true))",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromTableAlias{
						TableName: stmt.TableName{Table: sql.ID("t2")},
						Alias:     sql.ID("t2"),
					},
					Right: join.FromValues{
						Values: &stmt.Values{
							Rows: [][]expr.Expr{
								{&expr.Literal{int64(1)}, &expr.Literal{"abc"},
									&expr.Literal{true}},
							},
						},
					},
					Type: join.Join,
				},
			},
		},
		{
			sql: "select * from (select * from t1) s1 join (values (1, 'abc', true)) as v1",
			stmt: stmt.Select{
				From: join.FromJoin{
					Left: join.FromSelect{
						Select: &stmt.Select{
							From: join.FromTableAlias{
								TableName: stmt.TableName{Table: sql.ID("t1")},
								Alias:     sql.ID("t1"),
							},
						},
						Alias: sql.ID("s1"),
					},
					Right: join.FromValues{
						Values: &stmt.Values{
							Rows: [][]expr.Expr{
								{&expr.Literal{int64(1)}, &expr.Literal{"abc"},
									&expr.Literal{true}},
							},
						},
						Alias: sql.ID("v1"),
					},
					Type: join.Join,
				},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		ss, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.sql)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if ss, ok := ss.(*stmt.Select); !ok || !test.DeepEqual(&c.stmt, ss) {
				t.Errorf("Parse(%q) got %s want %s", c.sql, ss.String(), c.stmt.String())
			}
		}
	}
}

func TestValues(t *testing.T) {
	cases := []struct {
		sql  string
		stmt stmt.Values
		fail bool
	}{
		{sql: "values", fail: true},
		{sql: "values (", fail: true},
		{sql: "values ()", fail: true},
		{sql: "values (1", fail: true},
		{sql: "values (1, 2", fail: true},
		{sql: "values (1 2)", fail: true},
		{sql: "values (1, 2), (3)", fail: true},
		{sql: "values (1, 2, 3), (4, 5), (6, 7, 8)", fail: true},
		{
			sql: "values (1, 'abc', true)",
			stmt: stmt.Values{
				Rows: [][]expr.Expr{
					{&expr.Literal{int64(1)}, &expr.Literal{"abc"}, &expr.Literal{true}},
				},
			},
		},
		{
			sql: "values (1, 'abc', true), (2, 'def', false)",
			stmt: stmt.Values{
				Rows: [][]expr.Expr{
					{&expr.Literal{int64(1)}, &expr.Literal{"abc"}, &expr.Literal{true}},
					{&expr.Literal{int64(2)}, &expr.Literal{"def"}, &expr.Literal{false}},
				},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		vs, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.sql)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if vs, ok := vs.(*stmt.Values); !ok || !test.DeepEqual(&c.stmt, vs) {
				t.Errorf("Parse(%q) got %s want %s", c.sql, vs.String(), c.stmt.String())
			}
		}
	}
}

func TestDelete(t *testing.T) {
	cases := []struct {
		sql  string
		stmt stmt.Delete
		fail bool
	}{
		{sql: "delete", fail: true},
		{sql: "delete t", fail: true},
		{sql: "delete from", fail: true},
		{sql: "delete from t1, t2", fail: true},
		{sql: "delete from t where", fail: true},
		{
			sql: "delete from t",
			stmt: stmt.Delete{
				Table: stmt.TableName{Table: sql.ID("t")},
			},
		},
		{
			sql: "delete from t where c > 1",
			stmt: stmt.Delete{
				Table: stmt.TableName{Table: sql.ID("t")},
				Where: &expr.Binary{expr.GreaterThanOp, expr.Ref{sql.ID("c")},
					&expr.Literal{int64(1)}},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		ds, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.sql)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if ds, ok := ds.(*stmt.Delete); !ok || !test.DeepEqual(&c.stmt, ds) {
				t.Errorf("Parse(%q) got %s want %s", c.sql, ds.String(), c.stmt.String())
			}
		}
	}
}

func TestUpdate(t *testing.T) {
	cases := []struct {
		sql  string
		stmt stmt.Update
		fail bool
	}{
		{sql: "update", fail: true},
		{sql: "update t", fail: true},
		{sql: "update t set", fail: true},
		{sql: "update set t c = 5", fail: true},
		{sql: "update t c = 5", fail: true},
		{sql: "update t set c = 5,", fail: true},
		{sql: "update t set c = 5, where", fail: true},
		{sql: "update t set c = 5 where", fail: true},
		{sql: "update t set where c = 6", fail: true},
		{
			sql: "update t set c = 5",
			stmt: stmt.Update{
				Table: stmt.TableName{Table: sql.ID("t")},
				ColumnUpdates: []stmt.ColumnUpdate{
					{Column: sql.ID("c"), Expr: &expr.Literal{int64(5)}},
				},
			},
		},
		{
			sql: "update t set c = 0 where c > 1",
			stmt: stmt.Update{
				Table: stmt.TableName{Table: sql.ID("t")},
				ColumnUpdates: []stmt.ColumnUpdate{
					{Column: sql.ID("c"), Expr: &expr.Literal{int64(0)}},
				},
				Where: &expr.Binary{expr.GreaterThanOp, expr.Ref{sql.ID("c")},
					&expr.Literal{int64(1)}},
			},
		},
		{
			sql: "update t set c1 = 1, c2 = 2, c3 = 3",
			stmt: stmt.Update{
				Table: stmt.TableName{Table: sql.ID("t")},
				ColumnUpdates: []stmt.ColumnUpdate{
					{Column: sql.ID("c1"), Expr: &expr.Literal{int64(1)}},
					{Column: sql.ID("c2"), Expr: &expr.Literal{int64(2)}},
					{Column: sql.ID("c3"), Expr: &expr.Literal{int64(3)}},
				},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		us, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.sql)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if us, ok := us.(*stmt.Update); !ok || !test.DeepEqual(&c.stmt, us) {
				t.Errorf("Parse(%q) got %s want %s", c.sql, us.String(), c.stmt.String())
			}
		}
	}
}
