package parser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/evaluate/datadef"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/evaluate/misc"
	"github.com/leftmike/maho/evaluate/query"
	"github.com/leftmike/maho/parser/token"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

func TestScan(t *testing.T) {
	s := `create foobar * 123 (,) 'string' "identifier" ; 456.789`
	tokens := []rune{token.Reserved, token.Identifier, token.Star, token.Integer, token.LParen,
		token.Comma, token.RParen, token.String, token.Identifier, token.EndOfStatement,
		token.Float, token.EOF}
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
		stmt datadef.CreateTable
		fail bool
	}{
		{sql: "create temp table t (c int)", fail: true},
		{sql: "create temporary table t (c int)", fail: true},
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
		{sql: "create table t (c char(1,2))", fail: true},
		{sql: "create table t (c char(-1))", fail: true},
		{sql: "create table t (c blob binary)", fail: true},
		{sql: "create table t (c int binary)", fail: true},
		{sql: "create table t (c bool binary)", fail: true},
		{sql: "create table t (c char(123) binary)", fail: true},
		{sql: "create table t (c double binary)", fail: true},
		{sql: "create table t (c char null)", fail: true},
		{sql: "create table t (c char null, d int)", fail: true},
		{sql: "create table t (c char not null not null)", fail: true},
		{sql: "create table t (c char default)", fail: true},
		{sql: "create table t (c char default, d int)", fail: true},
		{sql: "create table t (c int default 0 default 1)", fail: true},
		{
			sql: "create table t (c1 int2, c2 smallint, c3 int4, c4 integer, c5 bigint, c6 int8)",
			stmt: datadef.CreateTable{
				Table: sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4"),
					sql.ID("c5"), sql.ID("c6")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.IntegerType, Size: 2},
					{Type: sql.IntegerType, Size: 2},
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.IntegerType, Size: 8},
					{Type: sql.IntegerType, Size: 8},
				},
			},
		},
		{
			sql: "create table if not exists t (c int)",
			stmt: datadef.CreateTable{
				Table:       sql.TableName{Table: sql.ID("t")},
				Columns:     []sql.Identifier{sql.ID("c")},
				ColumnTypes: []datadef.ColumnType{{Type: sql.IntegerType, Size: 4}},
				IfNotExists: true,
			},
		},
		{
			sql: "create table t (b1 bool, b2 boolean, d1 double, d2 double)",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("b1"), sql.ID("b2"), sql.ID("d1"), sql.ID("d2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.BooleanType, Size: 1},
					{Type: sql.BooleanType, Size: 1},
					{Type: sql.FloatType, Size: 8},
					{Type: sql.FloatType, Size: 8},
				},
			},
		},
		{
			sql: "create table t (b1 binary, b2 varbinary(123), b3 blob, b4 bytes, b5 bytea)",
			stmt: datadef.CreateTable{
				Table: sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("b1"), sql.ID("b2"), sql.ID("b3"), sql.ID("b4"),
					sql.ID("b5")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.BytesType, Fixed: true, Size: 1},
					{Type: sql.BytesType, Fixed: false, Size: 123},
					{Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize},
					{Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize},
					{Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize},
				},
			},
		},
		{
			sql: "create table t (b1 binary(123), b2 varbinary(456), b3 blob(789))",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("b1"), sql.ID("b2"), sql.ID("b3")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.BytesType, Fixed: true, Size: 123},
					{Type: sql.BytesType, Fixed: false, Size: 456},
					{Type: sql.BytesType, Fixed: false, Size: 789},
				},
			},
		},
		{
			sql: "create table t (b1 bytea(456), b2 bytes(789))",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("b1"), sql.ID("b2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.BytesType, Fixed: false, Size: 456},
					{Type: sql.BytesType, Fixed: false, Size: 789},
				},
			},
		},
		{
			sql: "create table t (c1 char, c2 varchar(123), c3 text)",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.StringType, Fixed: true, Size: 1},
					{Type: sql.StringType, Fixed: false, Size: 123},
					{Type: sql.StringType, Fixed: false, Size: sql.MaxColumnSize},
				},
			},
		},
		{
			sql: "create table t (c1 char(123), c2 varchar(456), c3 text(789))",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.StringType, Fixed: true, Size: 123},
					{Type: sql.StringType, Fixed: false, Size: 456},
					{Type: sql.StringType, Fixed: false, Size: 789},
				},
			},
		},
		{
			sql: "create table t (c1 varchar(64) default 'abcd', c2 int default 123)",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.StringType, Fixed: false, Size: 64,
						Default: expr.StringLiteral("abcd")},
					{Type: sql.IntegerType, Size: 4, Default: expr.Int64Literal(123)},
				},
				Constraints: []datadef.Constraint{
					{Type: sql.DefaultConstraint, Name: sql.ID("c1_default"), ColNum: 0},
					{Type: sql.DefaultConstraint, Name: sql.ID("c2_default"), ColNum: 1},
				},
			},
		},
		{
			sql: "create table t (c1 boolean default true, c2 boolean not null)",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.BooleanType, Size: 1, Default: expr.True()},
					{Type: sql.BooleanType, Size: 1, NotNull: true},
				},
				Constraints: []datadef.Constraint{
					{Type: sql.DefaultConstraint, Name: sql.ID("c1_default"), ColNum: 0},
					{Type: sql.NotNullConstraint, Name: sql.ID("c2_not_null"), ColNum: 1},
				},
			},
		},
		{
			sql: `create table t (c1 boolean default true not null,
c2 boolean not null default true)`,
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.BooleanType, Size: 1, Default: expr.True(), NotNull: true},
					{Type: sql.BooleanType, Size: 1, NotNull: true, Default: expr.True()},
				},
				Constraints: []datadef.Constraint{
					{Type: sql.DefaultConstraint, Name: sql.ID("c1_default"), ColNum: 0},
					{Type: sql.NotNullConstraint, Name: sql.ID("c1_not_null"), ColNum: 0},
					{Type: sql.NotNullConstraint, Name: sql.ID("c2_not_null"), ColNum: 1},
					{Type: sql.DefaultConstraint, Name: sql.ID("c2_default"), ColNum: 1},
				},
			},
		},
		{sql: "create table t (c1 int primary, c2 bool)", fail: true},
		{sql: "create table t (c1 int unique primary key, c2 bool)", fail: true},
		{sql: "create table t (c1 int, c2 bool, primary)", fail: true},
		{sql: "create table t (c1 int, c2 bool, primary key)", fail: true},
		{sql: "create table t (c1 int, c2 bool, primary key ())", fail: true},
		{sql: "create table t (c1 int primary key, c2 bool, primary key (c1))", fail: true},
		{sql: "create table t (c1 int, c2 bool primary key, primary key (c1))", fail: true},
		{
			sql: "create table t (c1 int primary key, c2 bool)",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.BooleanType, Size: 1},
				},
				Constraints: []datadef.Constraint{
					{
						Type:   sql.PrimaryConstraint,
						Name:   sql.ID("c1_primary"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c1")},
							Reverse: []bool{false},
						},
					},
				},
			},
		},
		{
			sql: "create table t (c1 int unique, c2 bool)",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.BooleanType, Size: 1},
				},
				Constraints: []datadef.Constraint{
					{
						Type:   sql.UniqueConstraint,
						Name:   sql.ID("c1_unique"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c1")},
							Reverse: []bool{false},
						},
					},
				},
			},
		},
		{
			sql: "create table t (c1 int, c2 bool, primary key (c1))",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.BooleanType, Size: 1},
				},
				Constraints: []datadef.Constraint{
					{
						Type:   sql.PrimaryConstraint,
						Name:   sql.ID("c1_primary"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c1")},
							Reverse: []bool{false},
						},
					},
				},
			},
		},
		{
			sql: "create table t (c1 int, c2 bool, primary key (c1 desc))",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.BooleanType, Size: 1},
				},
				Constraints: []datadef.Constraint{
					{
						Type:   sql.PrimaryConstraint,
						Name:   sql.ID("c1_primary"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c1")},
							Reverse: []bool{true},
						},
					},
				},
			},
		},
		{
			sql: "create table t (c1 int unique, c2 bool unique, primary key (c1 desc, c2 asc))",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.BooleanType, Size: 1},
				},
				Constraints: []datadef.Constraint{
					{
						Type:   sql.UniqueConstraint,
						Name:   sql.ID("c1_unique"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c1")},
							Reverse: []bool{false},
						},
					},
					{
						Type:   sql.UniqueConstraint,
						Name:   sql.ID("c2_unique"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c2")},
							Reverse: []bool{false},
						},
					},
					{
						Type:   sql.PrimaryConstraint,
						Name:   sql.ID("c1_c2_primary"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
							Reverse: []bool{true, false},
						},
					},
				},
			},
		},
		{
			sql: "create table t (c1 int primary key, c2 bool, unique (c2, c1))",
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.BooleanType, Size: 1},
				},
				Constraints: []datadef.Constraint{
					{
						Type:   sql.PrimaryConstraint,
						Name:   sql.ID("c1_primary"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c1")},
							Reverse: []bool{false},
						},
					},
					{
						Type:   sql.UniqueConstraint,
						Name:   sql.ID("c2_c1_unique"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c2"), sql.ID("c1")},
							Reverse: []bool{false, false},
						},
					},
				},
			},
		},
		{
			sql: `create table t (c1 int constraint con1 primary key, c2 bool,
constraint con2 unique (c2, c1))`,
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.BooleanType, Size: 1},
				},
				Constraints: []datadef.Constraint{
					{
						Type:   sql.PrimaryConstraint,
						Name:   sql.ID("con1"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c1")},
							Reverse: []bool{false},
						},
					},
					{
						Type:   sql.UniqueConstraint,
						Name:   sql.ID("con2"),
						ColNum: -1,
						Key: datadef.IndexKey{
							Unique:  true,
							Columns: []sql.Identifier{sql.ID("c2"), sql.ID("c1")},
							Reverse: []bool{false, false},
						},
					},
				},
			},
		},
		{
			sql: `create table t (c1 int constraint not_null not null,
c2 bool constraint dflt default true)`,
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.IntegerType, Size: 4, NotNull: true},
					{Type: sql.BooleanType, Size: 1, Default: expr.True()},
				},
				Constraints: []datadef.Constraint{
					{Type: sql.NotNullConstraint, Name: sql.ID("not_null"), ColNum: 0},
					{Type: sql.DefaultConstraint, Name: sql.ID("dflt"), ColNum: 1},
				},
			},
		},
		{
			sql:  "create table t (c1 int not null constraint not_null, c2 bool)",
			fail: true,
		},
		{
			sql:  "create table t (c1 int, c2 bool not null constraint not_null)",
			fail: true,
		},
		{
			sql:  "create table t (c1 int constraint c1 constraint c1 not null, c2 bool)",
			fail: true,
		},
		{
			sql:  "create table t (c1 int constraint c1, c2 bool)",
			fail: true,
		},
		{
			sql: `create table t (c1 int constraint c2_c1_unique primary key, c2 bool,
unique (c2, c1))`,
			fail: true,
		},
		{
			sql: `create table t (c1 int primary key, c2 bool,
constraint c1_primary unique (c2, c1))`,
			fail: true,
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
			var trc string
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if cs, ok := cs.(*datadef.CreateTable); !ok ||
				!testutil.DeepEqual(&c.stmt, cs, &trc) {
				t.Errorf("Parse(%q) got %s want %s\n%s", c.sql, cs.String(), c.stmt.String(), trc)
			}
		}
	}
}

func TestCreateIndex(t *testing.T) {
	cases := []struct {
		sql  string
		stmt datadef.CreateIndex
		fail bool
	}{
		{sql: "create index unique idx on tbl (c1)", fail: true},
		{sql: "create index idx tbl (c1)", fail: true},
		{sql: "create index tbl (c1)", fail: true},
		{
			sql: "create index idx on tbl (c1 DESC, c2)",
			stmt: datadef.CreateIndex{
				Table: sql.TableName{Table: sql.ID("tbl")},
				Index: sql.ID("idx"),
				Key: datadef.IndexKey{
					Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
					Reverse: []bool{true, false},
				},
			},
		},
		{
			sql: "create unique index if not exists idx on tbl (c1)",
			stmt: datadef.CreateIndex{
				Table: sql.TableName{Table: sql.ID("tbl")},
				Index: sql.ID("idx"),
				Key: datadef.IndexKey{
					Unique:  true,
					Columns: []sql.Identifier{sql.ID("c1")},
					Reverse: []bool{false},
				},
				IfNotExists: true,
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
			var trc string
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if cs, ok := cs.(*datadef.CreateIndex); !ok ||
				!testutil.DeepEqual(&c.stmt, cs, &trc) {
				t.Errorf("Parse(%q) got %s want %s\n%s", c.sql, cs.String(), c.stmt.String(), trc)
			}
		}
	}
}

func TestInsertValues(t *testing.T) {
	cases := []struct {
		sql  string
		stmt query.InsertValues
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
			stmt: query.InsertValues{
				Table: sql.TableName{Table: sql.ID("t")},
				Rows: [][]expr.Expr{
					{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
				},
			},
		},
		{
			sql: "insert into t values (1, 'abc', true), (2, 'def', false)",
			stmt: query.InsertValues{
				Table: sql.TableName{Table: sql.ID("t")},
				Rows: [][]expr.Expr{
					{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
					{expr.Int64Literal(2), expr.StringLiteral("def"), expr.False()},
				},
			},
		},
		{
			sql: "insert into t values (NULL, 'abc', NULL)",
			stmt: query.InsertValues{
				Table: sql.TableName{Table: sql.ID("t")},
				Rows: [][]expr.Expr{
					{expr.Nil(), expr.StringLiteral("abc"), expr.Nil()},
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
			var trc string
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if is, ok := is.(*query.InsertValues); !ok ||
				!testutil.DeepEqual(&c.stmt, is, &trc) {
				t.Errorf("Parse(%q) got %s want %s\n%s", c.sql, is.String(), c.stmt.String(), trc)
			}
		}
	}
}

func TestParseExpr(t *testing.T) {
	cases := []struct {
		sql  string
		expr string
	}{
		{"1 * 2 - 3 = 4", "(((1 * 2) - 3) == 4)"},
		{"1 / 2 * 3 - 5", "(((1 / 2) * 3) - 5)"},
		{"1 - 2 * (3 + 4) + 5", "((1 - (2 * (3 + 4))) + 5)"},
		{"1 - (2 + 3) * 4 + 5", "((1 - ((2 + 3) * 4)) + 5)"},
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
		{"c1 * 10 = c2 AND c2 * 10 = c3", "(((c1 * 10) == c2) AND ((c2 * 10) == c3))"},
		{"1 + 2 - 3", "((1 + 2) - 3)"},
		{"12 / 4 * 3", "((12 / 4) * 3)"},
		{"1 - 2 + 3", "((1 - 2) + 3)"},
		{"12 * 4 / 3", "((12 * 4) / 3)"},
		{"- 2 * 3", "((- 2) * 3)"},
		{"- (2 * 3)", "(- (2 * 3))"},
		{"1 + 2 * 3", "(1 + (2 * 3))"},
		{"1 * 2 + 3 / - 4", "((1 * 2) + (3 / (- 4)))"},
		{"count(*)", "count_all()"},
		{"count(123)", "count(123)"},
		{"count(1,23,456)", "count(1, 23, 456)"},
		{"x AND y AND z", "((x AND y) AND z)"},
		{"x * y / z", "((x * y) / z)"},
		{"123 + (select * from t)", "(123 + (SELECT * FROM t))"},
		{"(values (1)) + (show schema)", "((VALUES (1)) + (SHOW SCHEMA))"},
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
		"abc(*)",
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
		stmt query.Select
		fail bool
	}{
		{sql: "select", fail: true},
		{sql: "select *, * from t", fail: true},
		{sql: "select c, * from t", fail: true},
		{sql: "select c, from t", fail: true},
		{sql: "select t.c, c, * from t", fail: true},
		{
			sql:  "select *",
			stmt: query.Select{},
		},
		{
			sql: "select * from t",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
			},
		},
		{
			sql: "select * from t where x > 1",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Where: &expr.Binary{Op: expr.GreaterThanOp, Left: expr.Ref{sql.ID("x")},
					Right: expr.Int64Literal(1)},
			},
		},
		{
			sql: "select * from t where x = (show schema)",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Where: &expr.Binary{
					Op:   expr.EqualOp,
					Left: expr.Ref{sql.ID("x")},
					Right: expr.Stmt{
						Stmt: &misc.Show{
							Variable: sql.SCHEMA,
						},
					},
				},
			},
		},
		{
			sql: "select * from (table t) as t",
			stmt: query.Select{
				From: query.FromStmt{
					Stmt: &query.Select{
						From: query.FromTableAlias{
							TableName: sql.TableName{Table: sql.ID("t")},
						},
					},
					Alias: sql.ID("t"),
				},
			},
		},
		{
			sql: "select c from t",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.ExprResult{Expr: expr.Ref{sql.ID("c")}},
				},
			},
		},
		{
			sql: "select c1, c2, t.c3 from t",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.ExprResult{Expr: expr.Ref{sql.ID("c1")}},
					query.ExprResult{Expr: expr.Ref{sql.ID("c2")}},
					query.ExprResult{Expr: expr.Ref{sql.ID("t"), sql.ID("c3")}},
				},
			},
		},
		{
			sql: "select t.*, c1, c2 from t",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.TableResult{Table: sql.ID("t")},
					query.ExprResult{Expr: expr.Ref{sql.ID("c1")}},
					query.ExprResult{Expr: expr.Ref{sql.ID("c2")}},
				},
			},
		},
		{
			sql: "select c1, t.*, c2 from t",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.ExprResult{Expr: expr.Ref{sql.ID("c1")}},
					query.TableResult{Table: sql.ID("t")},
					query.ExprResult{Expr: expr.Ref{sql.ID("c2")}},
				},
			},
		},
		{
			sql: "select c1, c2, t.* from t",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.ExprResult{Expr: expr.Ref{sql.ID("c1")}},
					query.ExprResult{Expr: expr.Ref{sql.ID("c2")}},
					query.TableResult{Table: sql.ID("t")},
				},
			},
		},
		{
			sql: "select t2.c1 as a1, c2 as a2 from t",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.ExprResult{
						Expr:  expr.Ref{sql.ID("t2"), sql.ID("c1")},
						Alias: sql.ID("a1"),
					},
					query.ExprResult{Expr: expr.Ref{sql.ID("c2")}, Alias: sql.ID("a2")},
				},
			},
		},
		{
			sql: "select t2.c1 a1, c2 a2 from t",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.ExprResult{
						Expr:  expr.Ref{sql.ID("t2"), sql.ID("c1")},
						Alias: sql.ID("a1"),
					},
					query.ExprResult{Expr: expr.Ref{sql.ID("c2")}, Alias: sql.ID("a2")},
				},
			},
		},
		{
			sql: "select c1 + c2 as a from t",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.ExprResult{
						Expr: &expr.Binary{Op: expr.AddOp,
							Left: expr.Ref{sql.ID("c1")}, Right: expr.Ref{sql.ID("c2")}},
						Alias: sql.ID("a"),
					},
				},
			},
		},
		{
			sql: "select t1.c1, t2.c2 from t1, t2",
			stmt: query.Select{
				From: query.FromJoin{
					Left:  query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t1")}},
					Right: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t2")}},
					Type:  query.CrossJoin,
				},
				Results: []query.SelectResult{
					query.ExprResult{Expr: expr.Ref{sql.ID("t1"), sql.ID("c1")}},
					query.ExprResult{Expr: expr.Ref{sql.ID("t2"), sql.ID("c2")}},
				},
			},
		},
		{
			sql: "select * from t1, t2, t3",
			stmt: query.Select{
				From: query.FromJoin{
					Left: query.FromJoin{
						Left:  query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t1")}},
						Right: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t2")}},
						Type:  query.CrossJoin,
					},
					Right: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t3")}},
					Type:  query.CrossJoin,
				},
			},
		},
		{
			sql: "select * from t1 join t2 using (c1), t3",
			stmt: query.Select{
				From: query.FromJoin{
					Left: query.FromJoin{
						Left:  query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t1")}},
						Right: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t2")}},
						Type:  query.Join,
						Using: []sql.Identifier{sql.ID("c1")},
					},
					Right: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t3")}},
					Type:  query.CrossJoin,
				},
			},
		},
		{
			sql: "select * from (t1, t2) right join t3 using (c1)",
			stmt: query.Select{
				From: query.FromJoin{
					Left: query.FromJoin{
						Left:  query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t1")}},
						Right: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t2")}},
						Type:  query.CrossJoin,
					},
					Right: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t3")}},
					Type:  query.RightJoin,
					Using: []sql.Identifier{sql.ID("c1")},
				},
			},
		},
		{
			sql: "select * from t1 inner join t2 on c1 > 5",
			stmt: query.Select{
				From: query.FromJoin{
					Left:  query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t1")}},
					Right: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t2")}},
					Type:  query.Join,
					On: &expr.Binary{Op: expr.GreaterThanOp,
						Left: expr.Ref{sql.ID("c1")}, Right: expr.Int64Literal(5)},
				},
			},
		},
		{
			sql: "select * from t1 inner join t2 using (c1, c2, c3)",
			stmt: query.Select{
				From: query.FromJoin{
					Left:  query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t1")}},
					Right: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t2")}},
					Type:  query.Join,
					Using: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
				},
			},
		},
		{sql: "select * from t1, t2 full outer join t3", fail: true},
		{sql: "select * from t1 inner join t2", fail: true},
		{sql: "select * from t1 inner join t2", fail: true},
		{sql: "select * from t1 inner join t2", fail: true},
		{sql: "select * from t1 inner join t2 on c1 > 5 using (c1, c2)", fail: true},
		{sql: "select * from t1 cross join t2 on c1 > 5", fail: true},
		{sql: "select * from t1 cross join t2 using (c1, c2)", fail: true},
		{sql: "select * from t1 inner join t2 using ()", fail: true},
		{sql: "select * from t1 inner join t2 using (c1, c1)", fail: true},
		{
			sql: "select * from (select * from t1) as s1 join t2 using (c1)",
			stmt: query.Select{
				From: query.FromJoin{
					Left: query.FromStmt{
						Stmt: &query.Select{
							From: query.FromTableAlias{
								TableName: sql.TableName{Table: sql.ID("t1")},
							},
						},
						Alias: sql.ID("s1"),
					},
					Right: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t2")}},
					Type:  query.Join,
					Using: []sql.Identifier{sql.ID("c1")},
				},
			},
		},
		{
			sql: "select * from t2 join (values (1, 'abc', true)) as v1 using (c1, c2)",
			stmt: query.Select{
				From: query.FromJoin{
					Left: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t2")}},
					Right: query.FromStmt{
						Stmt: &query.Values{
							Expressions: [][]expr.Expr{
								{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
							},
						},
						Alias: sql.ID("v1"),
					},
					Type:  query.Join,
					Using: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				},
			},
		},
		{
			sql: "select * from (select * from t1) s1 join (values (1, 'abc', true)) as v1 " +
				"using (c1, c2)",
			stmt: query.Select{
				From: query.FromJoin{
					Left: query.FromStmt{
						Stmt: &query.Select{
							From: query.FromTableAlias{
								TableName: sql.TableName{Table: sql.ID("t1")},
							},
						},
						Alias: sql.ID("s1"),
					},
					Right: query.FromStmt{
						Stmt: &query.Values{
							Expressions: [][]expr.Expr{
								{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
							},
						},
						Alias: sql.ID("v1"),
					},
					Type:  query.Join,
					Using: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				},
			},
		},
		{sql: "select * from (values (1, 'abc', true)) as v1 (", fail: true},
		{sql: "select * from (values (1, 'abc', true)) as v1 )", fail: true},
		{sql: "select * from (values (1, 'abc', true)) as v1 (,", fail: true},
		{sql: "select * from (values (1, 'abc', true)) as v1 (a,)", fail: true},
		{sql: "select * from (values (1, 'abc', true)) as v1 (a b)", fail: true},
		{
			sql: "select * from (values (1, 'abc', true)) as v1",
			stmt: query.Select{
				From: query.FromStmt{
					Stmt: &query.Values{
						Expressions: [][]expr.Expr{
							{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
						},
					},
					Alias: sql.ID("v1"),
				},
			},
		},
		{
			sql: "select * from (values (1, 'abc', true)) as v1 (c1, c2, c3)",
			stmt: query.Select{
				From: query.FromStmt{
					Stmt: &query.Values{
						Expressions: [][]expr.Expr{
							{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
						},
					},
					Alias:         sql.ID("v1"),
					ColumnAliases: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
				},
			},
		},
		{sql: "select * from (select * from t1) as s1 (", fail: true},
		{sql: "select * from (select * from t1) as s1 )", fail: true},
		{sql: "select * from (select * from t1) as s1 (,", fail: true},
		{sql: "select * from (select * from t1) as s1 (a,)", fail: true},
		{sql: "select * from (select * from t1) as s1 (a b)", fail: true},
		{
			sql: "select * from (select * from t1) as s1",
			stmt: query.Select{
				From: query.FromStmt{
					Stmt: &query.Select{
						From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t1")}},
					},
					Alias: sql.ID("s1"),
				},
			},
		},
		{
			sql: "select * from (select * from t1) as s1 (c1)",
			stmt: query.Select{
				From: query.FromStmt{
					Stmt: &query.Select{
						From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t1")}},
					},
					Alias:         sql.ID("s1"),
					ColumnAliases: []sql.Identifier{sql.ID("c1")},
				},
			},
		},
		{
			sql: "select * from (select * from t1) as s1 (c1, c2)",
			stmt: query.Select{
				From: query.FromStmt{
					Stmt: &query.Select{
						From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t1")}},
					},
					Alias:         sql.ID("s1"),
					ColumnAliases: []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
				},
			},
		},
		{
			sql: "select * from (select * from t1) as s1 (c1, c2, c3)",
			stmt: query.Select{
				From: query.FromStmt{
					Stmt: &query.Select{
						From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t1")}},
					},
					Alias:         sql.ID("s1"),
					ColumnAliases: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
				},
			},
		},
		{sql: "select c where c > 5 from t", fail: true},
		{sql: "select c from t group", fail: true},
		{sql: "select c from t group by", fail: true},
		{sql: "select c from t group by c where c > 5", fail: true},
		{sql: "select c from t group by c having", fail: true},
		{sql: "select c from t group by c, d, having c > 5", fail: true},
		{
			sql: "select c from t group by c",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.ExprResult{Expr: expr.Ref{sql.ID("c")}},
				},
				GroupBy: []expr.Expr{expr.Ref{sql.ID("c")}},
			},
		},
		{
			sql: "select c from t group by c, d, e + f",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.ExprResult{Expr: expr.Ref{sql.ID("c")}},
				},
				GroupBy: []expr.Expr{expr.Ref{sql.ID("c")}, expr.Ref{sql.ID("d")},
					&expr.Binary{Op: expr.AddOp, Left: expr.Ref{sql.ID("e")},
						Right: expr.Ref{sql.ID("f")}},
				},
			},
		},
		{
			sql: "select c from t group by c having c > 1",
			stmt: query.Select{
				From: query.FromTableAlias{TableName: sql.TableName{Table: sql.ID("t")}},
				Results: []query.SelectResult{
					query.ExprResult{Expr: expr.Ref{sql.ID("c")}},
				},
				GroupBy: []expr.Expr{expr.Ref{sql.ID("c")}},
				Having: &expr.Binary{Op: expr.GreaterThanOp, Left: expr.Ref{sql.ID("c")},
					Right: expr.Int64Literal(1)},
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
			var trc string
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if ss, ok := ss.(*query.Select); !ok || !testutil.DeepEqual(&c.stmt, ss, &trc) {
				t.Errorf("Parse(%q) got %s want %s\n%s", c.sql, ss.String(), c.stmt.String(), trc)
			}
		}
	}
}

func TestValues(t *testing.T) {
	cases := []struct {
		sql  string
		stmt query.Values
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
			stmt: query.Values{
				Expressions: [][]expr.Expr{
					{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
				},
			},
		},
		{
			sql: "values (1, 'abc', true), (2, 'def', false)",
			stmt: query.Values{
				Expressions: [][]expr.Expr{
					{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
					{expr.Int64Literal(2), expr.StringLiteral("def"), expr.False()},
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
			var trc string
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if vs, ok := vs.(*query.Values); !ok || !testutil.DeepEqual(&c.stmt, vs, &trc) {
				t.Errorf("Parse(%q) got %s want %s: %s\n", c.sql, vs.String(), c.stmt.String(),
					trc)
			}
		}
	}
}

func TestDelete(t *testing.T) {
	cases := []struct {
		sql  string
		stmt query.Delete
		fail bool
	}{
		{sql: "delete", fail: true},
		{sql: "delete t", fail: true},
		{sql: "delete from", fail: true},
		{sql: "delete from t1, t2", fail: true},
		{sql: "delete from t where", fail: true},
		{
			sql: "delete from t",
			stmt: query.Delete{
				Table: sql.TableName{Table: sql.ID("t")},
			},
		},
		{
			sql: "delete from t where c > 1",
			stmt: query.Delete{
				Table: sql.TableName{Table: sql.ID("t")},
				Where: &expr.Binary{Op: expr.GreaterThanOp, Left: expr.Ref{sql.ID("c")},
					Right: expr.Int64Literal(1)},
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
			var trc string
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if ds, ok := ds.(*query.Delete); !ok || !testutil.DeepEqual(&c.stmt, ds, &trc) {
				t.Errorf("Parse(%q) got %s want %s\n%s", c.sql, ds.String(), c.stmt.String(), trc)
			}
		}
	}
}

func TestUpdate(t *testing.T) {
	cases := []struct {
		sql  string
		stmt query.Update
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
			stmt: query.Update{
				Table: sql.TableName{Table: sql.ID("t")},
				ColumnUpdates: []query.ColumnUpdate{
					{Column: sql.ID("c"), Expr: expr.Int64Literal(5)},
				},
			},
		},
		{
			sql: "update t set c = 0 where c > 1",
			stmt: query.Update{
				Table: sql.TableName{Table: sql.ID("t")},
				ColumnUpdates: []query.ColumnUpdate{
					{Column: sql.ID("c"), Expr: expr.Int64Literal(0)},
				},
				Where: &expr.Binary{Op: expr.GreaterThanOp, Left: expr.Ref{sql.ID("c")},
					Right: expr.Int64Literal(1)},
			},
		},
		{
			sql: "update t set c1 = 1, c2 = 2, c3 = 3",
			stmt: query.Update{
				Table: sql.TableName{Table: sql.ID("t")},
				ColumnUpdates: []query.ColumnUpdate{
					{Column: sql.ID("c1"), Expr: expr.Int64Literal(1)},
					{Column: sql.ID("c2"), Expr: expr.Int64Literal(2)},
					{Column: sql.ID("c3"), Expr: expr.Int64Literal(3)},
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
			var trc string
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else if us, ok := us.(*query.Update); !ok || !testutil.DeepEqual(&c.stmt, us, &trc) {
				t.Errorf("Parse(%q) got %s want %s\n%s", c.sql, us.String(), c.stmt.String(), trc)
			}
		}
	}
}

func TestCreateDatabase(t *testing.T) {
	cases := []struct {
		sql  string
		stmt datadef.CreateDatabase
		fail bool
	}{
		{sql: "create database", fail: true},
		{
			sql: "create database test",
			stmt: datadef.CreateDatabase{
				Database: sql.ID("test"),
			},
		},
		{sql: "create database test with", fail: true},
		{sql: "create database test with path", fail: true},
		{sql: "create database test with path = ", fail: true},
		{sql: "create database test with 'path' = value", fail: true},
		{sql: "create database test with create = value", fail: true},
		{sql: "create database test with path = 'string' engine", fail: true},
		{
			sql: "create database test with path = 'string' engine 'fast'",
			stmt: datadef.CreateDatabase{
				Database: sql.ID("test"),
				Options: map[sql.Identifier]string{
					sql.UnquotedID("path"):   "string",
					sql.UnquotedID("engine"): "fast",
				},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		cd, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.sql)
			}
		} else {
			var trc string
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.sql, err)
			} else {
				cd, ok := cd.(*datadef.CreateDatabase)
				if !ok || !testutil.DeepEqual(&c.stmt, cd, &trc) {
					t.Errorf("Parse(%q) got %s want %s\n%s", c.sql, cd.String(), c.stmt.String(),
						trc)
				}
			}
		}
	}
}
