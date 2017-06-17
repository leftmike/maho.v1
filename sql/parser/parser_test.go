package parser_test

import (
	"fmt"
	"maho/sql"
	. "maho/sql/parser"
	"maho/sql/stmt"
	"math"
	"strings"
	"testing"
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
		var p Parser
		p.Init(strings.NewReader(f), fmt.Sprintf("failed[%d]", i))
		stmt, err := p.Parse()
		if stmt != nil {
			t.Errorf("parse: \"%s\": stmt != nil: %s", f, stmt)
		} else if err == nil {
			t.Errorf("parse: \"%s\": did not fail: ", f)
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
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{Name: sql.Id("c1"), Type: sql.IntegerType, Size: 1, Width: 255},
					{Name: sql.Id("c2"), Type: sql.IntegerType, Size: 2, Width: 255},
					{Name: sql.Id("c3"), Type: sql.IntegerType, Size: 3, Width: 255},
					{Name: sql.Id("c4"), Type: sql.IntegerType, Size: 4, Width: 255},
					{Name: sql.Id("c5"), Type: sql.IntegerType, Size: 8, Width: 255},
				},
			},
		},
		{
			sql: "create table t (c1 tinyint(1), c2 smallint(2), c3 mediumint(3), c4 integer(4))",
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{Name: sql.Id("c1"), Type: sql.IntegerType, Size: 1, Width: 1},
					{Name: sql.Id("c2"), Type: sql.IntegerType, Size: 2, Width: 2},
					{Name: sql.Id("c3"), Type: sql.IntegerType, Size: 3, Width: 3},
					{Name: sql.Id("c4"), Type: sql.IntegerType, Size: 4, Width: 4},
				},
			},
		},
		{
			sql: "create table t (b1 bool, b2 boolean, d1 double, d2 double)",
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{Name: sql.Id("b1"), Type: sql.BooleanType, Size: 1},
					{Name: sql.Id("b2"), Type: sql.BooleanType, Size: 1},
					{Name: sql.Id("d1"), Type: sql.DoubleType, Size: 8, Width: 255, Fraction: 30},
					{Name: sql.Id("d2"), Type: sql.DoubleType, Size: 8, Width: 255, Fraction: 30},
				},
			},
		},
		{
			sql: "create table t (d1 double(123,4), d2 double(12,3))",
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{Name: sql.Id("d1"), Type: sql.DoubleType, Size: 8, Width: 123, Fraction: 4},
					{Name: sql.Id("d2"), Type: sql.DoubleType, Size: 8, Width: 12, Fraction: 3},
				},
			},
		},
		{
			sql: "create table t (b1 binary, b2 varbinary(123), b3 blob)",
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{
						Name:   sql.Id("b1"),
						Type:   sql.CharacterType,
						Fixed:  true,
						Binary: true,
						Size:   1,
					},
					{
						Name:   sql.Id("b2"),
						Type:   sql.CharacterType,
						Fixed:  false,
						Binary: true,
						Size:   123,
					},
					{
						Name:   sql.Id("b3"),
						Type:   sql.CharacterType,
						Fixed:  false,
						Binary: true,
						Size:   math.MaxUint32 - 1,
					},
				},
			},
		},
		{
			sql: "create table t (b1 binary(123), b2 varbinary(456), b3 blob(789))",
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{
						Name:   sql.Id("b1"),
						Type:   sql.CharacterType,
						Fixed:  true,
						Binary: true,
						Size:   123,
					},
					{
						Name:   sql.Id("b2"),
						Type:   sql.CharacterType,
						Fixed:  false,
						Binary: true,
						Size:   456,
					},
					{
						Name:   sql.Id("b3"),
						Type:   sql.CharacterType,
						Fixed:  false,
						Binary: true,
						Size:   789,
					},
				},
			},
		},
		{
			sql: "create table t (c1 char, c2 varchar(123), c3 text)",
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{Name: sql.Id("c1"), Type: sql.CharacterType, Fixed: true, Size: 1},
					{Name: sql.Id("c2"), Type: sql.CharacterType, Fixed: false, Size: 123},
					{
						Name:  sql.Id("c3"),
						Type:  sql.CharacterType,
						Fixed: false,
						Size:  math.MaxUint32 - 1,
					},
				},
			},
		},
		{
			sql: "create table t (c1 char(123), c2 varchar(456), c3 text(789))",
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{Name: sql.Id("c1"), Type: sql.CharacterType, Fixed: true, Size: 123},
					{Name: sql.Id("c2"), Type: sql.CharacterType, Fixed: false, Size: 456},
					{Name: sql.Id("c3"), Type: sql.CharacterType, Fixed: false, Size: 789},
				},
			},
		},
		{
			sql: "create table t (b1 char binary, b2 varchar(123) binary, b3 text binary)",
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{
						Name:   sql.Id("b1"),
						Type:   sql.CharacterType,
						Fixed:  true,
						Binary: true,
						Size:   1,
					},
					{
						Name:   sql.Id("b2"),
						Type:   sql.CharacterType,
						Fixed:  false,
						Binary: true,
						Size:   123,
					},
					{
						Name:   sql.Id("b3"),
						Type:   sql.CharacterType,
						Fixed:  false,
						Binary: true,
						Size:   math.MaxUint32 - 1,
					},
				},
			},
		},
		{
			sql: "create table t (c1 varchar(64) default 'abcd', c2 int default 123)",
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{Name: sql.Id("c1"), Type: sql.CharacterType, Fixed: false, Size: 64,
						Default: sql.Value("abcd")},
					{Name: sql.Id("c2"), Type: sql.IntegerType, Size: 4, Width: 255,
						Default: sql.Value(int64(123))},
				},
			},
		},
		{
			sql: "create table t (c1 boolean default true, c2 boolean not null)",
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{Name: sql.Id("c1"), Type: sql.BooleanType, Size: 1, Default: sql.Value(true)},
					{Name: sql.Id("c2"), Type: sql.BooleanType, Size: 1, NotNull: true},
				},
			},
		},
		{
			sql: `create table t (c1 boolean default true not null,
c2 boolean not null default true)`,
			stmt: stmt.CreateTable{
				Table: sql.Id("t"),
				Columns: []sql.Column{
					{Name: sql.Id("c1"), Type: sql.BooleanType, Size: 1, Default: sql.Value(true),
						NotNull: true},
					{Name: sql.Id("c2"), Type: sql.BooleanType, Size: 1, Default: sql.Value(true),
						NotNull: true},
				},
			},
		},
	}

	for i, c := range cases {
		var p Parser
		p.Init(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		stmt, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("parse: \"%s\": did not fail", c.sql)
			}
		} else {
			if err != nil {
				t.Errorf("parse: \"%s\": failed: %s", c.sql, err)
			} else {
				checkCreateTableStmts(t, c.sql, stmt, c.stmt)
			}
		}
	}
}

func checkCreateTableStmts(t *testing.T, s string, stmt1 stmt.Stmt, create2 stmt.CreateTable) {
	create1, ok := stmt1.(*stmt.CreateTable)
	if !ok {
		t.Errorf("parse: \"%s\": not a stmt.CreateTable: %s", s, stmt1)
		return
	}
	if create1.Database != create2.Database {
		t.Errorf("parse: \"%s\": database: %s != %s", s, create1.Database, create2.Database)
	}
	if create1.Table != create2.Table {
		t.Errorf("parse: \"%s\": table: %s != %s", s, create1.Table, create2.Table)
	}

	if len(create1.Columns) != len(create2.Columns) {
		t.Errorf("parse: \"%s\": len(columns): %d != %d", s, len(create1.Columns),
			len(create2.Columns))
	}
	for i, c1 := range create1.Columns {
		if c1 != create2.Columns[i] {
			t.Errorf("parse: \"%s\": column[%d]: %v != %v", s, i, c1, create2.Columns[i])
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
				InsertInto: stmt.InsertInto{
					Table: sql.Id("t"),
				},
				Rows: [][]sql.Value{{int64(1), "abc", true}},
			},
		},
		{
			sql: "insert into t values (1, 'abc', true), (2, 'def', false)",
			stmt: stmt.InsertValues{
				InsertInto: stmt.InsertInto{
					Table: sql.Id("t"),
				},
				Rows: [][]sql.Value{{int64(1), "abc", true}, {int64(2), "def", false}},
			},
		},
	}

	for i, c := range cases {
		var p Parser
		p.Init(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		stmt, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("parse: \"%s\": did not fail", c.sql)
			}
		} else {
			if err != nil {
				t.Errorf("parse: \"%s\": failed: %s", c.sql, err)
			} else {
				checkInsertValuesStmts(t, c.sql, stmt, c.stmt)
			}
		}
	}
}

func checkInsertValuesStmts(t *testing.T, s string, stmt1 stmt.Stmt, insert2 stmt.InsertValues) {
	insert1, ok := stmt1.(*stmt.InsertValues)
	if !ok {
		t.Errorf("parse: \"%s\": not a stmt.InsertValues: %s", s, stmt1)
		return
	}
	if insert1.Database != insert2.Database {
		t.Errorf("parse: \"%s\": database: %s != %s", s, insert1.Database, insert2.Database)
	}
	if insert1.Table != insert2.Table {
		t.Errorf("parse: \"%s\": table: %s != %s", s, insert1.Table, insert2.Table)
	}

	if len(insert1.Columns) != len(insert2.Columns) {
		t.Errorf("parse: \"%s\": len(columns): %d != %d", s, len(insert1.Columns),
			len(insert2.Columns))
	}
	for i, c1 := range insert1.Columns {
		if c1 != insert2.Columns[i] {
			t.Errorf("parse: \"%s\": column[%d]: %v != %v", s, i, c1, insert2.Columns[i])
		}
	}

	if len(insert1.Rows) != len(insert2.Rows) {
		t.Errorf("parse: \"%s\": len(rows): %d != %d", s, len(insert1.Rows),
			len(insert2.Rows))
	}
	for i, r1 := range insert1.Rows {
		r2 := insert2.Rows[i]
		if len(r1) != len(r2) {
			t.Errorf("parse: \"%s\": len(row[%d]): %d != %d", s, i, len(r1), len(r2))
		}
		for j, v1 := range r1 {
			if v1 != r2[j] {
				t.Errorf("parse: \"%s\": row[%d][%d]: %v != %v", s, i, j, v1, r2[j])
			}
		}
	}
}
