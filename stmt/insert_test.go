package stmt_test

import (
	"strings"
	"testing"

	"maho/db"
	"maho/engine"
	"maho/expr"
	"maho/parser"
	"maho/sql"
	"maho/store"
	"maho/store/test"
)

type insertCase struct {
	stmt string
	fail bool
	rows [][]sql.Value
}

var (
	insertColumns1     = []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")}
	insertColumnTypes1 = []db.ColumnType{
		{Type: sql.BooleanType, Size: 1},
		{Type: sql.CharacterType, Size: 128},
		{Type: sql.DoubleType, Size: 8, Width: 255, Fraction: 30},
		{Type: sql.IntegerType, Size: 4, Width: 255},
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
			rows: [][]sql.Value{{true, "abcd", 123.456, int64(789)}},
		},
		{
			stmt: "insert into t (c4, c1) values (123, false), (456)",
			rows: [][]sql.Value{{false, nil, nil, int64(123)}, {nil, nil, nil, int64(456)}},
		},
		{
			stmt: "insert into t (c3, c2, c1, c4) values (987.654, 'efghi', false, 321)",
			rows: [][]sql.Value{{false, "efghi", 987.654, int64(321)}},
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
			rows: [][]sql.Value{{nil, "123", nil, nil}},
		},
		{
			stmt: "insert into t (c2) values (123.456)",
			rows: [][]sql.Value{{nil, "123.456", nil, nil}},
		},
		{
			stmt: "insert into t (c3) values (true)",
			fail: true,
		},
		{
			stmt: "insert into t (c3) values ('   123   ')",
			rows: [][]sql.Value{{nil, nil, float64(123), nil}},
		},
		{
			stmt: "insert into t (c3) values ('123.456')",
			rows: [][]sql.Value{{nil, nil, 123.456, nil}},
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
			rows: [][]sql.Value{{nil, nil, nil, int64(123)}},
		},
		{
			stmt: "insert into t (c4) values (123.456)",
			rows: [][]sql.Value{{nil, nil, nil, int64(123)}},
		},
		{
			stmt: "insert into t (c4) values ('123b')",
			fail: true,
		},
	}

	insertColumns2 = []sql.Identifier{sql.ID("b1"), sql.ID("b2"), sql.ID("b3"), sql.ID("b4"),
		sql.ID("b5"), sql.ID("b6")}
	insertColumnTypes2 = []db.ColumnType{
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
			rows: [][]sql.Value{{true, true, true, true, true, true}},
		},
		{
			stmt: "insert into t2 values ('f', 'false', 'n', 'no', 'off', '0')",
			rows: [][]sql.Value{{false, false, false, false, false, false}},
		},
	}

	insertColumns3     = []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")}
	insertColumnTypes3 = []db.ColumnType{
		{Type: sql.IntegerType, Size: 4, Width: 255, Default: &expr.Literal{int64(1)}},
		{Type: sql.IntegerType, Size: 4, Width: 255, NotNull: true},
		{Type: sql.IntegerType, Size: 4, Width: 255, Default: &expr.Literal{int64(3)},
			NotNull: true},
	}
	insertCases3 = []insertCase{
		{
			stmt: "insert into t3 values (DEFAULT)",
			fail: true,
		},
		{
			stmt: "insert into t3 (c2) values (2)",
			rows: [][]sql.Value{{int64(1), int64(2), int64(3)}},
		},
		{
			stmt: "insert into t3 (c1, c2) values (NULL, 2)",
			rows: [][]sql.Value{{nil, int64(2), int64(3)}},
		},
		{
			stmt: "insert into t3 (c1, c2, c3) values (1, 2, NULL)",
			fail: true,
		},
	}
)

func TestInsert(t *testing.T) {
	dbase, err := store.Open("test", "test_insert")
	if err != nil {
		t.Error(err)
	}
	e, err := engine.Start(dbase)
	if err != nil {
		t.Error(err)
	}

	testInsert(t, e, dbase.(db.DatabaseModify), sql.ID("t"), insertColumns1, insertColumnTypes1,
		insertCases1)
	testInsert(t, e, dbase.(db.DatabaseModify), sql.ID("t2"), insertColumns2, insertColumnTypes2,
		insertCases2)
	testInsert(t, e, dbase.(db.DatabaseModify), sql.ID("t3"), insertColumns3, insertColumnTypes3,
		insertCases3)
}

func statement(e *engine.Engine, s string) error {
	p := parser.NewParser(strings.NewReader(s), "statement")
	stmt, err := p.Parse()
	if err != nil {
		return err
	}
	_, err = stmt.Execute(e)
	return err
}

func testInsert(t *testing.T, e *engine.Engine, dbase db.DatabaseModify, nam sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType, cases []insertCase) {

	for _, c := range cases {
		err := dbase.CreateTable(nam, cols, colTypes)
		if err != nil {
			t.Error(err)
			return
		}

		err = statement(e, c.stmt)
		if c.fail {
			if err == nil {
				t.Errorf("Parse(\"%s\").Execute() did not fail", c.stmt)
			}
		} else if err != nil {
			t.Errorf("Parse(\"%s\").Execute() failed with %s", c.stmt, err.Error())
		} else {
			tbl, err := dbase.Table(nam)
			if err != nil {
				t.Error(err)
				return
			}

			all := tbl.(test.AllRows).AllRows()
			if len(all) != len(c.rows) {
				t.Errorf("len(%s.Rows()) got %d want %d", nam, len(all), len(c.rows))
			} else {
				for i, r := range c.rows {
					if len(all[i]) != len(r) {
						t.Errorf("len(%s.Rows()[%d]) got %d want %d", nam, i, len(all[i]), len(r))
					} else {
						for j, v := range r {
							if all[i][j] != v {
								t.Errorf("%s.Rows()[%d][%d] got %v want %v", nam, i, j, all[i][j],
									v)
							}
						}
					}
				}
			}
		}

		err = dbase.DropTable(nam)
		if err != nil {
			t.Error(err)
			return
		}
	}
}
