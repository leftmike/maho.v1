package engine_test

import (
	"maho/engine"
	"maho/sql"
	"maho/sql/parser"
	"maho/store"
	"maho/store/test"
	"strings"
	"testing"
)

type insertCase struct {
	stmt string
	fail bool
	rows [][]sql.Value
}

var (
	insertColumns1 = []sql.Column{
		{Name: sql.Id("c1"), Type: sql.BooleanType, Size: 1},
		{Name: sql.Id("c2"), Type: sql.CharacterType, Size: 128},
		{Name: sql.Id("c3"), Type: sql.DoubleType, Size: 8, Width: 255, Fraction: 30},
		{Name: sql.Id("c4"), Type: sql.IntegerType, Size: 4, Width: 255},
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

	insertColumns2 = []sql.Column{
		{Name: sql.Id("b1"), Type: sql.BooleanType, Size: 1},
		{Name: sql.Id("b2"), Type: sql.BooleanType, Size: 1},
		{Name: sql.Id("b3"), Type: sql.BooleanType, Size: 1},
		{Name: sql.Id("b4"), Type: sql.BooleanType, Size: 1},
		{Name: sql.Id("b5"), Type: sql.BooleanType, Size: 1},
		{Name: sql.Id("b6"), Type: sql.BooleanType, Size: 1},
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

	insertColumns3 = []sql.Column{
		{Name: sql.Id("c1"), Type: sql.IntegerType, Size: 4, Width: 255, Default: int64(1)},
		{Name: sql.Id("c2"), Type: sql.IntegerType, Size: 4, Width: 255, NotNull: true},
		{Name: sql.Id("c3"), Type: sql.IntegerType, Size: 4, Width: 255, Default: int64(3),
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
	db, err := store.Open("test", "test_insert")
	if err != nil {
		t.Error(err)
	}
	e, err := engine.Start(db)
	if err != nil {
		t.Error(err)
	}

	testInsert(t, e, db, sql.Id("t"), insertColumns1, insertCases1)
	testInsert(t, e, db, sql.Id("t2"), insertColumns2, insertCases2)
	testInsert(t, e, db, sql.Id("t3"), insertColumns3, insertCases3)
}

func statement(e *engine.Engine, s string) error {
	var p parser.Parser
	p.Init(strings.NewReader(s), "statement")
	stmt, err := p.Parse()
	if err != nil {
		return err
	}
	_, err = stmt.Dispatch(e)
	return err
}

func testInsert(t *testing.T, e *engine.Engine, db store.Database, nam sql.Identifier,
	cols []sql.Column, cases []insertCase) {

	for _, c := range cases {
		err := db.CreateTable(nam, cols)
		if err != nil {
			t.Error(err)
			return
		}

		err = statement(e, c.stmt)
		if c.fail {
			if err == nil {
				t.Errorf("Parse(\"%s\").Dispatch() did not fail", c.stmt)
			}
		} else if err != nil {
			t.Errorf("Parse(\"%s\").Dispatch() failed with %s", c.stmt, err.Error())
		} else {
			tbl, err := db.Table(nam)
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

		err = db.DropTable(nam)
		if err != nil {
			t.Error(err)
			return
		}
	}
}