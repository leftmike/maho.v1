package engine_test

import (
	"maho/engine"
	"maho/sql"
	"maho/sql/parser"
	"maho/sql/stmt"
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
	insertCreate1 string       = "create table t (c1 bool, c2 varchar(128), c3 double, c4 int)"
	insertCases1  []insertCase = []insertCase{
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

	insertCreate2 string = `create table t2 (b1 bool, b2 bool, b3 bool, b4 bool, b5 bool,
b6 bool)`
	insertCases2 []insertCase = []insertCase{
		{
			stmt: "insert into t2 values ('t', 'true', 'y', 'yes', 'on', '1')",
			rows: [][]sql.Value{{true, true, true, true, true, true}},
		},
		{
			stmt: "insert into t2 values ('f', 'false', 'n', 'no', 'off', '0')",
			rows: [][]sql.Value{{false, false, false, false, false, false}},
		},
	}

	insertCreate3 string = `create table t3 (c1 int default 1, c2 int not null,
c3 int default 3 not null)`
	insertCases3 []insertCase = []insertCase{
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
	s, err := test.Make(sql.Id("test_insert"))
	if err != nil {
		t.Error(err)
	}
	e, err := engine.Start(s)
	if err != nil {
		t.Error(err)
	}

	testInsert(t, e, sql.Id("t"), insertCreate1, insertCases1)
	testInsert(t, e, sql.Id("t2"), insertCreate2, insertCases2)
	testInsert(t, e, sql.Id("t3"), insertCreate3, insertCases3)
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

func testInsert(t *testing.T, e *engine.Engine, tbl sql.Identifier, ctbl string,
	cases []insertCase) {
	err := statement(e, ctbl)
	if err != nil {
		t.Error(err)
		return
	}

	for _, c := range cases {
		err = statement(e, c.stmt)
		if c.fail {
			if err == nil {
				t.Errorf("engine: \"%s\": did not fail", c.stmt)
			}
		} else if err != nil {
			t.Errorf("%s: \"%s\"", err.Error(), c.stmt)
		}
	}

	rows, err := (&stmt.Select{Table: tbl}).Dispatch(e)
	if err != nil {
		t.Error(err)
	} else if rows, ok := rows.(store.Rows); ok {
		dest := make([]sql.Value, len(rows.Columns()))
		for _, c := range cases {
			if c.fail {
				continue
			}
			for _, r := range c.rows {
				if rows.Next(dest) != nil {
					t.Error("engine: expected more rows")
					break
				}
				if len(r) != len(dest) {
					t.Errorf("engine: len(r) != len(dest): %d != %d", len(r), len(dest))
					break
				}
				for i, v := range r {
					if v != dest[i] {
						t.Errorf("engine: \"%s\": row[%d]: %v != %v", c.stmt, i, v, dest[i])
					}
				}
			}
		}
		if rows.Next(dest) == nil {
			t.Errorf("engine: too many rows")
		}
	} else {
		t.Errorf("engine: unable to convert select result to rows")
	}
}
