package datadef_test

import (
	"reflect"
	"testing"

	"github.com/leftmike/maho/evaluate/datadef"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

func fln() testutil.FileLineNumber {
	return testutil.MakeFileLineNumber()
}

type tableType struct {
	cols     []sql.Identifier
	colTypes []sql.ColumnType
	primary  []sql.ColumnKey
	indexes  []sql.IndexType
}

func (tt tableType) Version() int64 {
	return 0
}

func (tt tableType) Columns() []sql.Identifier {
	return tt.cols
}

func (tt tableType) ColumnTypes() []sql.ColumnType {
	return tt.colTypes
}

func (tt tableType) PrimaryKey() []sql.ColumnKey {
	return tt.primary
}

func (tt tableType) Indexes() []sql.IndexType {
	return tt.indexes
}

func colKey(col int) sql.ColumnKey {
	return sql.MakeColumnKey(col, false)
}

func TestFKPrepare(t *testing.T) {
	tt1 := tableType{
		cols: []sql.Identifier{sql.ID("a1"), sql.ID("b1"), sql.ID("c1"), sql.ID("d1")},
		colTypes: []sql.ColumnType{
			sql.Int64ColType,
			sql.Int64ColType,
			sql.StringColType,
			sql.StringColType,
		},
		primary: []sql.ColumnKey{colKey(0), colKey(2)},
		indexes: []sql.IndexType{
			{
				Name:    sql.ID("idx1"),
				Key:     []sql.ColumnKey{colKey(0), colKey(1), colKey(2)},
				Columns: []int{0, 1, 2, 3},
				Unique:  false,
			},
			{
				Name:    sql.ID("idx2"),
				Key:     []sql.ColumnKey{colKey(0), colKey(1), colKey(3)},
				Columns: []int{0, 1, 2, 3},
				Unique:  true,
			},
			{
				Name:    sql.ID("idx3"),
				Key:     []sql.ColumnKey{colKey(1), colKey(2), colKey(3)},
				Columns: []int{0, 1, 2, 3},
				Unique:  true,
			},
			{
				Name:    sql.ID("idx4"),
				Key:     []sql.ColumnKey{colKey(0), colKey(1)},
				Columns: []int{0, 1, 2, 3},
				Unique:  true,
			},
		},
	}

	tt2 := tableType{
		cols: []sql.Identifier{sql.ID("a2"), sql.ID("b2"), sql.ID("c2"), sql.ID("d2")},
		colTypes: []sql.ColumnType{
			sql.Int64ColType,
			sql.StringColType,
			sql.Int64ColType,
			sql.StringColType,
		},
		primary: []sql.ColumnKey{colKey(3)},
		indexes: []sql.IndexType{},
	}

	tt3 := tableType{
		cols: []sql.Identifier{sql.ID("a3"), sql.ID("b3"), sql.ID("c3"), sql.ID("d3")},
		colTypes: []sql.ColumnType{
			sql.Int64ColType,
			sql.Int64ColType,
			sql.StringColType,
			sql.StringColType,
		},
	}

	tn1 := sql.TableName{Database: sql.ID("db"), Schema: sql.ID("sc"), Table: sql.ID("tbl1")}
	tn2 := sql.TableName{Database: sql.ID("db"), Schema: sql.ID("sc"), Table: sql.ID("tbl2")}
	tn3 := sql.TableName{Database: sql.ID("db"), Schema: sql.ID("sc"), Table: sql.ID("tbl3")}

	cases := []struct {
		fln    testutil.FileLineNumber
		sql    string
		fk     datadef.ForeignKey
		fktt   sql.TableType
		rtt    sql.TableType
		fkCols []int
		ridx   sql.Identifier
		fail   bool
	}{
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (c1) REFERENCES db.sc.tbl2",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn1,
				FKCols:   []sql.Identifier{sql.ID("c1")},
				RefTable: tn2,
			},
			fktt:   &tt1,
			rtt:    &tt2,
			fkCols: []int{2},
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (c1) REFERENCES db.sc.tbl2 (d2)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn1,
				FKCols:   []sql.Identifier{sql.ID("c1")},
				RefTable: tn2,
				RefCols:  []sql.Identifier{sql.ID("d2")},
			},
			fktt:   &tt1,
			rtt:    &tt2,
			fkCols: []int{2},
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (c1, d1) REFERENCES db.sc.tbl2",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn1,
				FKCols:   []sql.Identifier{sql.ID("c1"), sql.ID("d1")},
				RefTable: tn2,
			},
			fktt: &tt1,
			rtt:  &tt2,
			fail: true,
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (z1) REFERENCES db.sc.tbl2",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn1,
				FKCols:   []sql.Identifier{sql.ID("z1")},
				RefTable: tn2,
			},
			fktt: &tt1,
			rtt:  &tt2,
			fail: true,
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (a1) REFERENCES db.sc.tbl2",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn1,
				FKCols:   []sql.Identifier{sql.ID("a1")},
				RefTable: tn2,
			},
			fktt: &tt1,
			rtt:  &tt2,
			fail: true,
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (c2, b2) REFERENCES db.sc.tbl1",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn2,
				FKCols:   []sql.Identifier{sql.ID("c2"), sql.ID("b2")},
				RefTable: tn1,
			},
			fktt:   &tt2,
			rtt:    &tt1,
			fkCols: []int{2, 1},
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (c2, b2) REFERENCES db.sc.tbl1 (a1, c1)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn2,
				FKCols:   []sql.Identifier{sql.ID("c2"), sql.ID("b2")},
				RefTable: tn1,
				RefCols:  []sql.Identifier{sql.ID("a1"), sql.ID("c1")},
			},
			fktt:   &tt2,
			rtt:    &tt1,
			fkCols: []int{2, 1},
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (b2, c2) REFERENCES db.sc.tbl1 (c1, a1)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn2,
				FKCols:   []sql.Identifier{sql.ID("b2"), sql.ID("c2")},
				RefTable: tn1,
				RefCols:  []sql.Identifier{sql.ID("c1"), sql.ID("a1")},
			},
			fktt:   &tt2,
			rtt:    &tt1,
			fkCols: []int{2, 1},
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (a2, c2) REFERENCES db.sc.tbl1 (c1, a1)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn2,
				FKCols:   []sql.Identifier{sql.ID("a2"), sql.ID("c2")},
				RefTable: tn1,
				RefCols:  []sql.Identifier{sql.ID("c1"), sql.ID("a1")},
			},
			fktt: &tt2,
			rtt:  &tt1,
			fail: true,
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (a2, b2, c2) REFERENCES db.sc.tbl1",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn2,
				FKCols:   []sql.Identifier{sql.ID("a2"), sql.ID("b2"), sql.ID("c2")},
				RefTable: tn1,
			},
			fktt: &tt2,
			rtt:  &tt1,
			fail: true,
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (a3, b3, d3) REFERENCES db.sc.tbl1 (a1, b1, d1)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn3,
				FKCols:   []sql.Identifier{sql.ID("a3"), sql.ID("b3"), sql.ID("d3")},
				RefTable: tn1,
				RefCols:  []sql.Identifier{sql.ID("a1"), sql.ID("b1"), sql.ID("d1")},
			},
			fktt:   &tt3,
			rtt:    &tt1,
			fkCols: []int{0, 1, 3},
			ridx:   sql.ID("idx2"),
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (a3, b3, d3) REFERENCES db.sc.tbl1 (b1, a1, d1)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn3,
				FKCols:   []sql.Identifier{sql.ID("a3"), sql.ID("b3"), sql.ID("d3")},
				RefTable: tn1,
				RefCols:  []sql.Identifier{sql.ID("b1"), sql.ID("a1"), sql.ID("d1")},
			},
			fktt:   &tt3,
			rtt:    &tt1,
			fkCols: []int{1, 0, 3},
			ridx:   sql.ID("idx2"),
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (b3, d3, a3) REFERENCES db.sc.tbl1 (b1, d1, a1)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn3,
				FKCols:   []sql.Identifier{sql.ID("b3"), sql.ID("d3"), sql.ID("a3")},
				RefTable: tn1,
				RefCols:  []sql.Identifier{sql.ID("b1"), sql.ID("d1"), sql.ID("a1")},
			},
			fktt:   &tt3,
			rtt:    &tt1,
			fkCols: []int{0, 1, 3},
			ridx:   sql.ID("idx2"),
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (b3, a3, d3) REFERENCES db.sc.tbl1 (b1, d1, a1)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn3,
				FKCols:   []sql.Identifier{sql.ID("b3"), sql.ID("a3"), sql.ID("d3")},
				RefTable: tn1,
				RefCols:  []sql.Identifier{sql.ID("b1"), sql.ID("d1"), sql.ID("a1")},
			},
			fktt: &tt3,
			rtt:  &tt1,
			fail: true,
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (a3, b3, d3) REFERENCES db.sc.tbl1 (a1, b1, c1)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn3,
				FKCols:   []sql.Identifier{sql.ID("a3"), sql.ID("b3"), sql.ID("d3")},
				RefTable: tn1,
				RefCols:  []sql.Identifier{sql.ID("a1"), sql.ID("b1"), sql.ID("c1")},
			},
			fktt: &tt3,
			rtt:  &tt1,
			fail: true,
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (b3, c3, d3) REFERENCES db.sc.tbl1 (b1, c1, d1)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn3,
				FKCols:   []sql.Identifier{sql.ID("b3"), sql.ID("c3"), sql.ID("d3")},
				RefTable: tn1,
				RefCols:  []sql.Identifier{sql.ID("b1"), sql.ID("c1"), sql.ID("d1")},
			},
			fktt:   &tt3,
			rtt:    &tt1,
			fkCols: []int{1, 2, 3},
			ridx:   sql.ID("idx3"),
		},
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (c3, d3) REFERENCES db.sc.tbl1 (c1, d1)",
			fk: datadef.ForeignKey{
				Name:     sql.ID("fk_1"),
				FKTable:  tn3,
				FKCols:   []sql.Identifier{sql.ID("c3"), sql.ID("d3")},
				RefTable: tn1,
				RefCols:  []sql.Identifier{sql.ID("c1"), sql.ID("d1")},
			},
			fktt: &tt3,
			rtt:  &tt1,
			fail: true,
		},
	}

	for _, c := range cases {
		s := c.fk.String()
		if s != c.sql {
			t.Errorf("%sString: got %s want %s", c.fln, s, c.sql)
		}

		fkCols, ridx, err := c.fk.Prepare(c.fktt, c.rtt)
		if c.fail {
			if err == nil {
				t.Errorf("%sPrepare did not fail", c.fln)
			}
			continue
		} else if err != nil {
			t.Errorf("%sPrepare failed with %s", c.fln, err)
		}
		if !reflect.DeepEqual(c.fkCols, fkCols) {
			t.Errorf("%sPrepare foreign key cols: got %#v want %#v", c.fln, fkCols, c.fkCols)
		}
		if ridx != c.ridx {
			t.Errorf("%sPrepare ref index: got %s want %s", c.fln, ridx, c.ridx)
		}
	}
}
