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
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []sql.ColumnKey
	indexes     []sql.IndexType
}

func (tt tableType) Columns() []sql.Identifier {
	return tt.columns
}

func (tt tableType) ColumnTypes() []sql.ColumnType {
	return tt.columnTypes
}

func (tt tableType) PrimaryKey() []sql.ColumnKey {
	return tt.primary
}

func (tt tableType) Indexes() []sql.IndexType {
	return tt.indexes
}

func TestFKPrepare(t *testing.T) {
	tt1 := tableType{
		columns: []sql.Identifier{sql.ID("a1"), sql.ID("b1"), sql.ID("c1"), sql.ID("d1")},
		columnTypes: []sql.ColumnType{
			sql.Int64ColType,
			sql.Int64ColType,
			sql.StringColType,
			sql.StringColType,
		},
		primary: []sql.ColumnKey{sql.MakeColumnKey(0, false)},
		indexes: []sql.IndexType{},
	}

	tt2 := tableType{
		columns: []sql.Identifier{sql.ID("a2"), sql.ID("b2"), sql.ID("c2"), sql.ID("d2")},
		columnTypes: []sql.ColumnType{
			sql.StringColType,
			sql.StringColType,
			sql.Int64ColType,
			sql.Int64ColType,
		},
		primary: []sql.ColumnKey{sql.MakeColumnKey(3, false)},
		indexes: []sql.IndexType{},
	}

	tn1 := sql.TableName{Database: sql.ID("db"), Schema: sql.ID("sc"), Table: sql.ID("otbl")}
	tn2 := sql.TableName{Database: sql.ID("db"), Schema: sql.ID("sc"), Table: sql.ID("itbl")}

	cases := []struct {
		fln  testutil.FileLineNumber
		sql  string
		fk   datadef.ForeignKey
		ott  sql.TableType
		itt  sql.TableType
		ofkr sql.OutgoingFKRef
		ifkr sql.IncomingFKRef
		fail bool
	}{
		{
			fln: fln(),
			sql: "CONSTRAINT fk_1 FOREIGN KEY (c1) REFERENCES db.sc.itbl",
			fk: datadef.ForeignKey{
				Name:          sql.ID("fk_1"),
				OutgoingTable: tn1,
				OutgoingCols:  []sql.Identifier{sql.ID("c1")},
				IncomingTable: tn2,
			},
			ott: &tt1,
			itt: &tt2,
			ofkr: sql.OutgoingFKRef{
				Name:    sql.ID("fk_1"),
				Columns: []int{2},
				Table:   tn2,
				Index:   0,
			},
			ifkr: sql.IncomingFKRef{
				Name:         sql.ID("fk_1"),
				OutgoingCols: []int{2},
				Table:        tn1,
				IncomingCols: []int{3},
			},
		},
	}

	for _, c := range cases {
		var ofkr sql.OutgoingFKRef
		var ifkr sql.IncomingFKRef
		err := c.fk.Prepare(c.ott, c.itt, &ofkr, &ifkr)
		if c.fail {
			if err == nil {
				t.Errorf("%sPrepare did not fail", c.fln)
			}
			continue
		} else if err != nil {
			t.Errorf("%sPrepare failed with %s", c.fln, err)
		}
		if !reflect.DeepEqual(c.ofkr, ofkr) {
			t.Errorf("%sPrepare outgoing: got %#v want %#v", c.fln, ofkr, c.ofkr)
		}
		if !reflect.DeepEqual(c.ifkr, ifkr) {
			t.Errorf("%sPrepare incoming: got %#v want %#v", c.fln, ifkr, c.ifkr)
		}

		s := c.fk.String()
		if s != c.sql {
			t.Errorf("%sString: got %s want %s", c.fln, s, c.sql)
		}
	}
}
