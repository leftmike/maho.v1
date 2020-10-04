package engine_test

import (
	"testing"

	"github.com/leftmike/maho/sql"
)

func TestTriggers(t *testing.T) {
	e := startEngine(t, sql.ID("db"))
	tn := sql.TableName{sql.ID("db"), sql.PUBLIC, sql.ID("tbl1")}
	createTable(t, e.Begin(0), tn)
	insertRows(t, e.Begin(0), tn, 1, 8)
	deleteIndexRow(t, e.Begin(0), tn, 0, strVal("6"))
	deleteIndexRow(t, e.Begin(0), tn, 1, i64Val(2))
	updateIndexRow(t, e.Begin(0), tn, 0, strVal("5"), []sql.ColumnUpdate{{3, i64Val(30)}})
	updateIndexRow(t, e.Begin(0), tn, 1, i64Val(2), []sql.ColumnUpdate{{1, strVal("6")}})

	indexRows(t, e.Begin(0), tn, 0,
		[][]sql.Value{
			{strVal("1"), i64Val(8)},
			{strVal("2"), i64Val(7)},
			{strVal("5"), i64Val(4)},
			{strVal("6"), i64Val(6)},
			{strVal("7"), i64Val(2)},
			{strVal("8"), i64Val(1)},
		},
		[][]sql.Value{
			{i64Val(8), strVal("1"), i64Val(3), i64Val(7)},
			{i64Val(7), strVal("2"), i64Val(3), i64Val(6)},
			{i64Val(4), strVal("5"), i64Val(1), i64Val(30)},
			{i64Val(6), strVal("6"), i64Val(2), i64Val(5)},
			{i64Val(2), strVal("7"), i64Val(0), i64Val(1)},
			{i64Val(1), strVal("8"), i64Val(0), i64Val(0)},
		})

}
