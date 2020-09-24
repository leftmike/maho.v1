package test

import (
	"strings"
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/testutil"
)

var (
	minMaxRows   = makeValues(100)
	createMinMax = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-b"),
			cols: []sql.Identifier{sql.ID("ID"), sql.ID("col2"), sql.ID("col3"),
				sql.ID("col4"), sql.ID("col5")},
			colTypes: []sql.ColumnType{int32ColType, int64ColType, stringColType, int64ColType,
				stringColType},
			key: []sql.ColumnKey{sql.MakeColumnKey(0, false)},
		},
		{fln: fln(), cmd: cmdAddIndex, name: sql.ID("tbl-b"), idxname: sql.ID("idx-1"),
			key: []sql.ColumnKey{sql.MakeColumnKey(1, true)}, unique: true},
		{fln: fln(), cmd: cmdAddIndex, name: sql.ID("tbl-b"), idxname: sql.ID("idx-2"),
			key: []sql.ColumnKey{sql.MakeColumnKey(2, false), sql.MakeColumnKey(3, false)}},
		{fln: fln(), cmd: cmdAddIndex, name: sql.ID("tbl-b"), idxname: sql.ID("idx-3"),
			key: []sql.ColumnKey{sql.MakeColumnKey(4, false)}},
		{fln: fln(), cmd: cmdCommit},
	}
	checkMinMaxRows = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
		{fln: fln(), cmd: cmdRows, values: minMaxRows},
		{fln: fln(), cmd: cmdCommit},
	}

	primaryMinMaxTests = []interface{}{
		"createDatabase",

		createMinMax,
		insertRows(minMaxRows),
		checkMinMaxRows,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: nil,
				maxRow: []sql.Value{i64Val(1), nil, nil, nil},
				values: nil},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(0), nil, nil, nil},
				maxRow: []sql.Value{i64Val(1), nil, nil, nil},
				values: nil},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(0), nil, nil, nil},
				maxRow: []sql.Value{i64Val(2), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(2), i64Val(5), strVal(`""`), i64Val(1), strVal("yy")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(0), nil, nil, nil},
				maxRow: []sql.Value{i64Val(3), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(2), i64Val(5), strVal(`""`), i64Val(1), strVal("yy")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(0), nil, nil, nil},
				maxRow: []sql.Value{i64Val(4), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(2), i64Val(5), strVal(`""`), i64Val(1), strVal("yy")},
					{i64Val(4), i64Val(10), strVal("###"), i64Val(2), strVal("xxx")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(13), nil, nil, nil},
				maxRow: []sql.Value{i64Val(13), nil, nil, nil},
				values: nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(25), nil, nil, nil},
				maxRow: []sql.Value{i64Val(26), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(26), i64Val(65), strVal("......"), i64Val(3),
						strVal("mmmmmmmmmmmmmm")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(30), nil, nil, nil},
				maxRow: []sql.Value{i64Val(30), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(30), i64Val(75), strVal("00000000"), i64Val(5),
						strVal("kkkkkkkkkkkkkkkk")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(31), nil, nil, nil},
				maxRow: []sql.Value{i64Val(33), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(32), i64Val(80), strVal("1"), i64Val(6), strVal("z")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(31), nil, nil, nil},
				maxRow: []sql.Value{i64Val(37), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(32), i64Val(80), strVal("1"), i64Val(6), strVal("z")},
					{i64Val(34), i64Val(85), strVal("22"), i64Val(7), strVal("yy")},
					{i64Val(36), i64Val(90), strVal("333"), i64Val(8), strVal("xxx")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(40), nil, nil, nil},
				maxRow: []sql.Value{i64Val(41), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(40), i64Val(100), strVal("55555"), i64Val(0), strVal("vvvvv")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(50), nil, nil, nil},
				maxRow: []sql.Value{i64Val(59), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(50), i64Val(125), strVal("::"), i64Val(5), strVal("qqqqqqqqqq")},
					{i64Val(52), i64Val(130), strVal(";;;"), i64Val(6), strVal("ppppppppppp")},
					{i64Val(54), i64Val(135), strVal("<<<<"), i64Val(7), strVal("oooooooooooo")},
					{i64Val(56), i64Val(140), strVal("====="), i64Val(8), strVal("nnnnnnnnnnnnn")},
					{i64Val(58), i64Val(145), strVal(">>>>>>"), i64Val(9),
						strVal("mmmmmmmmmmmmmm")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(197), nil, nil, nil},
				maxRow: nil,
				values: [][]sql.Value{
					{i64Val(198), i64Val(495), strVal("****"), i64Val(9), strVal("wwww")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(197), nil, nil, nil},
				maxRow: []sql.Value{i64Val(199), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(198), i64Val(495), strVal("****"), i64Val(9), strVal("wwww")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(198), nil, nil, nil},
				maxRow: []sql.Value{i64Val(199), nil, nil, nil},
				values: [][]sql.Value{
					{i64Val(198), i64Val(495), strVal("****"), i64Val(9), strVal("wwww")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdRows,
				minRow: []sql.Value{i64Val(199), nil, nil, nil},
				maxRow: []sql.Value{i64Val(199), nil, nil, nil},
				values: nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
	}

	indexMinMaxTests = []interface{}{
		"createDatabase",

		createMinMax,
		insertRows(minMaxRows),
		checkMinMaxRows,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				idxValues: sortValues([]sql.ColumnKey{sql.MakeColumnKey(0, true)},
					pickColumns([]int{1, 0}, minMaxRows)),
				values: sortValues([]sql.ColumnKey{sql.MakeColumnKey(1, true)},
					pickColumns([]int{0, 1, 2, 3, 4}, minMaxRows)),
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-2"),
				idxValues: sortValues([]sql.ColumnKey{sql.MakeColumnKey(0, false),
					sql.MakeColumnKey(1, false), sql.MakeColumnKey(2, false)},
					pickColumns([]int{2, 3, 0}, minMaxRows)),
				values: sortValues([]sql.ColumnKey{sql.MakeColumnKey(2, false),
					sql.MakeColumnKey(3, false), sql.MakeColumnKey(0, false)},
					pickColumns([]int{0, 1, 2, 3, 4}, minMaxRows)),
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-3"),
				idxValues: sortValues(
					[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false)},
					pickColumns([]int{4, 0}, minMaxRows)),
				values: sortValues(
					[]sql.ColumnKey{sql.MakeColumnKey(4, false), sql.MakeColumnKey(0, false)},
					pickColumns([]int{0, 1, 2, 3, 4}, minMaxRows)),
			},
			{fln: fln(), cmd: cmdCommit},
		},
	}
)

func makeValues(cnt int) [][]sql.Value {
	var vals [][]sql.Value

	for n := 1; n < cnt; n += 1 {
		vals = append(vals,
			[]sql.Value{
				i64Val(n * 2),
				i64Val(n * 5),
				strVal(strings.Repeat(string([]byte{byte((n % 90) + 33)}), (n%8)+1)),
				i64Val(n % 10),
				strVal(strings.Repeat(string([]byte{byte(122 - (n % 16))}), (n%16)+1)),
			})
	}

	return vals
}

func pickColumns(cols []int, vals [][]sql.Value) [][]sql.Value {
	var rows [][]sql.Value

	for _, val := range vals {
		row := make([]sql.Value, len(cols))
		for cdx, col := range cols {
			row[cdx] = val[col]
		}
		rows = append(rows, row)
	}

	return rows
}

func sortValues(key []sql.ColumnKey, vals [][]sql.Value) [][]sql.Value {
	testutil.SortValues(key, vals)
	return vals
}

func insertRows(rows [][]sql.Value) []storeCmd {
	var cmds []storeCmd
	cmds = append(cmds, storeCmd{fln: fln(), cmd: cmdBegin})
	cmds = append(cmds, storeCmd{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")})

	for _, row := range rows {
		cmds = append(cmds, storeCmd{fln: fln(), cmd: cmdInsert, row: row})
	}

	cmds = append(cmds, storeCmd{fln: fln(), cmd: cmdCommit})
	return cmds
}

func RunPrimaryMinMaxTest(t *testing.T, st *storage.Store) {
	t.Helper()

	dbname := sql.ID("primary_min_max_test")
	for _, test := range primaryMinMaxTests {
		runTest(t, st, dbname, test)
	}
}

func RunIndexMinMaxTest(t *testing.T, st *storage.Store) {
	t.Helper()

	dbname := sql.ID("index_min_max_test")
	for _, test := range indexMinMaxTests {
		runTest(t, st, dbname, test)
	}
}

/*
       minRow (< and nil)
       maxRow
row #1
row #2
    :

       minRow (< and nil)
row #1 maxRow
row #2
    :

       minRow (< and nil)
row #1
       maxRow
row #2
    :

    :
row #m-1
         minRow
         maxRow
row #m
row #m+1
    :

    :
row #m-1
         minRow
row #m   maxRow
row #m+1
    :

    :
row #m-1
row #m   minRow, maxRow
row #m+1
    :

    :
row #m-1
         minRow
row #m
         maxRow
row #m+1
    :

    :
row #m-1
row #m   minRow
         maxRow
row #m+1
    :

    :
row #n-1
       minRow
row #n
       maxRow (> and nil)

    :
row #n-1
row #n minRow
       maxRow (> and nil)

    :
row #n-1
row #n
       minRow
       maxRow (> and nil)
*/
