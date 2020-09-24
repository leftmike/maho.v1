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

	index1MinMaxTests = []interface{}{
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
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow:    nil,
				maxRow:    []sql.Value{nil, i64Val(500), nil, nil, nil},
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow:    []sql.Value{nil, i64Val(505), nil, nil, nil},
				maxRow:    []sql.Value{nil, i64Val(500), nil, nil, nil},
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow: []sql.Value{nil, i64Val(500), nil, nil, nil},
				maxRow: []sql.Value{nil, i64Val(495), nil, nil, nil},
				idxValues: [][]sql.Value{
					{i64Val(495), i64Val(198)},
				},
				values: [][]sql.Value{
					{i64Val(198), i64Val(495), strVal("****"), i64Val(9), strVal("wwww")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow: nil,
				maxRow: []sql.Value{nil, i64Val(491), nil, nil, nil},
				idxValues: [][]sql.Value{
					{i64Val(495), i64Val(198)},
				},
				values: [][]sql.Value{
					{i64Val(198), i64Val(495), strVal("****"), i64Val(9), strVal("wwww")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow:    []sql.Value{nil, i64Val(404), nil, nil, nil},
				maxRow:    []sql.Value{nil, i64Val(401), nil, nil, nil},
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow: []sql.Value{nil, i64Val(353), nil, nil, nil},
				maxRow: []sql.Value{nil, i64Val(350), nil, nil, nil},
				idxValues: [][]sql.Value{
					{i64Val(350), i64Val(140)},
				},
				values: [][]sql.Value{
					{i64Val(140), i64Val(350), strVal("ggggggg"), i64Val(0), strVal("ttttttt")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow: []sql.Value{nil, i64Val(335), nil, nil, nil},
				maxRow: []sql.Value{nil, i64Val(335), nil, nil, nil},
				idxValues: [][]sql.Value{
					{i64Val(335), i64Val(134)},
				},
				values: [][]sql.Value{
					{i64Val(134), i64Val(335), strVal("dddd"), i64Val(7), strVal("wwww")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow: []sql.Value{nil, i64Val(299), nil, nil, nil},
				maxRow: []sql.Value{nil, i64Val(253), nil, nil, nil},
				idxValues: [][]sql.Value{
					{i64Val(295), i64Val(118)},
					{i64Val(290), i64Val(116)},
					{i64Val(285), i64Val(114)},
					{i64Val(280), i64Val(112)},
					{i64Val(275), i64Val(110)},
					{i64Val(270), i64Val(108)},
					{i64Val(265), i64Val(106)},
					{i64Val(260), i64Val(104)},
					{i64Val(255), i64Val(102)},
				},
				values: [][]sql.Value{
					{i64Val(118), i64Val(295), strVal(`\\\\`), i64Val(9), strVal("oooooooooooo")},
					{i64Val(116), i64Val(290), strVal("[[["), i64Val(8), strVal("ppppppppppp")},
					{i64Val(114), i64Val(285), strVal("ZZ"), i64Val(7), strVal("qqqqqqqqqq")},
					{i64Val(112), i64Val(280), strVal("Y"), i64Val(6), strVal("rrrrrrrrr")},
					{i64Val(110), i64Val(275), strVal("XXXXXXXX"), i64Val(5), strVal("ssssssss")},
					{i64Val(108), i64Val(270), strVal("WWWWWWW"), i64Val(4), strVal("ttttttt")},
					{i64Val(106), i64Val(265), strVal("VVVVVV"), i64Val(3), strVal("uuuuuu")},
					{i64Val(104), i64Val(260), strVal("UUUUU"), i64Val(2), strVal("vvvvv")},
					{i64Val(102), i64Val(255), strVal("TTTT"), i64Val(1), strVal("wwww")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow: []sql.Value{nil, i64Val(200), nil, nil, nil},
				maxRow: []sql.Value{nil, i64Val(199), nil, nil, nil},
				idxValues: [][]sql.Value{
					{i64Val(200), i64Val(80)},
				},
				values: [][]sql.Value{
					{i64Val(80), i64Val(200), strVal("I"), i64Val(0), strVal("rrrrrrrrr")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow: []sql.Value{nil, i64Val(12), nil, nil, nil},
				maxRow: []sql.Value{nil, i64Val(0), nil, nil, nil},
				idxValues: [][]sql.Value{
					{i64Val(10), i64Val(4)},
					{i64Val(5), i64Val(2)},
				},
				values: [][]sql.Value{
					{i64Val(4), i64Val(10), strVal("###"), i64Val(2), strVal("xxx")},
					{i64Val(2), i64Val(5), strVal(`""`), i64Val(1), strVal("yy")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow: []sql.Value{nil, i64Val(7), nil, nil, nil},
				maxRow: nil,
				idxValues: [][]sql.Value{
					{i64Val(5), i64Val(2)},
				},
				values: [][]sql.Value{
					{i64Val(2), i64Val(5), strVal(`""`), i64Val(1), strVal("yy")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow: []sql.Value{nil, i64Val(5), nil, nil, nil},
				maxRow: nil,
				idxValues: [][]sql.Value{
					{i64Val(5), i64Val(2)},
				},
				values: [][]sql.Value{
					{i64Val(2), i64Val(5), strVal(`""`), i64Val(1), strVal("yy")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow:    []sql.Value{nil, i64Val(3), nil, nil, nil},
				maxRow:    []sql.Value{nil, i64Val(3), nil, nil, nil},
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-1"),
				minRow:    []sql.Value{nil, i64Val(2), nil, nil, nil},
				maxRow:    nil,
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
	}

	index2MinMaxTests = []interface{}{
		"createDatabase",

		createMinMax,
		insertRows(minMaxRows),
		checkMinMaxRows,
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
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-2"),
				minRow:    nil,
				maxRow:    []sql.Value{nil, nil, strVal("!!!"), i64Val(-1), nil},
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-2"),
				minRow:    []sql.Value{nil, nil, strVal("!!"), i64Val(0), nil},
				maxRow:    []sql.Value{nil, nil, strVal("!!!"), i64Val(-1), nil},
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-2"),
				minRow: nil,
				maxRow: []sql.Value{nil, nil, strVal("!!!"), i64Val(1), nil},
				idxValues: [][]sql.Value{
					{strVal("!!!"), i64Val(0), i64Val(180)},
				},
				values: [][]sql.Value{
					{i64Val(180), i64Val(450), strVal("!!!"), i64Val(0), strVal("ppppppppppp")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-2"),
				minRow: []sql.Value{nil, nil, nil, i64Val(0), nil},
				maxRow: []sql.Value{nil, nil, strVal(`""`), i64Val(2), nil},
				idxValues: [][]sql.Value{
					{strVal("!!!"), i64Val(0), i64Val(180)},
					{strVal(`""`), i64Val(1), i64Val(2)},
				},
				values: [][]sql.Value{
					{i64Val(180), i64Val(450), strVal("!!!"), i64Val(0), strVal("ppppppppppp")},
					{i64Val(2), i64Val(5), strVal(`""`), i64Val(1), strVal("yy")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-2"),
				minRow:    []sql.Value{nil, nil, strVal("&&&&&&&&"), i64Val(6), nil},
				maxRow:    []sql.Value{nil, nil, strVal("'"), i64Val(5), nil},
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-2"),
				minRow: []sql.Value{nil, nil, strVal("9"), i64Val(4), nil},
				maxRow: []sql.Value{nil, nil, strVal("A"), i64Val(3), nil},
				idxValues: [][]sql.Value{
					{strVal("9"), i64Val(4), i64Val(48)},
					{strVal("::"), i64Val(5), i64Val(50)},
					{strVal(";;;"), i64Val(6), i64Val(52)},
					{strVal("<<<<"), i64Val(7), i64Val(54)},
					{strVal("====="), i64Val(8), i64Val(56)},
					{strVal(">>>>>>"), i64Val(9), i64Val(58)},
					{strVal("???????"), i64Val(0), i64Val(60)},
					{strVal("@@@@@@@@"), i64Val(1), i64Val(62)},
					{strVal("A"), i64Val(2), i64Val(64)},
				},
				values: [][]sql.Value{
					{i64Val(48), i64Val(120), strVal("9"), i64Val(4), strVal("rrrrrrrrr")},
					{i64Val(50), i64Val(125), strVal("::"), i64Val(5), strVal("qqqqqqqqqq")},
					{i64Val(52), i64Val(130), strVal(";;;"), i64Val(6), strVal("ppppppppppp")},
					{i64Val(54), i64Val(135), strVal("<<<<"), i64Val(7), strVal("oooooooooooo")},
					{i64Val(56), i64Val(140), strVal("====="), i64Val(8), strVal("nnnnnnnnnnnnn")},
					{i64Val(58), i64Val(145), strVal(">>>>>>"), i64Val(9),
						strVal("mmmmmmmmmmmmmm")},
					{i64Val(60), i64Val(150), strVal("???????"), i64Val(0),
						strVal("lllllllllllllll")},
					{i64Val(62), i64Val(155), strVal("@@@@@@@@"), i64Val(1),
						strVal("kkkkkkkkkkkkkkkk")},
					{i64Val(64), i64Val(160), strVal("A"), i64Val(2), strVal("z")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-2"),
				minRow: []sql.Value{nil, nil, strVal("zz"), i64Val(9), nil},
				maxRow: []sql.Value{nil, nil, strVal("zz"), i64Val(10), nil},
				idxValues: [][]sql.Value{
					{strVal("zz"), i64Val(9), i64Val(178)},
				},
				values: [][]sql.Value{
					{i64Val(178), i64Val(445), strVal("zz"), i64Val(9), strVal("qqqqqqqqqq")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-2"),
				minRow: []sql.Value{nil, nil, strVal("zz"), i64Val(9), nil},
				maxRow: nil,
				idxValues: [][]sql.Value{
					{strVal("zz"), i64Val(9), i64Val(178)},
				},
				values: [][]sql.Value{
					{i64Val(178), i64Val(445), strVal("zz"), i64Val(9), strVal("qqqqqqqqqq")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-2"),
				minRow:    []sql.Value{nil, nil, strVal("zzz"), i64Val(9), nil},
				maxRow:    nil,
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
	}

	index3MinMaxTests = []interface{}{
		"createDatabase",

		createMinMax,
		insertRows(minMaxRows),
		checkMinMaxRows,
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
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-3"),
				minRow:    nil,
				maxRow:    []sql.Value{nil, nil, nil, nil, strVal("jjj")},
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-3"),
				minRow:    []sql.Value{nil, nil, nil, nil, strVal("iii")},
				maxRow:    []sql.Value{nil, nil, nil, nil, strVal("jjj")},
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-3"),
				minRow: []sql.Value{nil, nil, nil, nil, strVal("iii")},
				maxRow: []sql.Value{nil, nil, nil, nil, strVal("l")},
				idxValues: [][]sql.Value{
					{strVal("kkkkkkkkkkkkkkkk"), i64Val(30)},
					{strVal("kkkkkkkkkkkkkkkk"), i64Val(62)},
					{strVal("kkkkkkkkkkkkkkkk"), i64Val(94)},
					{strVal("kkkkkkkkkkkkkkkk"), i64Val(126)},
					{strVal("kkkkkkkkkkkkkkkk"), i64Val(158)},
					{strVal("kkkkkkkkkkkkkkkk"), i64Val(190)},
				},
				values: [][]sql.Value{
					{i64Val(30), i64Val(75), strVal("00000000"), i64Val(5),
						strVal("kkkkkkkkkkkkkkkk")},
					{i64Val(62), i64Val(155), strVal("@@@@@@@@"), i64Val(1),
						strVal("kkkkkkkkkkkkkkkk")},
					{i64Val(94), i64Val(235), strVal("PPPPPPPP"), i64Val(7),
						strVal("kkkkkkkkkkkkkkkk")},
					{i64Val(126), i64Val(315), strVal("````````"), i64Val(3),
						strVal("kkkkkkkkkkkkkkkk")},
					{i64Val(158), i64Val(395), strVal("pppppppp"), i64Val(9),
						strVal("kkkkkkkkkkkkkkkk")},
					{i64Val(190), i64Val(475), strVal("&&&&&&&&"), i64Val(5),
						strVal("kkkkkkkkkkkkkkkk")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-3"),
				minRow: []sql.Value{nil, nil, nil, nil, strVal("ssssssss")},
				maxRow: []sql.Value{nil, nil, nil, nil, strVal("tttttttt")},
				idxValues: [][]sql.Value{
					{strVal("ssssssss"), i64Val(14)},
					{strVal("ssssssss"), i64Val(46)},
					{strVal("ssssssss"), i64Val(78)},
					{strVal("ssssssss"), i64Val(110)},
					{strVal("ssssssss"), i64Val(142)},
					{strVal("ssssssss"), i64Val(174)},
					{strVal("ttttttt"), i64Val(12)},
					{strVal("ttttttt"), i64Val(44)},
					{strVal("ttttttt"), i64Val(76)},
					{strVal("ttttttt"), i64Val(108)},
					{strVal("ttttttt"), i64Val(140)},
					{strVal("ttttttt"), i64Val(172)},
				},
				values: [][]sql.Value{
					{i64Val(14), i64Val(35), strVal("(((((((("), i64Val(7), strVal("ssssssss")},
					{i64Val(46), i64Val(115), strVal("88888888"), i64Val(3), strVal("ssssssss")},
					{i64Val(78), i64Val(195), strVal("HHHHHHHH"), i64Val(9), strVal("ssssssss")},
					{i64Val(110), i64Val(275), strVal("XXXXXXXX"), i64Val(5), strVal("ssssssss")},
					{i64Val(142), i64Val(355), strVal("hhhhhhhh"), i64Val(1), strVal("ssssssss")},
					{i64Val(174), i64Val(435), strVal("xxxxxxxx"), i64Val(7), strVal("ssssssss")},
					{i64Val(12), i64Val(30), strVal("'''''''"), i64Val(6), strVal("ttttttt")},
					{i64Val(44), i64Val(110), strVal("7777777"), i64Val(2), strVal("ttttttt")},
					{i64Val(76), i64Val(190), strVal("GGGGGGG"), i64Val(8), strVal("ttttttt")},
					{i64Val(108), i64Val(270), strVal("WWWWWWW"), i64Val(4), strVal("ttttttt")},
					{i64Val(140), i64Val(350), strVal("ggggggg"), i64Val(0), strVal("ttttttt")},
					{i64Val(172), i64Val(430), strVal("wwwwwww"), i64Val(6), strVal("ttttttt")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-3"),
				minRow: []sql.Value{nil, nil, nil, nil, strVal("z")},
				maxRow: []sql.Value{nil, nil, nil, nil, strVal("zz")},
				idxValues: [][]sql.Value{
					{strVal("z"), i64Val(32)},
					{strVal("z"), i64Val(64)},
					{strVal("z"), i64Val(96)},
					{strVal("z"), i64Val(128)},
					{strVal("z"), i64Val(160)},
					{strVal("z"), i64Val(192)},
				},
				values: [][]sql.Value{
					{i64Val(32), i64Val(80), strVal("1"), i64Val(6), strVal("z")},
					{i64Val(64), i64Val(160), strVal("A"), i64Val(2), strVal("z")},
					{i64Val(96), i64Val(240), strVal("Q"), i64Val(8), strVal("z")},
					{i64Val(128), i64Val(320), strVal("a"), i64Val(4), strVal("z")},
					{i64Val(160), i64Val(400), strVal("q"), i64Val(0), strVal("z")},
					{i64Val(192), i64Val(480), strVal("'"), i64Val(6), strVal("z")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-3"),
				minRow:    []sql.Value{nil, nil, nil, nil, strVal("zz")},
				maxRow:    []sql.Value{nil, nil, nil, nil, strVal("zzz")},
				idxValues: nil,
				values:    nil,
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdIndexRows, idxname: sql.ID("idx-3"),
				minRow:    []sql.Value{nil, nil, nil, nil, strVal("zz")},
				maxRow:    nil,
				idxValues: nil,
				values:    nil,
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

	for _, test := range index1MinMaxTests {
		runTest(t, st, sql.ID("index1_min_max_test"), test)
	}

	for _, test := range index2MinMaxTests {
		runTest(t, st, sql.ID("index2_min_max_test"), test)
	}

	for _, test := range index3MinMaxTests {
		runTest(t, st, sql.ID("index3_min_max_test"), test)
	}
}
