package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

func mustCompile(t *testing.T, e expr.Expr) sql.CExpr {
	ce, err := expr.Compile(context.Background(), nil, nil, nil, e)
	if err != nil {
		t.Fatal(err)
	}
	return ce
}

func TestEncodeDecode(t *testing.T) {
	tt := &TableType{
		ver:  123,
		cols: []sql.Identifier{sql.ID("col1"), sql.ID("col2"), sql.ID("col3"), sql.ID("col4")},
		colTypes: []sql.ColumnType{
			{
				Type:    sql.BooleanType,
				NotNull: true,
			},
			{
				Type: sql.IntegerType,
				Size: 4,
				Default: mustCompile(t, &expr.Binary{
					Op:    expr.AddOp,
					Left:  expr.Int64Literal(123),
					Right: expr.Int64Literal(456),
				}),
				DefaultExpr: "123 + 456",
			},
			{
				Type:    sql.StringType,
				Size:    2048,
				NotNull: true,
			},
			{
				Type:  sql.StringType,
				Size:  128,
				Fixed: true,
			},
		},
		primary: []sql.ColumnKey{sql.MakeColumnKey(1, true), sql.MakeColumnKey(0, false)},
		indexes: []sql.IndexType{
			{
				Name:    sql.ID("index_one"),
				Key:     []sql.ColumnKey{sql.MakeColumnKey(3, true)},
				Columns: []int{3},
				Unique:  true,
			},
		},
		constraints: []constraint{
			{
				name:   sql.ID("col1_not_null"),
				typ:    sql.NotNullConstraint,
				colNum: 0,
			},
			{
				name:   sql.ID("col2_default"),
				typ:    sql.DefaultConstraint,
				colNum: 1,
			},
			{
				name:   sql.ID("col3_not_null"),
				typ:    sql.NotNullConstraint,
				colNum: 2,
			},
		},
		checks: []checkConstraint{
			{
				name: sql.ID("check_1"),
				check: mustCompile(t, &expr.Binary{
					Op:    expr.GreaterThanOp,
					Left:  expr.Int64Literal(123),
					Right: expr.Int64Literal(456),
				}),
				checkExpr: "123 > 456",
			},
		},
		foreignKeys: []foreignKey{},
		foreignRefs: []foreignRef{},
		triggers:    []trigger{},
	}

	buf, err := tt.Encode()
	if err != nil {
		t.Errorf("Encode() failed with %s", err)
	}

	tt2, err := DecodeTableType(sql.TableName{sql.DATABASE, sql.SCHEMA, sql.TABLE}, buf)
	if err != nil {
		t.Errorf("DecodeTableType() failed with %s", err)
	}
	if !reflect.DeepEqual(tt, tt2) {
		t.Errorf("DecodeTableType() got %#v want %#v", tt2, tt)
	}
}
