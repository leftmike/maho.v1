package sql_test

import (
	"testing"

	"github.com/leftmike/maho/sql"
)

func TestDataType(t *testing.T) {
	cases := []struct {
		ct sql.ColumnType
		dt string
	}{
		{
			sql.ColumnType{Type: sql.BooleanType},
			"BOOL",
		},
		{
			sql.ColumnType{Type: sql.StringType, Fixed: false, Size: 123},
			"VARCHAR(123)",
		},
		{
			sql.ColumnType{Type: sql.StringType, Fixed: true, Size: 123},
			"CHAR(123)",
		},
		{
			sql.ColumnType{Type: sql.StringType, Size: sql.MaxColumnSize},
			"TEXT",
		},
		{
			sql.ColumnType{Type: sql.BytesType, Fixed: false, Size: 123},
			"VARBINARY(123)",
		},
		{
			sql.ColumnType{Type: sql.BytesType, Fixed: true, Size: 123},
			"BINARY(123)",
		},
		{
			sql.ColumnType{Type: sql.BytesType, Size: sql.MaxColumnSize},
			"BYTES",
		},
		{
			sql.ColumnType{Type: sql.FloatType},
			"DOUBLE",
		},
		{
			sql.ColumnType{Type: sql.IntegerType, Size: 2},
			"SMALLINT",
		},
		{
			sql.ColumnType{Type: sql.IntegerType, Size: 4},
			"INT",
		},
		{
			sql.ColumnType{Type: sql.IntegerType, Size: 8},
			"BIGINT",
		},
	}

	for _, c := range cases {
		if c.ct.DataType() != c.dt {
			t.Errorf("ColumnType{%v}.DataType() got %s want %s", c.ct, c.ct.DataType(), c.dt)
		}
	}
}
