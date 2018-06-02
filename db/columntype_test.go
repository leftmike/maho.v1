package db_test

import (
	"testing"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/sql"
)

func TestDataType(t *testing.T) {
	cases := []struct {
		ct db.ColumnType
		dt string
	}{
		{
			db.ColumnType{Type: sql.BooleanType},
			"BOOL",
		},
		{
			db.ColumnType{Type: sql.CharacterType, Binary: false, Fixed: false, Size: 123},
			"VARCHAR(123)",
		},
		{
			db.ColumnType{Type: sql.CharacterType, Binary: false, Fixed: true, Size: 123},
			"CHAR(123)",
		},
		{
			db.ColumnType{Type: sql.CharacterType, Binary: false, Size: db.MaxColumnSize},
			"TEXT",
		},
		{
			db.ColumnType{Type: sql.CharacterType, Binary: true, Fixed: false, Size: 123},
			"VARBINARY(123)",
		},
		{
			db.ColumnType{Type: sql.CharacterType, Binary: true, Fixed: true, Size: 123},
			"BINARY(123)",
		},
		{
			db.ColumnType{Type: sql.CharacterType, Binary: true, Size: db.MaxColumnSize},
			"BLOB",
		},
		{
			db.ColumnType{Type: sql.FloatType},
			"DOUBLE",
		},
		{
			db.ColumnType{Type: sql.IntegerType, Size: 2},
			"SMALLINT",
		},
		{
			db.ColumnType{Type: sql.IntegerType, Size: 4},
			"INT",
		},
		{
			db.ColumnType{Type: sql.IntegerType, Size: 8},
			"BIGINT",
		},
	}

	for _, c := range cases {
		if c.ct.DataType() != c.dt {
			t.Errorf("ColumnType{%v}.DataType() got %s want %s", c.ct, c.ct.DataType(), c.dt)
		}
	}
}
