package datadef_test

import (
	"testing"

	"github.com/leftmike/maho/evaluate/datadef"
	"github.com/leftmike/maho/sql"
)

func TestCreateTable(t *testing.T) {
	s := datadef.CreateTable{
		Table: sql.TableName{
			Database: sql.ID("xyz"),
			Schema:   sql.ID("mno"),
			Table:    sql.ID("abc"),
		},
	}
	r := "CREATE TABLE xyz.mno.abc ()"
	if s.String() != r {
		t.Errorf("CreateTable{}.String() got %s want %s", s.String(), r)
	}
}
