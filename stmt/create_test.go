package stmt_test

import (
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/stmt"
)

func TestCreateTable(t *testing.T) {
	s := stmt.CreateTable{Table: sql.TableName{sql.ID("xyz"), sql.ID("abc")}}
	r := "CREATE TABLE xyz.abc ()"
	if s.String() != r {
		t.Errorf("CreateTable{}.String() got %s want %s", s.String(), r)
	}
}
