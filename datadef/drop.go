package datadef

import (
	"context"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type DropTable struct {
	IfExists bool
	Tables   []sql.TableName
}

func (stmt *DropTable) String() string {
	s := "DROP TABLE "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	for i, tbl := range stmt.Tables {
		if i > 0 {
			s += ", "
		}
		s += tbl.String()
	}
	return s
}

func (stmt *DropTable) Plan(ctx context.Context, tx engine.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *DropTable) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	for _, tbl := range stmt.Tables {
		err := engine.DropTable(ctx, tx, tbl.Database, tbl.Table, stmt.IfExists)
		if err != nil {
			return 0, err
		}
	}
	return 0, nil
}