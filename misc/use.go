package misc

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type Use struct {
	Database sql.Identifier
}

func (stmt *Use) String() string {
	return fmt.Sprintf("USE %s", stmt.Database)
}

func (stmt *Use) Plan(ctx context.Context, tx engine.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *Use) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return 0, engine.Use(stmt.Database)
}
