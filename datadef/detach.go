package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type DetachDatabase struct {
	Database sql.Identifier
}

func (stmt *DetachDatabase) String() string {
	return fmt.Sprintf("DETACH DATABASE %s", stmt.Database)
}

func (stmt *DetachDatabase) Plan(ctx context.Context, tx engine.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *DetachDatabase) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return 0, nil // XXX
}
