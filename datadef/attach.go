package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type AttachDatabase struct {
	Database sql.Identifier
	Options  map[sql.Identifier]string
}

func (stmt *AttachDatabase) String() string {
	s := fmt.Sprintf("ATTACH DATABASE %s", stmt.Database)
	if len(stmt.Options) > 0 {
		s += " WITH"
		for opt, val := range stmt.Options {
			s = fmt.Sprintf("%s %s = %s", s, opt, val)
		}
	}
	return s
}

func (stmt *AttachDatabase) Plan(ctx context.Context, tx engine.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *AttachDatabase) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return 0, engine.AttachDatabase(tx.DefaultEngine(), stmt.Database, stmt.Options)
}
