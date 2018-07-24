package evaluate

import (
	"context"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type Session interface {
	Context() context.Context
	DefaultEngine() string // XXX: delete?
	DefaultDatabase() sql.Identifier // XXX: delete?
	Manager() *engine.Manager
	Begin() error
	Commit() error
	Rollback() error
}
