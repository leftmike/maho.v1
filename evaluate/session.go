package evaluate

import (
	"context"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type Session interface {
	Context() context.Context
	DefaultEngine() string
	DefaultDatabase() sql.Identifier
	Manager() *engine.Manager
	Begin() error
	Commit() error
	Rollback() error
	Set(v sql.Identifier, s string) error
}
