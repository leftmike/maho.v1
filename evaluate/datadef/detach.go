package datadef

import (
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type DetachDatabase struct {
	Database sql.Identifier
	Options  map[sql.Identifier]string
}

func (stmt *DetachDatabase) String() string {
	s := fmt.Sprintf("DETACH DATABASE %s", stmt.Database)
	if len(stmt.Options) > 0 {
		s += " WITH"
		for opt, val := range stmt.Options {
			s = fmt.Sprintf("%s %s = %s", s, opt, val)
		}
	}
	return s
}

func (stmt *DetachDatabase) Plan(ses *evaluate.Session, tx *engine.Transaction) (interface{},
	error) {

	return stmt, nil
}

func (stmt *DetachDatabase) Execute(ses *evaluate.Session, tx *engine.Transaction) (int64, error) {
	return -1, ses.Manager.DetachDatabase(stmt.Database, stmt.Options)
}
