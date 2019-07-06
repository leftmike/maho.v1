package datadef

import (
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
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

func (stmt *DropTable) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *DropTable) Execute(ses *evaluate.Session, tx engine.Transaction) (int64, error) {
	for _, tbl := range stmt.Tables {
		dbname := tbl.Database
		if dbname == 0 {
			dbname = ses.DefaultDatabase
		}
		err := ses.Manager.DropTable(ses, tx, dbname, tbl.Table, stmt.IfExists)
		if err != nil {
			return -1, err
		}
	}
	return -1, nil
}

type DropDatabase struct {
	IfExists bool
	Database sql.Identifier
	Options  map[sql.Identifier]string
}

func (stmt *DropDatabase) String() string {
	s := "DETACH DATABASE "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	s += stmt.Database.String()
	if len(stmt.Options) > 0 {
		s += " WITH"
		for opt, val := range stmt.Options {
			s = fmt.Sprintf("%s %s = %s", s, opt, val)
		}
	}
	return s
}

func (stmt *DropDatabase) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{},
	error) {

	return stmt, nil
}

func (stmt *DropDatabase) Execute(ses *evaluate.Session, tx engine.Transaction) (int64, error) {
	return -1, ses.Manager.DropDatabase(stmt.Database, stmt.IfExists, stmt.Options)
}
