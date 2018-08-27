package datadef

import (
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

func (stmt *DropTable) Plan(ses *evaluate.Session, tx *engine.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *DropTable) Execute(ses *evaluate.Session, tx *engine.Transaction) (int64, error) {
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
