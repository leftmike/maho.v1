package stmt

import (
	"fmt"

	"github.com/leftmike/maho/db"
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

func (stmt *DropTable) Plan(e *engine.Engine) (interface{}, error) {
	return stmt, nil
}

func (stmt *DropTable) Execute(e *engine.Engine) (int64, error) {
	for _, tbl := range stmt.Tables {
		d, err := e.LookupDatabase(tbl.Database)
		if err != nil {
			return 0, err
		}
		dbase, ok := d.(db.DatabaseModify)
		if !ok {
			return 0, fmt.Errorf("engine: database \"%s\" can't be modified", d.Name())
		}
		err = dbase.DropTable(tbl.Table, stmt.IfExists)
		if err != nil {
			return 0, err
		}
	}
	return 0, nil
}
