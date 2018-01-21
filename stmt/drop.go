package stmt

import (
	"fmt"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
)

type DropTable struct {
	IfExists bool
	Tables   []TableName
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

func (stmt *DropTable) Execute(e *engine.Engine) (interface{}, error) {
	for _, tbl := range stmt.Tables {
		d, err := e.LookupDatabase(tbl.Database)
		if err != nil {
			return nil, err
		}
		dbase, ok := d.(db.DatabaseModify)
		if !ok {
			return nil, fmt.Errorf("\"%s\" database can't be modified", d.Name())
		}
		err = dbase.DropTable(tbl.Table, stmt.IfExists)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
