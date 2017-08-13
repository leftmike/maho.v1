package stmt

import (
	"fmt"

	"maho/db"
	"maho/engine"
)

type DropTable struct {
	Tables []TableName
}

func (stmt *DropTable) String() string {
	s := "DROP TABLE "
	for i, tbl := range stmt.Tables {
		if i > 0 {
			s += ", "
		}
		s += tbl.String()
	}
	return s
}

func (stmt *DropTable) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)

	for _, tbl := range stmt.Tables {
		d, err := e.LookupDatabase(tbl.Database)
		if err != nil {
			return nil, err
		}
		dbase, ok := d.(db.DatabaseModify)
		if !ok {
			return nil, fmt.Errorf("\"%s\" database can't be modified", d.Name())
		}
		err = dbase.DropTable(tbl.Table)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
