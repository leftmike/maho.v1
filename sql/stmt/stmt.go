package stmt

import (
	"fmt"
	"maho/store"
)

type Stmt interface {
	fmt.Stringer
	Dispatch(e Executer) (interface{}, error)
}

type Executer interface {
	CreateTable(stmt *CreateTable) (interface{}, error)
	DropTable(stmt *DropTable) (interface{}, error)
	InsertValues(stmt *InsertValues) (interface{}, error)
	Select(stmt *Select) (store.Rows, error)
}
