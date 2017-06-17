package stmt

import (
	"maho/store"
)

type Stmt interface {
	String() string
	Dispatch(e Executer) (interface{}, error)
}

type Executer interface {
	CreateTable(stmt *CreateTable) (interface{}, error)
	InsertValues(stmt *InsertValues) (interface{}, error)
	Select(stmt *Select) (store.Rows, error)
}
