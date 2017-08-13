package db

import (
	"maho/sql"
)

type Database interface {
	Name() sql.Identifier
	Type() sql.Identifier
	CreateTable(name sql.Identifier, cols []ColumnType) error
	DropTable(name sql.Identifier) error
	Table(name sql.Identifier) (Table, error)
	Tables() ([]sql.Identifier, [][]ColumnType)
}
