package db

import (
	"maho/sql"
)

type Database interface {
	Name() sql.Identifier
	Type() sql.Identifier
	Table(name sql.Identifier) (Table, error)
	Tables() []sql.Identifier
}

type DatabaseModify interface {
	Database
	CreateTable(name sql.Identifier, cols []sql.Identifier, colTypes []ColumnType) error
	DropTable(name sql.Identifier) error
}
