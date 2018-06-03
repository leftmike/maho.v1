package datadef

import (
	"fmt"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/sql"
)

type CreateTable struct {
	Table       sql.TableName
	Columns     []sql.Identifier
	ColumnTypes []db.ColumnType
}

func (stmt *CreateTable) String() string {
	s := fmt.Sprintf("CREATE TABLE %s (", stmt.Table)

	for i, ct := range stmt.ColumnTypes {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s %s", stmt.Columns[i], ct.DataType())
		if ct.NotNull {
			s += " NOT NULL"
		}
		if ct.Default != nil {
			s += fmt.Sprintf(" DEFAULT %s", ct.Default)
		}
	}
	s += ")"
	return s
}

func (stmt *CreateTable) Plan(ses *execute.Session, tx *engine.Transaction) (execute.Plan, error) {
	return stmt, nil
}

func (stmt *CreateTable) Execute(ses *execute.Session, tx *engine.Transaction) (int64, error) {
	return -1, engine.CreateTable(ses, tx, stmt.Table.Database, stmt.Table.Table, stmt.Columns,
		stmt.ColumnTypes)
}

type CreateDatabase struct {
	Database sql.Identifier
	Options  map[sql.Identifier]string
}

func (stmt *CreateDatabase) String() string {
	s := fmt.Sprintf("CREATE DATABASE %s", stmt.Database)
	if len(stmt.Options) > 0 {
		s += " WITH"
		for opt, val := range stmt.Options {
			s = fmt.Sprintf("%s %s = %s", s, opt, val)
		}
	}
	return s
}

func (stmt *CreateDatabase) Plan(ses *execute.Session, tx *engine.Transaction) (execute.Plan,
	error) {

	return stmt, nil
}

func (stmt *CreateDatabase) Execute(ses *execute.Session, tx *engine.Transaction) (int64, error) {
	return -1, engine.CreateDatabase(ses.DefaultEngine(), stmt.Database, stmt.Options)
}
