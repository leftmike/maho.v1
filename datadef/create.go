package datadef

import (
	"fmt"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/session"
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

func (stmt *CreateTable) Plan(ctx session.Context, tx *engine.Transaction) (execute.Plan, error) {
	return stmt, nil
}

func (stmt *CreateTable) Execute(ctx session.Context, tx *engine.Transaction) (int64, error) {
	return 0, engine.CreateTable(ctx, tx, stmt.Table.Database, stmt.Table.Table, stmt.Columns,
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

func (stmt *CreateDatabase) Plan(ctx session.Context, tx *engine.Transaction) (execute.Plan,
	error) {

	return stmt, nil
}

func (stmt *CreateDatabase) Execute(ctx session.Context, tx *engine.Transaction) (int64, error) {
	return 0, engine.CreateDatabase(ctx.DefaultEngine(), stmt.Database, stmt.Options)
}
