package datadef

import (
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type CreateTable struct {
	Table       sql.TableName
	Columns     []sql.Identifier
	ColumnTypes []sql.ColumnType
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

func (stmt *CreateTable) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *CreateTable) Execute(ses *evaluate.Session, tx engine.Transaction) (int64, error) {
	return -1, ses.Engine.CreateTable(ses.Context(), tx, ses.ResolveTableName(stmt.Table),
		stmt.Columns, stmt.ColumnTypes)
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

func (stmt *CreateDatabase) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{},
	error) {

	return stmt, nil
}

func (stmt *CreateDatabase) Execute(ses *evaluate.Session, tx engine.Transaction) (int64, error) {
	return -1, ses.Engine.CreateDatabase(stmt.Database, stmt.Options)
}

type CreateSchema struct {
	Schema sql.SchemaName
}

func (stmt *CreateSchema) String() string {
	return fmt.Sprintf("CREATE SCHEMA %s", stmt.Schema)
}

func (stmt *CreateSchema) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{},
	error) {

	return stmt, nil
}

func (stmt *CreateSchema) Execute(ses *evaluate.Session, tx engine.Transaction) (int64, error) {
	return -1, ses.Engine.CreateSchema(ses.Context(), tx, stmt.Schema)
}
