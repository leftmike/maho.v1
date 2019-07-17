package datadef

import (
	"context"
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

type createTablePlan struct {
	CreateTable
	eng engine.Engine
}

func (stmt *CreateTable) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	return &createTablePlan{
		CreateTable: CreateTable{
			Table:       ses.ResolveTableName(stmt.Table),
			Columns:     stmt.Columns,
			ColumnTypes: stmt.ColumnTypes,
		},
		eng: ses.Engine,
	}, nil
}

func (plan *createTablePlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return -1, plan.eng.CreateTable(ctx, tx, plan.Table, plan.Columns, plan.ColumnTypes)
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

type createDatabasePlan struct {
	CreateDatabase
	eng engine.Engine
}

func (stmt *CreateDatabase) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{},
	error) {

	return &createDatabasePlan{
		CreateDatabase: CreateDatabase{
			Database: stmt.Database,
			Options:  stmt.Options,
		},
		eng: ses.Engine,
	}, nil
}

func (plan *createDatabasePlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return -1, plan.eng.CreateDatabase(plan.Database, plan.Options)
}

type CreateSchema struct {
	Schema sql.SchemaName
}

func (stmt *CreateSchema) String() string {
	return fmt.Sprintf("CREATE SCHEMA %s", stmt.Schema)
}

func (stmt *CreateSchema) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{},
	error) {

	return &createSchemaPlan{
		CreateSchema: CreateSchema{
			Schema: stmt.Schema,
		},
		eng: ses.Engine,
	}, nil
}

type createSchemaPlan struct {
	CreateSchema
	eng engine.Engine
}

func (plan *createSchemaPlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return -1, plan.eng.CreateSchema(ctx, tx, plan.Schema)
}
