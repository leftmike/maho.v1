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
	Primary     sql.IndexKey
	Indexes     []sql.IndexKey
	IfNotExists bool
}

func (stmt *CreateTable) String() string {
	s := "CREATE TABLE"
	if stmt.IfNotExists {
		s += " IF NOT EXISTS"
	}
	s = fmt.Sprintf("%s %s (", s, stmt.Table)

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
	if len(stmt.Primary.Columns) > 0 {
		s += fmt.Sprintf(", PRIMARY KEY %s", stmt.Primary)
	}
	for _, key := range stmt.Indexes {
		s += fmt.Sprintf(", UNIQUE %s", key)
	}
	s += ")"
	return s
}

func (stmt *CreateTable) findColumn(nam sql.Identifier) (int, bool) {
	for i, col := range stmt.Columns {
		if nam == col {
			return i, true
		}
	}
	return -1, false
}

func (stmt *CreateTable) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	stmt.Table = ses.ResolveTableName(stmt.Table)

	for _, col := range stmt.Primary.Columns {
		i, ok := stmt.findColumn(col)
		if !ok {
			return nil, fmt.Errorf("engine: unknown column %s in primary key for table %s", col,
				stmt.Table)
		}
		stmt.ColumnTypes[i].NotNull = true
	}

	for _, ik := range stmt.Indexes {
		for _, col := range ik.Columns {
			if _, ok := stmt.findColumn(col); !ok {
				return nil, fmt.Errorf("engine: unknown column %s in index for table %s", col,
					stmt.Table)
			}
		}
	}
	return stmt, nil
}

func (stmt *CreateTable) Execute(ctx context.Context, eng engine.Engine,
	tx engine.Transaction) (int64, error) {

	err := eng.CreateTable(ctx, tx, stmt.Table, stmt.Columns, stmt.ColumnTypes, stmt.Primary,
		stmt.IfNotExists)
	if err != nil {
		return -1, err
	}

	for i, ik := range stmt.Indexes {
		err = eng.CreateIndex(ctx, tx, sql.ID(fmt.Sprintf("index-%d", i)), stmt.Table, ik, false)
		if err != nil {
			return -1, err
		}
	}
	return -1, nil
}

type CreateIndex struct {
	Index       sql.Identifier
	Table       sql.TableName
	Key         sql.IndexKey
	IfNotExists bool
}

func (stmt *CreateIndex) String() string {
	s := "CREATE"
	if stmt.Key.Unique {
		s += " UNIQUE "
	}
	s += " INDEX"
	if stmt.IfNotExists {
		s += " IF NOT EXISTS"
	}
	s += fmt.Sprintf(" %s ON %s (%s)", stmt.Index, stmt.Table, stmt.Key)
	return s
}

func (stmt *CreateIndex) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	stmt.Table = ses.ResolveTableName(stmt.Table)
	return stmt, nil
}

func (stmt *CreateIndex) Execute(ctx context.Context, eng engine.Engine,
	tx engine.Transaction) (int64, error) {

	return -1, eng.CreateIndex(ctx, tx, stmt.Index, stmt.Table, stmt.Key, stmt.IfNotExists)
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

func (stmt *CreateDatabase) Command(ses *evaluate.Session) error {
	return ses.Engine.CreateDatabase(stmt.Database, stmt.Options)
}

type CreateSchema struct {
	Schema sql.SchemaName
}

func (stmt *CreateSchema) String() string {
	return fmt.Sprintf("CREATE SCHEMA %s", stmt.Schema)
}

func (stmt *CreateSchema) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{},
	error) {

	stmt.Schema = ses.ResolveSchemaName(stmt.Schema)
	return stmt, nil
}

func (stmt *CreateSchema) Execute(ctx context.Context, eng engine.Engine,
	tx engine.Transaction) (int64, error) {

	return -1, eng.CreateSchema(ctx, tx, stmt.Schema)
}
