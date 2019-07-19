package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type KeyType int

const (
	PrimaryKey KeyType = iota
	UniqueKey
)

type Key struct {
	Type    KeyType
	Columns []sql.Identifier
	Reverse []bool // ASC = false, DESC = true
}

func (k1 Key) Equal(k2 Key) bool {
	if len(k1.Columns) != len(k2.Columns) {
		return false
	}

	for cdx := range k1.Columns {
		if k1.Columns[cdx] != k2.Columns[cdx] || k1.Reverse[cdx] != k2.Reverse[cdx] {
			return false
		}
	}
	return true
}

type CreateTable struct {
	Table       sql.TableName
	Columns     []sql.Identifier
	ColumnTypes []sql.ColumnType
	Keys        []Key
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
	for _, key := range stmt.Keys {
		switch key.Type {
		case PrimaryKey:
			s += ", PRIMARY KEY ("
		case UniqueKey:
			s += ", UNIQUE ("
		default:
			panic(fmt.Sprintf("unexpected key type: %d", key.Type))
		}

		for i := range key.Columns {
			if i > 0 {
				s += ", "
			}
			if key.Reverse[i] {
				s += fmt.Sprintf("%s DESC", key.Columns[i])
			} else {
				s += fmt.Sprintf("%s ASC", key.Columns[i])
			}
		}
		s += ")"
	}
	s += ")"
	return s
}

func (stmt *CreateTable) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	stmt.Table = ses.ResolveTableName(stmt.Table)
	return stmt, nil
}

func (stmt *CreateTable) Execute(ctx context.Context, eng engine.Engine,
	tx engine.Transaction) (int64, error) {

	if stmt.IfNotExists {
		_, err := eng.LookupTable(ctx, tx, stmt.Table)
		if err == nil {
			return -1, nil
		}
	}

	return -1, eng.CreateTable(ctx, tx, stmt.Table, stmt.Columns, stmt.ColumnTypes)
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
