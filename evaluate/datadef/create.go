package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

func columnNumber(nam sql.Identifier, columns []sql.Identifier) (int, bool) {
	for num, col := range columns {
		if nam == col {
			return num, true
		}
	}
	return -1, false
}

func indexKeyToColumnKeys(ik sql.IndexKey, columns []sql.Identifier) ([]sql.ColumnKey, error) {
	var colKeys []sql.ColumnKey

	for cdx, col := range ik.Columns {
		num, ok := columnNumber(col, columns)
		if !ok {
			return nil, fmt.Errorf("unknown column %s", col)
		}
		colKeys = append(colKeys, sql.MakeColumnKey(num, ik.Reverse[cdx]))
	}

	return colKeys, nil
}

type CreateTable struct {
	Table          sql.TableName
	Columns        []sql.Identifier
	ColumnTypes    []sql.ColumnType
	Primary        sql.IndexKey
	primaryColKeys []sql.ColumnKey
	Indexes        []sql.IndexKey
	indexesColKeys [][]sql.ColumnKey
	IfNotExists    bool
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

func (stmt *CreateTable) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	stmt.Table = ses.ResolveTableName(stmt.Table)

	var err error
	stmt.primaryColKeys, err = indexKeyToColumnKeys(stmt.Primary, stmt.Columns)
	if err != nil {
		return nil, fmt.Errorf("engine: %s in primary key for table %s", err, stmt.Table)
	}
	for _, col := range stmt.Primary.Columns {
		num, _ := columnNumber(col, stmt.Columns)
		stmt.ColumnTypes[num].NotNull = true
	}

	for _, ik := range stmt.Indexes {
		colKeys, err := indexKeyToColumnKeys(ik, stmt.Columns)
		if err != nil {
			return nil, fmt.Errorf("engine: %s in unique key for table %s", err, stmt.Table)
		}
		stmt.indexesColKeys = append(stmt.indexesColKeys, colKeys)
	}

	return stmt, nil
}

func (stmt *CreateTable) Execute(ctx context.Context, e *engine.Engine,
	tx engine.Transaction) (int64, error) {

	err := e.CreateTable(ctx, tx, stmt.Table, stmt.Columns, stmt.ColumnTypes, stmt.primaryColKeys,
		stmt.IfNotExists)
	if err != nil {
		return -1, err
	}
	tx.NextStmt()

	for i, ik := range stmt.Indexes {
		err = e.CreateIndex(ctx, tx, sql.ID(fmt.Sprintf("index-%d", i)), stmt.Table, ik.Unique,
			stmt.indexesColKeys[i], false)
		if err != nil {
			return -1, err
		}
		tx.NextStmt()
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

func (stmt *CreateIndex) Execute(ctx context.Context, e *engine.Engine,
	tx engine.Transaction) (int64, error) {

	_, tt, err := e.LookupTable(ctx, tx, stmt.Table)
	if err != nil {
		return -1, err
	}

	colKeys, err := indexKeyToColumnKeys(stmt.Key, tt.Columns())
	if err != nil {
		return -1, fmt.Errorf("engine: %s in unique key for table %s", err, stmt.Table)
	}

	return -1, e.CreateIndex(ctx, tx, stmt.Index, stmt.Table, stmt.Key.Unique, colKeys,
		stmt.IfNotExists)
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

func (stmt *CreateSchema) Execute(ctx context.Context, e *engine.Engine,
	tx engine.Transaction) (int64, error) {

	return -1, e.CreateSchema(ctx, tx, stmt.Schema)
}
