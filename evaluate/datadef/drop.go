package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type DropTable struct {
	IfExists bool
	Tables   []sql.TableName
}

func (stmt *DropTable) String() string {
	s := "DROP TABLE "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	for i, tbl := range stmt.Tables {
		if i > 0 {
			s += ", "
		}
		s += tbl.String()
	}
	return s
}

func (stmt *DropTable) Plan(ses *evaluate.Session, ctx context.Context, pe evaluate.PlanEngine,
	tx sql.Transaction) (evaluate.Plan, error) {

	for idx, tn := range stmt.Tables {
		stmt.Tables[idx] = ses.ResolveTableName(tn)
	}
	return stmt, nil
}

func (stmt *DropTable) Explain() string {
	return stmt.String()
}

func (stmt *DropTable) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	for _, tn := range stmt.Tables {
		err := e.DropTable(ctx, tx, tn, stmt.IfExists)
		if err != nil {
			return -1, err
		}
	}
	return -1, nil
}

type DropIndex struct {
	Table    sql.TableName
	Index    sql.Identifier
	IfExists bool
}

func (stmt *DropIndex) String() string {
	s := "DROP INDEX "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	s += fmt.Sprintf("%s ON %s", stmt.Index, stmt.Table)
	return s
}

func (stmt *DropIndex) Plan(ses *evaluate.Session, ctx context.Context, pe evaluate.PlanEngine,
	tx sql.Transaction) (evaluate.Plan, error) {

	stmt.Table = ses.ResolveTableName(stmt.Table)
	return stmt, nil
}

func (stmt *DropIndex) Explain() string {
	return stmt.String()
}

func (stmt *DropIndex) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	return -1, e.DropIndex(ctx, tx, stmt.Index, stmt.Table, stmt.IfExists)
}

type DropDatabase struct {
	IfExists bool
	Database sql.Identifier
	Options  map[sql.Identifier]string
}

func (stmt *DropDatabase) String() string {
	s := "DROP DATABASE "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	s += stmt.Database.String()
	if len(stmt.Options) > 0 {
		s += " WITH"
		for opt, val := range stmt.Options {
			s = fmt.Sprintf("%s %s = %s", s, opt, val)
		}
	}
	return s
}

func (stmt *DropDatabase) Plan(ses *evaluate.Session, ctx context.Context, pe evaluate.PlanEngine,
	tx sql.Transaction) (evaluate.Plan, error) {

	return stmt, nil
}

func (stmt *DropDatabase) Explain() string {
	return stmt.String()
}

func (stmt *DropDatabase) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	return -1, e.DropDatabase(stmt.Database, stmt.IfExists, stmt.Options)
}

type DropSchema struct {
	IfExists bool
	Schema   sql.SchemaName
}

func (stmt *DropSchema) String() string {
	s := "DROP SCHEMA "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	return s + stmt.Schema.String()
}

func (stmt *DropSchema) Plan(ses *evaluate.Session, ctx context.Context, pe evaluate.PlanEngine,
	tx sql.Transaction) (evaluate.Plan, error) {

	stmt.Schema = ses.ResolveSchemaName(stmt.Schema)
	return stmt, nil
}

func (stmt *DropSchema) Explain() string {
	return stmt.String()
}

func (stmt *DropSchema) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	return -1, e.DropSchema(ctx, tx, stmt.Schema, stmt.IfExists)
}
