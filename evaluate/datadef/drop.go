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

func (stmt *DropTable) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	for idx, tn := range stmt.Tables {
		stmt.Tables[idx] = pctx.ResolveTableName(tn)
	}
	return stmt, nil
}

func (_ *DropTable) Planned() {}

func (stmt *DropTable) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	for _, tn := range stmt.Tables {
		err := tx.DropTable(ctx, tn, stmt.IfExists)
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

func (stmt *DropIndex) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	stmt.Table = pctx.ResolveTableName(stmt.Table)
	return stmt, nil
}

func (_ *DropIndex) Planned() {}

func (stmt *DropIndex) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	return -1, tx.DropIndex(ctx, stmt.Index, stmt.Table, stmt.IfExists)
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

func (stmt *DropDatabase) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	return stmt, nil
}

func (_ *DropDatabase) Planned() {}

func (stmt *DropDatabase) Command(ctx context.Context, ses *evaluate.Session, e sql.Engine) (int64,
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

func (stmt *DropSchema) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	stmt.Schema = pctx.ResolveSchemaName(stmt.Schema)
	return stmt, nil
}

func (_ *DropSchema) Planned() {}

func (stmt *DropSchema) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	return -1, tx.DropSchema(ctx, stmt.Schema, stmt.IfExists)
}
