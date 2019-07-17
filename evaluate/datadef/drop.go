package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/engine"
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

func (stmt *DropTable) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	var tables []sql.TableName
	for _, tn := range stmt.Tables {
		tables = append(tables, ses.ResolveTableName(tn))
	}
	return &dropTablePlan{
		DropTable: DropTable{
			IfExists: stmt.IfExists,
			Tables:   tables,
		},
		eng: ses.Engine,
	}, nil
}

type dropTablePlan struct {
	DropTable
	eng engine.Engine
}

func (plan *dropTablePlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	for _, tn := range plan.Tables {
		err := plan.eng.DropTable(ctx, tx, tn, plan.IfExists)
		if err != nil {
			return -1, err
		}
	}
	return -1, nil
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

func (stmt *DropDatabase) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{},
	error) {

	return &dropDatabasePlan{
		DropDatabase: DropDatabase{
			IfExists: stmt.IfExists,
			Database: stmt.Database,
			Options:  stmt.Options,
		},
		eng: ses.Engine,
	}, nil
}

type dropDatabasePlan struct {
	DropDatabase
	eng engine.Engine
}

func (plan *dropDatabasePlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return -1, plan.eng.DropDatabase(plan.Database, plan.IfExists, plan.Options)
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

func (stmt *DropSchema) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	return &dropSchemaPlan{
		DropSchema: DropSchema{
			IfExists: stmt.IfExists,
			Schema:   stmt.Schema,
		},
		eng: ses.Engine,
	}, nil
}

type dropSchemaPlan struct {
	DropSchema
	eng engine.Engine
}

func (plan *dropSchemaPlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return -1, plan.eng.DropSchema(ctx, tx, plan.Schema, plan.IfExists)
}
