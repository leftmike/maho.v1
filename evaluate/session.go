package evaluate

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/sql"
)

type Session struct {
	User            string
	Type            string
	Addr            string
	ctx             context.Context
	e               sql.Engine
	defaultDatabase sql.Identifier
	defaultSchema   sql.Identifier
	sesid           uint64
	tx              sql.Transaction
}

func NewSession(e sql.Engine, defaultDatabase, defaultSchema sql.Identifier) *Session {
	return &Session{
		ctx:             context.Background(),
		e:               e,
		defaultDatabase: defaultDatabase,
		defaultSchema:   defaultSchema,
	}
}

func (ses *Session) SetSessionID(sesid uint64) {
	ses.sesid = sesid
}

func (ses *Session) String() string {
	return fmt.Sprintf("session-%d", ses.sesid)
}

func (ses *Session) Begin() error {
	if ses.tx != nil {
		return fmt.Errorf("execute: session already has active transaction")
	}
	ses.tx = ses.e.Begin(ses.sesid)
	return nil
}

func (ses *Session) Commit() error {
	if ses.tx == nil {
		return fmt.Errorf("execute: session does not have active transaction")
	}
	err := ses.tx.Commit(ses.ctx)
	ses.tx = nil
	return err
}

func (ses *Session) Rollback() error {
	if ses.tx == nil {
		return fmt.Errorf("execute: session does not have active transaction")
	}
	err := ses.tx.Rollback()
	ses.tx = nil
	return err
}

type runFunc func(ctx context.Context, ses *Session, e sql.Engine, tx sql.Transaction) error

func (ses *Session) Run(stmt Stmt, run runFunc) error {
	if ses.tx != nil {
		err := ses.tx.NextStmt(ses.ctx)
		if err != nil {
			return err
		}

		return run(ses.ctx, ses, ses.e, ses.tx)
	}
	if _, ok := stmt.(*Begin); ok {
		return ses.Begin()
	}

	tx := ses.e.Begin(ses.sesid)
	err := run(ses.ctx, ses, ses.e, tx)
	if err != nil {
		rerr := tx.Rollback()
		if rerr != nil {
			err = fmt.Errorf("%s; rollback: %s", err, rerr)
		}
	} else {
		err = tx.Commit(ses.ctx)
	}
	return err
}

func (ses *Session) Set(v sql.Identifier, s string) error {
	if v == sql.DATABASE {
		ses.defaultDatabase = sql.ID(s)
	} else if v == sql.SCHEMA {
		ses.defaultSchema = sql.ID(s)
	} else {
		return fmt.Errorf("set: %s not found", v)
	}
	return nil
}

type values struct {
	cols  []sql.Identifier
	rows  [][]sql.Value
	index int
}

func (v *values) Columns() []sql.Identifier {
	return v.cols
}

func (v *values) Close() error {
	v.index = len(v.rows)
	return nil
}

func (v *values) Next(ctx context.Context, dest []sql.Value) error {
	if v.index == len(v.rows) {
		return io.EOF
	}
	copy(dest, v.rows[v.index])
	v.index += 1
	return nil
}

func (_ *values) Delete(ctx context.Context) error {
	return fmt.Errorf("values: rows may not be deleted")
}

func (_ *values) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("values: rows may not be updated")
}

func (ses *Session) Columns(v sql.Identifier) []sql.Identifier {
	if v == sql.DATABASE {
		return []sql.Identifier{sql.DATABASE}
	} else if v == sql.SCHEMA {
		return []sql.Identifier{sql.SCHEMA}
	} else if _, ok := config.Lookup(v.String()); ok {
		return []sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("value")}
	}
	return nil
}

func (ses *Session) Show(v sql.Identifier) (sql.Rows, error) {
	if v == sql.DATABASE {
		return &values{
			cols: []sql.Identifier{sql.DATABASE},
			rows: [][]sql.Value{{sql.StringValue(ses.defaultDatabase.String())}},
		}, nil
	} else if v == sql.SCHEMA {
		return &values{
			cols: []sql.Identifier{sql.SCHEMA},
			rows: [][]sql.Value{{sql.StringValue(ses.defaultSchema.String())}},
		}, nil
	} else if cv, ok := config.Lookup(v.String()); ok {
		return &values{
			cols: []sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("value")},
			rows: [][]sql.Value{
				{sql.StringValue(cv.Name()), sql.StringValue(cv.By()), sql.StringValue(cv.Val())},
			},
		}, nil
	}
	return nil, fmt.Errorf("show: %s not found", v)
}

func (ses *Session) ResolveTableName(tn sql.TableName) sql.TableName {
	if tn.Database == 0 {
		tn.Database = ses.defaultDatabase
		if tn.Schema == 0 {
			tn.Schema = ses.defaultSchema
		}
	}
	return tn
}

func (ses *Session) ResolveSchemaName(sn sql.SchemaName) sql.SchemaName {
	if sn.Database == 0 {
		sn.Database = ses.defaultDatabase
	}
	return sn
}

func (ses *Session) PlanParameter(num int) (*sql.Value, error) {
	return nil, errors.New("engine: unexpected parameter, not preparing a statement")
}
