package evaluate

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type Session struct {
	Engine          engine.Engine
	DefaultDatabase sql.Identifier
	DefaultSchema   sql.Identifier
	User            string
	Type            string
	Addr            string
	Interactive     bool
	sid             uint64
	tx              engine.Transaction
}

func (ses *Session) SetSID(sid uint64) {
	ses.sid = sid
}

func (ses *Session) String() string {
	return fmt.Sprintf("session-%d", ses.sid)
}

func (ses *Session) Context() context.Context {
	return nil
}

func (ses *Session) Begin() error {
	if ses.tx != nil {
		return fmt.Errorf("execute: session already has active transaction")
	}
	ses.tx = ses.Engine.Begin(ses.sid)
	return nil
}

func (ses *Session) Commit() error {
	if ses.tx == nil {
		return fmt.Errorf("execute: session does not have active transaction")
	}
	err := ses.tx.Commit(ses.Context())
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

func (ses *Session) Run(stmt Stmt, run func(tx engine.Transaction, stmt Stmt) error) error {

	if ses.tx != nil {
		ses.tx.NextStmt()
		return run(ses.tx, stmt)
	}

	tx := ses.Engine.Begin(ses.sid)
	err := run(tx, stmt)
	if err != nil {
		rerr := tx.Rollback()
		if rerr != nil {
			err = fmt.Errorf("%s; rollback: %s", err, rerr)
		}
	} else {
		err = tx.Commit(ses.Context())
	}
	return err
}

func (ses *Session) Set(v sql.Identifier, s string) error {
	if v == sql.DATABASE {
		ses.DefaultDatabase = sql.ID(s)
	} else if v == sql.SCHEMA {
		ses.DefaultSchema = sql.ID(s)
	} else {
		return fmt.Errorf("set: %s not found", v)
	}
	return nil
}

type values struct {
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
}

func (v *values) Columns() []sql.Identifier {
	return v.columns
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

func (ses *Session) Show(v sql.Identifier) (engine.Rows, error) {
	if v == sql.DATABASE {
		return &values{
			columns: []sql.Identifier{sql.DATABASE},
			rows:    [][]sql.Value{{sql.StringValue(ses.DefaultDatabase.String())}},
		}, nil
	} else if v == sql.SCHEMA {
		return &values{
			columns: []sql.Identifier{sql.SCHEMA},
			rows:    [][]sql.Value{{sql.StringValue(ses.DefaultSchema.String())}},
		}, nil
	} else if cv, ok := config.Lookup(v.String()); ok {
		return &values{
			columns: []sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("value")},
			rows: [][]sql.Value{
				{sql.StringValue(cv.Name()), sql.StringValue(cv.By()), sql.StringValue(cv.Val())},
			},
		}, nil
	}
	return nil, fmt.Errorf("show: %s not found", v)
}

func (ses *Session) ResolveTableName(tn sql.TableName) sql.TableName {
	if tn.Database == 0 {
		tn.Database = ses.DefaultDatabase
		if tn.Schema == 0 {
			tn.Schema = ses.DefaultSchema
		}
	}
	return tn
}

func (ses *Session) ResolveSchemaName(sn sql.SchemaName) sql.SchemaName {
	if sn.Database == 0 {
		sn.Database = ses.DefaultDatabase
	}
	return sn
}
