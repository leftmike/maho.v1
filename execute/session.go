package execute

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type Session struct {
	mgr *engine.Manager
	eng  string
	name sql.Identifier
	tx   *engine.Transaction
}

func NewSession(mgr *engine.Manager, eng string, name sql.Identifier) *Session {
	return &Session{
		mgr: mgr,
		eng:  eng,
		name: name,
	}
}

func (ses *Session) Context() context.Context {
	return nil
}

func (ses *Session) DefaultEngine() string { // XXX: delete
	return ses.eng
}

func (ses *Session) DefaultDatabase() sql.Identifier { // XXX: delete
	return ses.name
}

func (ses *Session) Begin() error {
	if ses.tx != nil {
		return fmt.Errorf("execute: session already has active transaction")
	}
	ses.tx = ses.mgr.Begin()
	return nil
}

func (ses *Session) Commit() error {
	if ses.tx == nil {
		return fmt.Errorf("execute: session does not have active transaction")
	}
	err := ses.tx.Commit(ses)
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

func (ses *Session) Run(stmt Stmt, run func(tx *engine.Transaction, stmt Stmt) error) error {
	if ses.tx != nil {
		ses.tx.NextStmt()
		return run(ses.tx, stmt)
	}

	tx := ses.mgr.Begin()
	err := run(tx, stmt)
	if err != nil {
		rerr := tx.Rollback()
		if rerr != nil {
			err = fmt.Errorf("%s; rollback: %s", err, rerr)
		}
	} else {
		err = tx.Commit(ses)
	}
	return err
}

func (ses *Session) AttachDatabase(name sql.Identifier, options engine.Options) error {
	return ses.mgr.AttachDatabase(ses.eng, name, options)
}

func (ses *Session) CreateDatabase(name sql.Identifier, options engine.Options) error {
	return ses.mgr.CreateDatabase(ses.eng, name, options)
}

func (ses *Session) DetachDatabase(name sql.Identifier) error {
	return ses.mgr.DetachDatabase(name)
}

func (ses *Session) CreateTable(tx *engine.Transaction, dbname, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	return ses.mgr.CreateTable(ses, tx, dbname, tblname, cols, colTypes)
}

func (ses *Session) DropTable(tx *engine.Transaction, dbname, tblname sql.Identifier,
	exists bool) error {

	return ses.mgr.DropTable(ses, tx, dbname, tblname, exists)
}

func (ses *Session) LookupTable(tx *engine.Transaction, dbname,
	tblname sql.Identifier) (engine.Table, error) {

	return ses.mgr.LookupTable(ses, tx, dbname, tblname)
}
