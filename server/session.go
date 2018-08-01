package server

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

type Session struct {
	mgr             *engine.Manager
	defaultEngine   string
	defaultDatabase sql.Identifier
	tx              *engine.Transaction
}

func NewSession(mgr *engine.Manager, eng string, name sql.Identifier) *Session {
	return &Session{
		mgr:             mgr,
		defaultEngine:   eng,
		defaultDatabase: name,
	}
}

func (ses *Session) Context() context.Context {
	return nil
}

func (ses *Session) DefaultEngine() string {
	return ses.defaultEngine
}

func (ses *Session) DefaultDatabase() sql.Identifier {
	return ses.defaultDatabase
}

func (ses *Session) Manager() *engine.Manager {
	return ses.mgr
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

func (ses *Session) Run(stmt parser.Stmt,
	run func(tx *engine.Transaction, stmt parser.Stmt) error) error {

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

func (ses *Session) Set(v sql.Identifier, s string) error {
	if v == sql.DATABASE {
		ses.defaultDatabase = sql.ID(s)
	} else if v == sql.ENGINE {
		ses.defaultEngine = s
	} else {
		return fmt.Errorf("set: %s not found", v)
	}
	return nil
}
