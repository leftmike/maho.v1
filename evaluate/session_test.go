package evaluate

import (
	"context"
	"errors"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type testEngine struct {
	t            *testing.T
	transactions []testTransaction
}

type testTransaction struct {
	t               *testing.T
	sid             uint64
	wantRollback    bool
	wantCommit      bool
	nextStmtAllowed int
}

func (te *testEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	te.t.Error("CreateSystemTable should never be called")
}

func (te *testEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	te.t.Error("CreateInfoTable should never be called")
}

func (te *testEngine) CreateDatabase(name sql.Identifier, options engine.Options) error {
	te.t.Error("CreateDatabase should never be called")
	return nil
}

func (te *testEngine) DropDatabase(name sql.Identifier, exists bool, options engine.Options) error {
	te.t.Error("DropDatabase should never be called")
	return nil
}

func (te *testEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier) (engine.Table, error) {

	te.t.Error("LookupTable should never be called")
	return nil, nil
}

func (te *testEngine) CreateTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier, cols []sql.Identifier, colTypes []sql.ColumnType) error {

	te.t.Error("CreateTable should never be called")
	return nil
}

func (te *testEngine) DropTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier, exists bool) error {

	te.t.Error("DropTable should never be called")
	return nil
}

func (te *testEngine) Begin(sid uint64) engine.Transaction {
	if len(te.transactions) == 0 {
		te.t.Error("Begin called too many times on engine")
	}
	tx := te.transactions[0]
	te.transactions = te.transactions[1:]
	tx.t = te.t
	tx.sid = sid
	return &tx
}

func (_ *testEngine) IsTransactional() bool {
	return true
}

func (ttx *testTransaction) Commit(ctx context.Context) error {
	if !ttx.wantCommit {
		ttx.t.Error("Commit unexpected")
	}
	ttx.wantCommit = false
	if ttx.nextStmtAllowed != 0 {
		ttx.t.Error("NextStmt not called enough times before Commit")
	}
	return nil
}

func (ttx *testTransaction) Rollback() error {
	if !ttx.wantRollback {
		ttx.t.Error("Rollback unexpected")
	}
	ttx.wantRollback = false
	if ttx.nextStmtAllowed != 0 {
		ttx.t.Error("NextStmt not called enough times before Rollback")
	}
	return nil
}

func (ttx *testTransaction) NextStmt() {
	ttx.nextStmtAllowed -= 1
}

func TestSessionCommit(t *testing.T) {
	te := &testEngine{
		t: t,
		transactions: []testTransaction{
			{wantCommit: true},
		},
	}

	ses := Session{Engine: te}
	ses.SetSID(123)
	if ses.String() != "session-123" || ses.sid != 123 {
		t.Errorf("SetSid: got %s want session-123", ses.String())
	}
	err := ses.Begin()
	if err != nil {
		t.Errorf("Begin failed with %s", err)
	}
	err = ses.Begin()
	if err == nil {
		t.Error("Begin should have failed; already active transaction")
	}
	err = ses.Commit()
	if err != nil {
		t.Errorf("Commit failed with %s", err)
	}
	err = ses.Commit()
	if err == nil {
		t.Error("Commit should have failed; no active transaction")
	}
	err = ses.Rollback()
	if err == nil {
		t.Error("Rollback should have failed; no active transaction")
	}
}

func TestSessionRollback(t *testing.T) {
	te := &testEngine{
		t: t,
		transactions: []testTransaction{
			{wantRollback: true},
		},
	}

	ses := Session{Engine: te}
	err := ses.Begin()
	if err != nil {
		t.Errorf("Begin failed with %s", err)
	}
	err = ses.Rollback()
	if err != nil {
		t.Errorf("Rollback failed with %s", err)
	}
	err = ses.Rollback()
	if err == nil {
		t.Error("Rollback should have failed; no active transaction")
	}
	err = ses.Commit()
	if err == nil {
		t.Error("Commit should have failed; no active transaction")
	}
}

func TestSessionRunExplicit(t *testing.T) {
	te := &testEngine{
		t: t,
		transactions: []testTransaction{
			{wantCommit: true, nextStmtAllowed: 2},
		},
	}

	ses := Session{Engine: te}
	err := ses.Begin()
	if err != nil {
		t.Errorf("Begin failed with %s", err)
	}

	var ran bool
	err = ses.Run(nil,
		func(tx engine.Transaction, stmt Stmt) error {
			ran = true
			return nil
		})
	if err != nil {
		t.Errorf("Run() failed with %s", err)
	}
	if !ran {
		t.Error("Run() func not called")
	}

	ran = false
	wantErr := errors.New("failed")
	err = ses.Run(nil,
		func(tx engine.Transaction, stmt Stmt) error {
			ran = true
			return wantErr
		})
	if err != wantErr {
		t.Errorf("Run() got %v want %v", err, wantErr)
	}
	if !ran {
		t.Error("Run func not called")
	}

	err = ses.Commit()
	if err != nil {
		t.Errorf("Commit failed with %s", err)
	}
}

func TestSessionRunImplicit(t *testing.T) {
	te := &testEngine{
		t: t,
		transactions: []testTransaction{
			{wantCommit: true},
			{wantRollback: true},
		},
	}

	ses := Session{Engine: te}

	var ran bool
	err := ses.Run(nil,
		func(tx engine.Transaction, stmt Stmt) error {
			ran = true
			return nil
		})
	if err != nil {
		t.Errorf("Run() failed with %s", err)
	}
	if !ran {
		t.Error("Run() func not called")
	}

	ran = false
	wantErr := errors.New("failed")
	err = ses.Run(nil,
		func(tx engine.Transaction, stmt Stmt) error {
			ran = true
			return wantErr
		})
	if err != wantErr {
		t.Errorf("Run() got %v want %v", err, wantErr)
	}
	if !ran {
		t.Error("Run func not called")
	}

	err = ses.Commit()
	if err == nil {
		t.Error("Commit should have failed; no active transaction")
	}
	err = ses.Rollback()
	if err == nil {
		t.Error("Rollback should have failed; no active transaction")
	}
}