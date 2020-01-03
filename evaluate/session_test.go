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
	sesid           uint64
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

func (te *testEngine) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	te.t.Error("CreateDatabase should never be called")
	return nil
}

func (te *testEngine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options engine.Options) error {

	te.t.Error("DropDatabase should never be called")
	return nil
}

func (te *testEngine) CreateSchema(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) error {

	te.t.Error("CreateSchema should never be called")
	return nil
}

func (te *testEngine) DropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	te.t.Error("DropSchema should never be called")
	return nil
}

func (te *testEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	te.t.Error("LookupTable should never be called")
	return nil, nil
}

func (te *testEngine) CreateTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	te.t.Error("CreateTable should never be called")
	return nil
}

func (te *testEngine) DropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	te.t.Error("DropTable should never be called")
	return nil
}

func (te *testEngine) CreateIndex(ctx context.Context, tx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	te.t.Error("CreateIndex should never be called")
	return nil
}

func (te *testEngine) DropIndex(ctx context.Context, tx engine.Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	te.t.Error("DropIndex should never be called")
	return nil
}

func (te *testEngine) Begin(sesid uint64) engine.Transaction {
	if len(te.transactions) == 0 {
		te.t.Error("Begin called too many times on engine")
	}
	tx := te.transactions[0]
	te.transactions = te.transactions[1:]
	tx.t = te.t
	tx.sesid = sesid
	return &tx
}

func (te *testEngine) ListDatabases(ctx context.Context,
	tx engine.Transaction) ([]sql.Identifier, error) {

	te.t.Error("ListDatabases should never be called")
	return nil, nil
}

func (te *testEngine) ListSchemas(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	te.t.Error("ListSchemas should never be called")
	return nil, nil
}

func (te *testEngine) ListTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	te.t.Error("ListTables should never be called")
	return nil, nil
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
	ses.SetSessionID(123)
	if ses.String() != "session-123" || ses.sesid != 123 {
		t.Errorf("SetSessionID: got %s want session-123", ses.String())
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
