package evaluate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type testStore struct {
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

func (st *testStore) CreateDatabase(dbname sql.Identifier,
	options map[sql.Identifier]string) error {

	st.t.Error("CreateDatabase should never be called")
	return nil
}

func (st *testStore) DropDatabase(dbname sql.Identifier, ifExists bool,
	options map[sql.Identifier]string) error {

	st.t.Error("DropDatabase should never be called")
	return nil
}

func (st *testStore) CreateSchema(ctx context.Context, tx sql.Transaction,
	sn sql.SchemaName) error {

	st.t.Error("CreateSchema should never be called")
	return nil
}

func (st *testStore) DropSchema(ctx context.Context, tx sql.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	st.t.Error("DropSchema should never be called")
	return nil
}

func (st *testStore) LookupTable(ctx context.Context, tx sql.Transaction,
	tn sql.TableName) (engine.Table, *engine.TableType, error) {

	st.t.Error("LookupTable should never be called")
	return nil, nil, nil
}

func (st *testStore) CreateTable(ctx context.Context, tx sql.Transaction, tn sql.TableName,
	tt *engine.TableType, ifNotExists bool) error {

	st.t.Error("CreateTable should never be called")
	return nil
}

func (st *testStore) DropTable(ctx context.Context, tx sql.Transaction, tn sql.TableName,
	ifExists bool) error {

	st.t.Error("DropTable should never be called")
	return nil
}

func (st *testStore) AddIndex(ctx context.Context, tx sql.Transaction, tn sql.TableName,
	tt *engine.TableType) error {

	st.t.Error("AddIndex should never be called")
	return nil
}

func (st *testStore) RemoveIndex(ctx context.Context, tx sql.Transaction, tn sql.TableName,
	tt *engine.TableType, rdx int) error {

	st.t.Error("RemoveIndex should never be called")
	return nil
}

func (st *testStore) Begin(sesid uint64) sql.Transaction {
	if len(st.transactions) == 0 {
		st.t.Error("Begin called too many times on engine")
	}
	tx := st.transactions[0]
	st.transactions = st.transactions[1:]
	tx.t = st.t
	tx.sesid = sesid
	return &tx
}

func (st *testStore) ListDatabases(ctx context.Context, tx sql.Transaction) ([]sql.Identifier,
	error) {

	st.t.Error("ListDatabases should never be called")
	return nil, nil
}

func (st *testStore) ListSchemas(ctx context.Context, tx sql.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	st.t.Error("ListSchemas should never be called")
	return nil, nil
}

func (st *testStore) ListTables(ctx context.Context, tx sql.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	st.t.Error("ListTables should never be called")
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
	st := &testStore{
		t: t,
		transactions: []testTransaction{
			{wantCommit: true},
		},
	}

	ses := evaluate.Session{Engine: engine.NewEngine(st)}
	ses.SetSessionID(123)
	if ses.String() != "session-123" {
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
	st := &testStore{
		t: t,
		transactions: []testTransaction{
			{wantRollback: true},
		},
	}

	ses := evaluate.Session{Engine: engine.NewEngine(st)}
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
	st := &testStore{
		t: t,
		transactions: []testTransaction{
			{wantCommit: true, nextStmtAllowed: 2},
		},
	}

	ses := evaluate.Session{Engine: engine.NewEngine(st)}
	err := ses.Begin()
	if err != nil {
		t.Errorf("Begin failed with %s", err)
	}

	var ran bool
	err = ses.Run(nil,
		func(tx sql.Transaction, stmt evaluate.Stmt) error {
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
		func(tx sql.Transaction, stmt evaluate.Stmt) error {
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
	st := &testStore{
		t: t,
		transactions: []testTransaction{
			{wantCommit: true},
			{wantRollback: true},
		},
	}

	ses := evaluate.Session{Engine: engine.NewEngine(st)}

	var ran bool
	err := ses.Run(nil,
		func(tx sql.Transaction, stmt evaluate.Stmt) error {
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
		func(tx sql.Transaction, stmt evaluate.Stmt) error {
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
