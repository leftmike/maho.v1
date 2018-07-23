package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/sql"
)

type testOp struct {
	op   string
	args []string
}

type testEngine struct {
	done chan struct{}
	ops  []testOp
}

func (te *testEngine) op(op string, args ...string) {
	te.ops = append(te.ops, testOp{op, args})
}

func (te *testEngine) checkOps(t *testing.T, ops []testOp) {
	t.Helper()

	for idx, op := range te.ops {
		if idx == len(ops) {
			t.Error("too many ops")
			break
		}
		if op.op != ops[idx].op {
			t.Errorf("%d: got %s want %s", idx, op.op, ops[idx].op)
		} else if !reflect.DeepEqual(op.args, ops[idx].args) {
			t.Errorf("%d: %s: got %#v want %#v", idx, op.op, op.args, ops[idx].args)
		}
	}

	te.ops = nil
}

func (te *testEngine) AttachDatabase(name sql.Identifier, path string, options Options) (Database,
	error) {

	<-te.done
	te.op("AttachDatabase", name.String(), path)
	return &testDatabase{te}, nil
}

func (te *testEngine) CreateDatabase(name sql.Identifier, path string, options Options) (Database,
	error) {

	te.op("CreateDatabase", name.String(), path)
	return &testDatabase{te}, nil
}

type testDatabase struct {
	te *testEngine
}

func (tdb *testDatabase) Message() string {
	tdb.te.op("Message")
	return ""
}

func (tdb *testDatabase) LookupTable(ses db.Session, tx interface{},
	tblname sql.Identifier) (db.Table, error) {

	_ = tx.(*tcontext)
	tdb.te.op("LookupTable", tblname.String())
	return nil, nil
}

func (tdb *testDatabase) CreateTable(ses db.Session, tx interface{}, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	_ = tx.(*tcontext)
	tdb.te.op("CreateTable", tblname.String())
	return nil
}

func (tdb *testDatabase) DropTable(ses db.Session, tx interface{}, tblname sql.Identifier,
	exists bool) error {

	_ = tx.(*tcontext)
	tdb.te.op("DropTable", tblname.String(), fmt.Sprintf("%v", exists))
	return nil
}

func (tdb *testDatabase) ListTables(ses db.Session, tx interface{}) ([]TableEntry, error) {
	tdb.te.op("ListTables")
	return nil, nil
}

func (tdb *testDatabase) Begin(lkr fatlock.Locker) interface{} {
	tdb.te.op("Begin")
	return &tcontext{tdb}
}

func (tdb *testDatabase) Commit(ses db.Session, tx interface{}) error {
	tctx := tx.(*tcontext)
	if tctx.tdb != tdb {
		panic("tctx.tdb != tdb")
	}
	tdb.te.op("Commit")
	return nil
}

func (tdb *testDatabase) Rollback(tx interface{}) error {
	tctx := tx.(*tcontext)
	if tctx.tdb != tdb {
		panic("tctx.tdb != tdb")
	}
	tdb.te.op("Rollback")
	return nil
}

func (tdb *testDatabase) NextStmt(tx interface{}) {
	tctx := tx.(*tcontext)
	if tctx.tdb != tdb {
		panic("tctx.tdb != tdb")
	}
	tdb.te.op("NextStmt")
}

type tcontext struct {
	tdb *testDatabase
}

type session struct {
	eng  string
	name sql.Identifier
}

func (ses *session) Context() context.Context {
	return nil
}

func (ses *session) DefaultEngine() string {
	return ses.eng
}

func (ses *session) DefaultDatabase() sql.Identifier {
	return ses.name
}

func checkDatabaseState(t *testing.T, m *Manager, state databaseState, name sql.Identifier) {
	t.Helper()

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if de, ok := m.databases[name]; ok {
		if de.state != state {
			t.Errorf("database(%s).state: got %s want %s", name, de.state, state)
		}
	} else {
		t.Errorf("database(%s) not found", name)
	}
}

func registerEngine() (*Manager, *testEngine) {
	te := &testEngine{}
	m := NewManager(map[string]Engine{
		"test": te,
	})
	return m, te
}

func TestEngine(t *testing.T) {
	m, te := registerEngine()
	te.done = make(chan struct{})
	db1 := "db-1"
	db2 := "db-2"

	err := m.AttachDatabase("test", sql.ID(db1), nil)
	if err != nil {
		t.Fatalf("AttachDatabase() failed with %s", err)
	}
	checkDatabaseState(t, m, Attaching, sql.ID(db1))
	te.done <- struct{}{}

	time.Sleep(50 * time.Millisecond)
	checkDatabaseState(t, m, Running, sql.ID(db1))

	err = m.CreateDatabase("test", sql.ID(db2), Options{sql.WAIT: "true"})
	if err != nil {
		t.Errorf("CreateDatabase() failed with %s", err)
	}
	checkDatabaseState(t, m, Running, sql.ID(db2))

	te.checkOps(t, []testOp{
		{op: "AttachDatabase", args: []string{db1, filepath.Join("testdata", db1)}},
		{op: "CreateDatabase", args: []string{db2, filepath.Join("testdata", db2)}},
	})
}

func TestDatabase(t *testing.T) {
	m, te := registerEngine()
	db := "db"

	err := m.CreateDatabase("test", sql.ID(db), Options{sql.WAIT: "true"})
	if err != nil {
		t.Errorf("CreateDatabase() failed with %s", err)
	}
	te.checkOps(t, []testOp{
		{op: "CreateDatabase", args: []string{db, filepath.Join("testdata", db)}},
	})

	tx := m.Begin()
	ses := &session{
		eng:  "test",
		name: sql.ID(db),
	}
	err = m.CreateTable(ses, tx, 0, sql.ID("table1"), nil, nil)
	if err != nil {
		t.Errorf("CreateTable(table1) failed with %s", err)
	}
	tx.NextStmt()
	tx.NextStmt()
	err = tx.Commit(ses)
	if err != nil {
		t.Errorf("Commit() failed with %s", err)
	}
	te.checkOps(t, []testOp{
		{op: "Begin"},
		{op: "CreateTable", args: []string{"table1"}},
		{op: "NextStmt"},
		{op: "NextStmt"},
		{op: "Commit"},
	})

	tx = m.Begin()
	ses = &session{
		eng:  "test",
		name: sql.ID(db),
	}
	_, err = m.LookupTable(ses, tx, 0, sql.ID("table1"))
	if err != nil {
		t.Errorf("LookupTable(table1) failed with %s", err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Errorf("Rollback() failed with %s", err)
	}
	te.checkOps(t, []testOp{
		{op: "Begin"},
		{op: "LookupTable", args: []string{"table1"}},
		{op: "Rollback"},
	})
}
