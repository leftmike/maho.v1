package engine

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/session"
	"github.com/leftmike/maho/sql"
)

type testOp struct {
	op   string
	args []string
}

type testEngine struct {
	cond *sync.Cond
	done bool
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

	te.cond.L.Lock()
	for !te.done {
		te.cond.Wait()
	}
	te.cond.L.Unlock()

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

func (tdb *testDatabase) LookupTable(ctx session.Context, tx interface{},
	tblname sql.Identifier) (db.Table, error) {

	_ = tx.(*tcontext)
	tdb.te.op("LookupTable", tblname.String())
	return nil, nil
}

func (tdb *testDatabase) CreateTable(ctx session.Context, tx interface{}, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	_ = tx.(*tcontext)
	tdb.te.op("CreateTable", tblname.String())
	return nil
}

func (tdb *testDatabase) DropTable(ctx session.Context, tx interface{}, tblname sql.Identifier,
	exists bool) error {

	_ = tx.(*tcontext)
	tdb.te.op("DropTable", tblname.String(), fmt.Sprintf("%v", exists))
	return nil
}

func (tdb *testDatabase) ListTables(ctx session.Context, tx interface{}) ([]TableEntry, error) {
	tdb.te.op("ListTables")
	return nil, nil
}

func (tdb *testDatabase) Begin() interface{} {
	tdb.te.op("Begin")
	return &tcontext{tdb}
}

func (tdb *testDatabase) Commit(ctx session.Context, tx interface{}) error {
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

func (tdb *testDatabase) NextCommand(tx interface{}) {
	tctx := tx.(*tcontext)
	if tctx.tdb != tdb {
		panic("tctx.tdb != tdb")
	}
	tdb.te.op("NextCommand")
}

type tcontext struct {
	tdb *testDatabase
}

func checkDatabaseState(t *testing.T, state databaseState, name sql.Identifier) {
	t.Helper()

	mutex.RLock()
	defer mutex.RUnlock()

	if de, ok := databases[name]; ok {
		if de.state != state {
			t.Errorf("database(%s).state: got %s want %s", name, de.state, state)
		}
	} else {
		t.Errorf("database(%s) not found", name)
	}
}

func TestEngine(t *testing.T) {
	te := &testEngine{
		cond: sync.NewCond(&sync.Mutex{}),
	}
	Register("test", te)

	te.done = false
	err := AttachDatabase("test", sql.ID("db1"), nil)
	if err != nil {
		t.Errorf("AttachDatabase() failed with %s", err)
	}
	checkDatabaseState(t, Attaching, sql.ID("db1"))
	te.done = true
	te.cond.Signal()

	time.Sleep(50 * time.Millisecond)
	checkDatabaseState(t, Running, sql.ID("db1"))

	err = CreateDatabase("test", sql.ID("db2"), Options{sql.WAIT: "true"})
	if err != nil {
		t.Errorf("CreateDatabase() failed with %s", err)
	}
	checkDatabaseState(t, Running, sql.ID("db2"))

	te.checkOps(t, []testOp{
		{op: "AttachDatabase", args: []string{"db1", filepath.Join("testdata", "db1")}},
		{op: "CreateDatabase", args: []string{"db2", filepath.Join("testdata", "db2")}},
	})
}

func TestDatabase(t *testing.T) {
	te := &testEngine{}
	Register("test2", te)

	err := CreateDatabase("test2", sql.ID("db3"), Options{sql.WAIT: "true"})
	if err != nil {
		t.Errorf("CreateDatabase() failed with %s", err)
	}
	te.checkOps(t, []testOp{
		{op: "CreateDatabase", args: []string{"db3", filepath.Join("testdata", "db3")}},
	})

	tx := Begin()
	ctx := session.NewContext("test2", sql.ID("db3"))
	err = CreateTable(ctx, tx, 0, sql.ID("table1"), nil, nil)
	if err != nil {
		t.Errorf("CreateTable(table1) failed with %s", err)
	}
	tx.NextCommand()
	tx.NextCommand()
	err = tx.Commit(ctx)
	if err != nil {
		t.Errorf("Commit() failed with %s", err)
	}
	te.checkOps(t, []testOp{
		{op: "Begin"},
		{op: "CreateTable", args: []string{"table1"}},
		{op: "NextCommand"},
		{op: "NextCommand"},
		{op: "Commit"},
	})

	tx = Begin()
	ctx = session.NewContext("test2", sql.ID("db3"))
	_, err = LookupTable(ctx, tx, 0, sql.ID("table1"))
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
