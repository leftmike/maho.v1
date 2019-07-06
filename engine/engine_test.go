package engine

import (
	"testing"
)

/*
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

func (te *testEngine) AttachDatabase(svcs Services, name sql.Identifier, path string,
	options Options) (Database, error) {

	<-te.done
	te.op("AttachDatabase", name.String(), path)
	return &testDatabase{te, name}, nil
}

func (te *testEngine) CreateDatabase(svcs Services, name sql.Identifier, path string,
	options Options) (Database, error) {

	te.op("CreateDatabase", name.String(), path)
	return &testDatabase{te, name}, nil
}

type testDatabase struct {
	te   *testEngine
	name sql.Identifier
}

func (tdb *testDatabase) Message() string {
	tdb.te.op("Message")
	return ""
}

func (tdb *testDatabase) LookupTable(ses Session, tx interface{}, tblname sql.Identifier) (Table,
	error) {

	_ = tx.(*tcontext)
	tdb.te.op("LookupTable", tblname.String())
	return nil, nil
}

func (tdb *testDatabase) CreateTable(ses Session, tx interface{}, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	_ = tx.(*tcontext)
	tdb.te.op("CreateTable", tblname.String())
	return nil
}

func (tdb *testDatabase) DropTable(ses Session, tx interface{}, tblname sql.Identifier,
	exists bool) error {

	_ = tx.(*tcontext)
	tdb.te.op("DropTable", tblname.String(), fmt.Sprintf("%v", exists))
	return nil
}

func (tdb *testDatabase) ListTables(ses Session, tx interface{}) ([]TableEntry, error) {
	tdb.te.op("ListTables")
	return nil, nil
}

func (tdb *testDatabase) Begin(lkr fatlock.Locker) interface{} {
	tdb.te.op("Begin")
	return &tcontext{tdb}
}

func (tdb *testDatabase) Commit(ses Session, tx interface{}) error {
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

const (
	dbCantClose  = "db-cant-close"
	dbCloseError = "db-close-error"
)

func (tdb *testDatabase) CanClose(drop bool) bool {
	tdb.te.op("CanClose", tdb.name.String(), fmt.Sprintf("%t", drop))
	return tdb.name != sql.ID(dbCantClose)
}

func (tdb *testDatabase) Close(drop bool) error {
	if drop {
		<-tdb.te.done
	}

	tdb.te.op("Close", tdb.name.String(), fmt.Sprintf("%t", drop))
	if tdb.name == sql.ID(dbCloseError) {
		return errors.New("close error")
	}
	return nil
}

type tcontext struct {
	tdb *testDatabase
}

type session struct{}

func (_ session) Context() context.Context {
	return nil
}

func checkDatabaseState(t *testing.T, m *Manager, state databaseState, name sql.Identifier) {
	t.Helper()

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if de, ok := m.databases[name]; ok {
		if de.state != state {
			t.Errorf("database %s.state: got %s want %s", name, de.state, state)
		}
	} else {
		t.Errorf("database %s not found", name)
	}
}

func checkNotFound(t *testing.T, m *Manager, name sql.Identifier) {
	t.Helper()

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if _, ok := m.databases[name]; ok {
		t.Errorf("database %s was found", name)
	}
}

func registerEngine() (*Manager, *testEngine) {
	te := &testEngine{}
	m := NewManager("testdata", te)
	return m, te
}
*/
func TestEngine(t *testing.T) {
	/* XXX
	m, te := registerEngine()
	te.done = make(chan struct{})
	db1 := "db-1"
	db2 := "db-2"

	err := m.AttachDatabase(sql.ID(db1), nil)
	if err != nil {
		t.Errorf("AttachDatabase(db1) failed with %s", err)
	}
	checkDatabaseState(t, m, Attaching, sql.ID(db1))
	te.done <- struct{}{}

	time.Sleep(50 * time.Millisecond)
	checkDatabaseState(t, m, Running, sql.ID(db1))

	err = m.CreateDatabase(sql.ID(db2), Options{})
	if err != nil {
		t.Errorf("CreateDatabase(db2) failed with %s", err)
	}
	checkDatabaseState(t, m, Running, sql.ID(db2))

	err = m.CreateDatabase(sql.ID(dbCantClose), Options{})
	if err != nil {
		t.Errorf("CreateDatabase(dbCantClose) failed with %s", err)
	}
	checkDatabaseState(t, m, Running, sql.ID(dbCantClose))

	err = m.CreateDatabase(sql.ID(dbCloseError), Options{})
	if err != nil {
		t.Errorf("CreateDatabase(dbCloseError) failed with %s", err)
	}
	checkDatabaseState(t, m, Running, sql.ID(dbCloseError))

	err = m.DetachDatabase(sql.ID(db2), Options{})
	if err != nil {
		t.Errorf("DetachDatabase(db2) failed with %s", err)
	}
	checkNotFound(t, m, sql.ID(db2))

	err = m.DetachDatabase(sql.ID(dbCantClose), Options{})
	if err == nil {
		t.Errorf("DetachDatabase(dbCantClose) did not fail")
	}
	checkDatabaseState(t, m, Running, sql.ID(dbCantClose))

	err = m.DetachDatabase(sql.ID(dbCloseError), Options{})
	if err == nil {
		t.Errorf("DetachDatabase(dbCloseError) did not fail")
	}
	checkDatabaseState(t, m, ErrorDetaching, sql.ID(dbCloseError))

	err = m.DropDatabase(sql.ID(db1), false, nil)
	if err != nil {
		t.Errorf("DropDatabase(db1) failed with %s", err)
	}
	checkDatabaseState(t, m, Dropping, sql.ID(db1))
	te.done <- struct{}{}

	time.Sleep(50 * time.Millisecond)
	checkNotFound(t, m, sql.ID(db1))

	te.checkOps(t, []testOp{
		{op: "AttachDatabase", args: []string{db1, filepath.Join("testdata", db1)}},
		{op: "CreateDatabase", args: []string{db2, filepath.Join("testdata", db2)}},
		{op: "CreateDatabase", args: []string{dbCantClose, filepath.Join("testdata", dbCantClose)}},
		{op: "CreateDatabase",
			args: []string{dbCloseError, filepath.Join("testdata", dbCloseError)}},
		{op: "CanClose", args: []string{db2, "false"}},
		{op: "Close", args: []string{db2, "false"}},
		{op: "CanClose", args: []string{dbCantClose, "false"}},
		{op: "CanClose", args: []string{dbCloseError, "false"}},
		{op: "Close", args: []string{dbCloseError, "false"}},
		{op: "CanClose", args: []string{db1, "true"}},
		{op: "Close", args: []string{db1, "true"}},
	})
	*/
}

func TestDatabase(t *testing.T) {
	/*
		m, te := registerEngine()
		db := "db"

		err := m.CreateDatabase(sql.ID(db), Options{})
		if err != nil {
			t.Errorf("CreateDatabase() failed with %s", err)
		}
		te.checkOps(t, []testOp{
			{op: "CreateDatabase", args: []string{db, filepath.Join("testdata", db)}},
		})

		tx := m.Begin(0)
		ses := session{}
		err = m.CreateTable(ses, tx, sql.ID(db), sql.ID("table1"), nil, nil)
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

		tx = m.Begin(0)
		ses = session{}
		_, err = m.LookupTable(ses, tx, sql.ID(db), sql.ID("table1"))
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
	*/
}
