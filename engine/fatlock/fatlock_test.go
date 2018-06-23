package fatlock_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/sql"
)

type testLocker struct {
	ses         *session
	lockerState fatlock.LockerState
}

func (tl *testLocker) LockerState() *fatlock.LockerState {
	return &tl.lockerState
}

func (tl *testLocker) String() string {
	return fmt.Sprintf("locker-%d", tl.ses.id)
}

type session struct {
	id int
	tl *testLocker
}

func (ses *session) Context() context.Context {
	return nil
}

func (ses *session) DefaultEngine() string {
	return ""
}

func (ses *session) DefaultDatabase() sql.Identifier {
	return 0
}

func (ses *session) String() string {
	return fmt.Sprintf("session-%d", ses.id)
}

var sessions [10]session

func getSession(ses int) *session {
	sessions[ses].id = ses
	return &sessions[ses]
}

type testStep interface {
	step(t *testing.T)
}

type stepLockTable struct {
	ses  int
	tbl  sql.Identifier
	ll   fatlock.LockLevel
	fail bool
	wg   *sync.WaitGroup
}

func (slt stepLockTable) lockTable(t *testing.T, ses *session) {
	t.Helper()

	err := fatlock.LockTable(ses, ses.tl, sql.ID("db"), slt.tbl, slt.ll)
	if slt.fail {
		if err == nil {
			t.Errorf("LockTable(%s, %s, %s) did not fail", ses, ses.tl, slt.ll)
		}
	} else if err != nil {
		t.Errorf("LockTable(%s, %s, %s) failed with %s", ses, ses.tl, slt.ll, err)
	}
}

func (slt stepLockTable) step(t *testing.T) {
	t.Helper()

	ses := getSession(slt.ses)
	if ses.tl == nil {
		ses.tl = &testLocker{ses: ses}
	}

	if slt.wg != nil {
		slt.wg.Add(1)

		go func() {
			defer slt.wg.Done()

			slt.lockTable(t, ses)
		}()
	} else {
		slt.lockTable(t, ses)
	}
}

type stepReleaseLocks struct {
	ses  int
	fail bool
	keep bool
}

func (srl stepReleaseLocks) step(t *testing.T) {
	t.Helper()

	ses := getSession(srl.ses)
	err := fatlock.ReleaseLocks(ses.tl)
	if srl.fail {
		if err == nil {
			t.Errorf("ReleaseLocks(%s) did not fail", ses.tl)
		}
	} else if err != nil {
		t.Errorf("ReleaseLocks(%s) failed with %s", ses.tl, err)
	}
	if !srl.keep {
		ses.tl = nil
	}
}

type stepWait struct {
	wg *sync.WaitGroup
}

func (sw stepWait) step(t *testing.T) {
	t.Helper()

	sw.wg.Wait()
}

type stepLocks []fatlock.Lock

func (sl stepLocks) Len() int {
	return len(sl)
}

func (sl stepLocks) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}

func (sl stepLocks) Less(i, j int) bool {
	if sl[i].Key < sl[j].Key {
		return true
	} else if sl[i].Key > sl[j].Key {
		return false
	}

	if sl[i].Locker < sl[j].Locker {
		return true
	} else if sl[i].Locker > sl[j].Locker {
		return false
	}

	if sl[i].Level < sl[j].Level {
		return true
	} else if sl[i].Level > sl[j].Level {
		return false
	}

	return sl[i].Place < sl[j].Place
}

func (sl stepLocks) step(t *testing.T) {
	t.Helper()

	lks := fatlock.Locks()

	sort.Sort(sl)
	sort.Sort((stepLocks)(lks))

	wnt := ([]fatlock.Lock)(sl)
	if !reflect.DeepEqual(lks, wnt) {
		t.Errorf("Locks() got %#v want %#v", lks, wnt)
	}
}

type stepSleep struct{}

func (_ stepSleep) step(t *testing.T) {
	t.Helper()

	time.Sleep(20 * time.Millisecond)
}

func TestFatlock1(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ACCESS},
		stepLocks{{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ACCESS}},
		stepReleaseLocks{ses: 0},

		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ACCESS},
		stepLocks{{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ACCESS}},
		stepReleaseLocks{ses: 0, keep: true},
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ACCESS, fail: true},
		stepLocks(nil),
		stepReleaseLocks{ses: 0, fail: true},

		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ACCESS},
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ACCESS},
		stepLockTable{ses: 1, tbl: tbl1, ll: fatlock.ACCESS},
		stepLockTable{ses: 2, tbl: tbl1, ll: fatlock.ACCESS},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ACCESS},
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ACCESS},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.ACCESS},
		},
		stepReleaseLocks{ses: 1},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ACCESS},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.ACCESS},
		},
		stepReleaseLocks{ses: 2},
		stepLocks{{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ACCESS}},
		stepReleaseLocks{ses: 0},
		stepLocks(nil),

		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ACCESS},
		stepLocks{{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ACCESS}},
		stepLockTable{ses: 1, tbl: tbl1, ll: fatlock.ROW_MODIFY},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ACCESS},
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY},
		},
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ROW_MODIFY},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ROW_MODIFY},
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY},
		},
		stepLockTable{ses: 2, tbl: tbl1, ll: fatlock.ACCESS},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ROW_MODIFY},
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.ACCESS},
		},
		stepReleaseLocks{ses: 1},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ROW_MODIFY},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.ACCESS},
		},
		stepReleaseLocks{ses: 2},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ROW_MODIFY},
		},
		stepReleaseLocks{ses: 0},

		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ACCESS},
		stepLockTable{ses: 1, tbl: tbl1, ll: fatlock.METADATA_MODIFY},
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ROW_MODIFY, fail: true},
		stepLockTable{ses: 2, tbl: tbl1, ll: fatlock.ACCESS},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ACCESS},
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.METADATA_MODIFY},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.ACCESS},
		},
		stepReleaseLocks{ses: 2},
		stepReleaseLocks{ses: 1},
		stepReleaseLocks{ses: 0},

		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ACCESS},
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.METADATA_MODIFY},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.METADATA_MODIFY},
		},
		stepReleaseLocks{ses: 0},
	}

	for _, ts := range steps {
		ts.step(t)
	}
}

func TestFatlock2(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	var wg sync.WaitGroup
	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.METADATA_MODIFY},
		stepLockTable{ses: 1, tbl: tbl1, ll: fatlock.ROW_MODIFY, wg: &wg},
		stepSleep{},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.METADATA_MODIFY},
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY, Place: 1},
		},
		stepReleaseLocks{ses: 0},
		stepWait{wg: &wg},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY},
		},
		stepReleaseLocks{ses: 1},
	}

	for _, ts := range steps {
		ts.step(t)
	}
}

func TestFatlock3(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	var wg sync.WaitGroup
	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.METADATA_MODIFY},
		stepLockTable{ses: 1, tbl: tbl1, ll: fatlock.ROW_MODIFY, wg: &wg},
		stepSleep{},
		stepLockTable{ses: 2, tbl: tbl1, ll: fatlock.ACCESS, wg: &wg},
		stepSleep{},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.METADATA_MODIFY},
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY, Place: 1},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.ACCESS, Place: 2},
		},
		stepReleaseLocks{ses: 0},
		stepWait{wg: &wg},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.ACCESS},
		},
		stepReleaseLocks{ses: 1},
		stepReleaseLocks{ses: 2},
	}

	for _, ts := range steps {
		ts.step(t)
	}
}

func TestFatlock4(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	var wg, wg2 sync.WaitGroup
	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.METADATA_MODIFY},
		stepLockTable{ses: 1, tbl: tbl1, ll: fatlock.ROW_MODIFY, wg: &wg},
		stepLockTable{ses: 2, tbl: tbl1, ll: fatlock.METADATA_MODIFY, wg: &wg2},
		stepSleep{},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.METADATA_MODIFY},
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY, Place: 1},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.METADATA_MODIFY, Place: 2},
		},
		stepReleaseLocks{ses: 0},
		stepWait{wg: &wg},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.METADATA_MODIFY, Place: 1},
		},
		stepReleaseLocks{ses: 1},
		stepWait{wg: &wg2},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.METADATA_MODIFY},
		},
		stepReleaseLocks{ses: 2},
	}

	for _, ts := range steps {
		ts.step(t)
	}
}

func TestFatlock5(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	var wg sync.WaitGroup
	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ROW_MODIFY},
		stepLockTable{ses: 1, tbl: tbl1, ll: fatlock.METADATA_MODIFY, wg: &wg},
		stepSleep{},
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.METADATA_MODIFY, fail: true},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ROW_MODIFY},
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.METADATA_MODIFY, Place: 1},
		},
		stepReleaseLocks{ses: 0},
		stepWait{wg: &wg},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.METADATA_MODIFY},
		},
		stepReleaseLocks{ses: 1},
	}

	for _, ts := range steps {
		ts.step(t)
	}
}

func TestFatlock6(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	var wg sync.WaitGroup
	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: fatlock.ROW_MODIFY},
		stepLockTable{ses: 1, tbl: tbl1, ll: fatlock.ROW_MODIFY},
		stepLockTable{ses: 2, tbl: tbl1, ll: fatlock.METADATA_MODIFY, wg: &wg},
		stepSleep{},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-0", Level: fatlock.ROW_MODIFY},
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.METADATA_MODIFY, Place: 1},
		},
		stepReleaseLocks{ses: 0},
		stepSleep{},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-1", Level: fatlock.ROW_MODIFY},
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.METADATA_MODIFY, Place: 1},
		},
		stepReleaseLocks{ses: 1},
		stepWait{wg: &wg},
		stepLocks{
			{Key: "db.tbl1", Locker: "locker-2", Level: fatlock.METADATA_MODIFY},
		},
		stepReleaseLocks{ses: 2},
	}

	for _, ts := range steps {
		ts.step(t)
	}
}
