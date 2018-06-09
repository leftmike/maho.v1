package fatlock_test

import (
	"testing"

	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/sql"
)

type testLocker struct {
	lockerState fatlock.LockerState
}

func (tl *testLocker) LockerState() *fatlock.LockerState {
	return &tl.lockerState
}

func TestFatlock(t *testing.T) {
	var tl testLocker

	err := fatlock.LockTable(&tl, sql.ID("db"), sql.ID("tbl1"), fatlock.ACCESS)
	if err != nil {
		t.Errorf("LockTable() failed with %s", err)
	}

	err = fatlock.ReleaseLocks(&tl)
	if err != nil {
		t.Errorf("ReleaseLocks() failed with %s", err)
	}
}
