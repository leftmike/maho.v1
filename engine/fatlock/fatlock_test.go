package fatlock_test

import (
	"context"
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

type session struct{}

func (ses session) Context() context.Context {
	return nil
}

func (ses session) DefaultEngine() string {
	return ""
}

func (ses session) DefaultDatabase() sql.Identifier {
	return 0
}

func TestFatlock(t *testing.T) {
	var tl testLocker

	ses := session{}
	err := fatlock.LockTable(ses, &tl, sql.ID("db"), sql.ID("tbl1"), fatlock.ACCESS)
	if err != nil {
		t.Errorf("LockTable() failed with %s", err)
	}

	err = fatlock.ReleaseLocks(&tl)
	if err != nil {
		t.Errorf("ReleaseLocks() failed with %s", err)
	}
}
