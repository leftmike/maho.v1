package fatlock

import (
	"fmt"
	"sync"

	"github.com/leftmike/maho/sql"
)

type LockLevel int

const (
	ACCESS LockLevel = iota + 1
	ROW_MODIFY
	METADATA_MODIFY
	EXCLUSIVE
)

type lockKey struct {
	db, tbl sql.Identifier
}

type lock struct {
	level LockLevel
	obj   *object
}

type object struct {
	lockers map[Locker]struct{}
}

type Locker interface {
	LockerState() *LockerState
}

type LockerState struct {
	locks map[lockKey]*lock
}

var (
	mutex   sync.Mutex
	objects = map[lockKey]*object{}
)

func LockTable(lkr Locker, db, tbl sql.Identifier, ll LockLevel) error {
	ls := lkr.LockerState()
	if ls.locks == nil {
		ls.locks = map[lockKey]*lock{}
	}
	key := lockKey{db, tbl}
	lk, ok := ls.locks[key]
	if ok {
		if ll <= lk.level {
			return nil // Already have the object locked at a sufficient level.
		}
		// XXX: should see if the level can be increased by looking at other lockers.
		lk.level = ll
		return nil
	}

	mutex.Lock()
	defer mutex.Unlock()

	obj, ok := objects[key]
	if ok {
		// XXX: test to see if lock level is sharable with existing locks.
		return fmt.Errorf("fatlock: sharing locks not implemented")
	}

	obj = &object{
		lockers: map[Locker]struct{}{},
	}
	obj.lockers[lkr] = struct{}{}
	objects[key] = obj

	ls.locks[key] = &lock{
		level: ll,
		obj:   obj,
	}
	return nil
}

func ReleaseLocks(lkr Locker) error {
	mutex.Lock()
	defer mutex.Unlock()

	ls := lkr.LockerState()
	for key, lk := range ls.locks {
		delete(lk.obj.lockers, lkr)
		if len(lk.obj.lockers) == 0 {
			delete(objects, key)
		}
	}
	// XXX
	return nil
}
