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

var lockSharing = [5][5]bool{
	ACCESS:          [5]bool{ACCESS: true, ROW_MODIFY: true, METADATA_MODIFY: true},
	ROW_MODIFY:      [5]bool{ACCESS: true, ROW_MODIFY: true},
	METADATA_MODIFY: [5]bool{ACCESS: true},
	EXCLUSIVE:       [5]bool{},
}

type lockKey struct {
	db, tbl sql.Identifier
}

type lock struct {
	level LockLevel
	obj   *object
}

type object struct {
	level       LockLevel
	key         lockKey
	locks       map[*LockerState]*lock
	firstWaiter *LockerState
	lastWaiter  *LockerState
}

type Locker interface {
	LockerState() *LockerState
}

type LockerState struct {
	locks      map[lockKey]*lock
	nextWaiter *LockerState
}

var (
	mutex   sync.Mutex
	objects = map[lockKey]*object{}
)

func canShareLock(obj *object, ll LockLevel) bool {
	if obj.firstWaiter != nil {
		return false
	}
	return lockSharing[obj.level][ll]
}

func canIncreaseLock(obj *object, ll LockLevel) bool {
	if len(obj.locks) == 1 {
		return true
	}
	return canShareLock(obj, ll)
}

func addLock(obj *object, ls *LockerState, ll LockLevel) {
	lk := &lock{
		level: ll,
		obj:   obj,
	}
	ls.locks[obj.key] = lk
	obj.locks[ls] = lk
	if ll > obj.level {
		obj.level = ll
	}
}

func LockTable(lkr Locker, db, tbl sql.Identifier, ll LockLevel) error {
	ls := lkr.LockerState()
	if ls.locks == nil {
		ls.locks = map[lockKey]*lock{}
	}
	key := lockKey{db, tbl}

	mutex.Lock()
	defer mutex.Unlock()

	lk, ok := ls.locks[key]
	if ok {
		if ll <= lk.level {
			return nil // Already have the object locked at a sufficient level.
		} else if canIncreaseLock(lk.obj, ll) {
			lk.level = ll
			if ll > lk.obj.level {
				lk.obj.level = ll
			}
			return nil
		}

		//releaseLock(lk)
		// XXX: wait for the lock to become available
		return fmt.Errorf("fatlock: waiting on locks not implemented")
	}

	obj, ok := objects[key]
	if ok {
		if canShareLock(obj, ll) {
			addLock(obj, ls, ll)
			return nil
		}

		// XXX: wait for the lock to become available
		return fmt.Errorf("fatlock: waiting on locks not implemented")
	}

	obj = &object{
		level: ll,
		key:   key,
		locks: map[*LockerState]*lock{},
	}
	objects[key] = obj

	addLock(obj, ls, ll)
	return nil
}

func releaseLock(ls *LockerState, lk *lock) {
	obj := lk.obj
	delete(obj.locks, ls)
	if len(obj.locks) == 0 {
		if obj.firstWaiter == nil {
			delete(objects, obj.key)
		} else {
			// XXX: wake up any waiters who can now lock the object
		}
	} else {
		obj.level = 0
		for _, lk := range obj.locks {
			if lk.level > obj.level {
				obj.level = lk.level
			}
		}
		if obj.firstWaiter == nil {
			// XXX: wake up any waiters who now have a sharable lock request
		}
	}
}

func ReleaseLocks(lkr Locker) error {
	mutex.Lock()
	defer mutex.Unlock()

	ls := lkr.LockerState()
	for _, lk := range ls.locks {
		releaseLock(ls, lk)
	}
	return nil
}
