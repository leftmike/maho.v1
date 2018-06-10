package fatlock

import (
	"fmt"
	"sync"

	"github.com/leftmike/maho/db"
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

// A lock will exist for every Locker that has an object currently locked.
type lock struct {
	level LockLevel
	obj   *object
}

// An object is the thing that can be locked.
type object struct {
	level LockLevel
	key   lockKey
	locks map[*LockerState]*lock // The concurrent locks on the object.

	// Waiters for a lock are maintained in a queue; firstWaiter is the next Locker allowed
	// to try to aquire a lock on the object when notified; lastWaiter is where Lockers are
	// added to the queue; LockerState.nextWaiter is used to link the queue of waiters together.
	firstWaiter *LockerState
	lastWaiter  *LockerState
}

// Locker is something that locks an object.
type Locker interface {
	LockerState() *LockerState
}

// LockerState keeps track of the state of a Locker.
type LockerState struct {
	locks      map[lockKey]*lock // The set of locks this Locker currently holds.
	nextWaiter *LockerState      // Used to link the queue of waiters together.
	waitCh     chan struct{}     // Used to notify a Locker to try to aquire a lock.
}

var (
	mutex sync.Mutex

	// The set of locked objects: all of them have at least one lock.
	objects = map[lockKey]*object{}
)

// canShareLock tests if the lock level can share access with the current lock level on the
// object. Don't use this function (directly) when trying to increase the lock level of a
// currently held lock: use canIncreaseLock instead.
func canShareLock(obj *object, ll LockLevel) bool {
	if obj.firstWaiter != nil {
		return false
	}
	return lockSharing[obj.level][ll]
}

// canIncreaseLock tests if an existing lock on the object can be increased to a higher lock level.
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

func waitForLock(ses db.Session, obj *object, ls *LockerState, ll LockLevel) error {
	// Add the locker to the queue of waiters.
	ls.nextWaiter = nil
	if obj.lastWaiter != nil {
		obj.lastWaiter.nextWaiter = ls
	} else {
		obj.firstWaiter = ls
	}
	obj.lastWaiter = ls

	// Loop, waiting until the lock becomes available. Note that the mutex is locked when this
	// function is called, so unlock it before waiting on the channel.
	for {
		mutex.Unlock()
		<-ls.waitCh
		mutex.Lock()

		if canShareLock(obj, ll) {
			break
		}
	}

	// Remove the locker from the queue of waiters, and notify the next waiter, if there is one.
	obj.firstWaiter = ls.nextWaiter
	if obj.firstWaiter == nil {
		obj.lastWaiter = nil
	} else {
		obj.firstWaiter.waitCh <- struct{}{}
	}

	return nil
}

// LockTable locks the table (specified by db.tbl) for lkr at the specified lock level. It may
// block waiting for a lock.
func LockTable(ses db.Session, lkr Locker, db, tbl sql.Identifier, ll LockLevel) error {
	ls := lkr.LockerState()
	if ls.locks == nil {
		ls.locks = map[lockKey]*lock{}
		ls.waitCh = make(chan struct{}, 1)
	}
	key := lockKey{db, tbl}

	mutex.Lock()
	defer mutex.Unlock()

	lk, ok := ls.locks[key]
	if ok {
		// The lkr already has the object locked.
		if ll <= lk.level {
			return nil // Already have the object locked at a sufficient level.
		} else if canIncreaseLock(lk.obj, ll) {
			// The lock level of the existing lock can be increased.
			lk.level = ll
			if ll > lk.obj.level {
				lk.obj.level = ll
			}
			return nil
		}

		return fmt.Errorf("fatlock: unable to increase level of held lock")
	}

	obj, ok := objects[key]
	if ok {
		if canShareLock(obj, ll) {
			addLock(obj, ls, ll)
			return nil
		}

		err := waitForLock(ses, obj, ls, ll)
		if err != nil {
			return err
		}
		addLock(obj, ls, ll)
		return nil
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
		// The object is no longer locked; notify the first waiter if there is one, otherwise
		// delete it from the map of locked objects.
		if obj.firstWaiter == nil {
			delete(objects, obj.key)
		} else {
			obj.level = 0
			obj.firstWaiter.waitCh <- struct{}{}
		}
	} else {
		// Recompute the level of lock on the object.
		obj.level = 0
		for _, lk := range obj.locks {
			if lk.level > obj.level {
				obj.level = lk.level
			}
		}
		// Notify the first waiter, if there is one, so it can check if it can share the lock.
		if obj.firstWaiter != nil {
			obj.firstWaiter.waitCh <- struct{}{}
		}
	}
}

// ReleaseLocks will release all locks held by lkr.
func ReleaseLocks(lkr Locker) error {
	mutex.Lock()
	defer mutex.Unlock()

	ls := lkr.LockerState()
	for _, lk := range ls.locks {
		releaseLock(ls, lk)
	}
	return nil
}
