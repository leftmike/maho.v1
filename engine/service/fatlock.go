package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

type LockLevel int

const (
	ACCESS LockLevel = iota + 1
	ROW_MODIFY
	METADATA_MODIFY
	EXCLUSIVE
)

func (ll LockLevel) String() string {
	switch ll {
	case ACCESS:
		return "ACCESS"
	case ROW_MODIFY:
		return "ROW_MODIFY"
	case METADATA_MODIFY:
		return "METADATA_MODIFY"
	case EXCLUSIVE:
		return "EXCLUSIVE"
	default:
		return fmt.Sprintf("LockLevel(%d)", ll)
	}
}

var lockSharing = [5][5]bool{
	ACCESS:          [5]bool{ACCESS: true, ROW_MODIFY: true, METADATA_MODIFY: true},
	ROW_MODIFY:      [5]bool{ACCESS: true, ROW_MODIFY: true},
	METADATA_MODIFY: [5]bool{ACCESS: true},
	EXCLUSIVE:       [5]bool{},
}

type lockKey struct {
	db, tbl sql.Identifier
}

func (lk lockKey) String() string {
	return fmt.Sprintf("%s.%s", lk.db, lk.tbl)
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
	String() string
}

// LockerState keeps track of the state of a Locker.
type LockerState struct {
	released   bool
	locks      map[lockKey]*lock // The set of locks this Locker currently holds.
	nextWaiter *LockerState      // Used to link the queue of waiters together.
	// Used to notify a Locker to try to aquire a lock; true means the lock is unlocked.
	waitCh    chan bool
	waitLevel LockLevel // Lock level that the Locker is waiting for.
	locker    Locker
}

type LockService struct {
	mutex sync.Mutex

	// The set of locked objects: all of them have at least one lock.
	objects map[lockKey]*object
}

func (svc *LockService) Init(e engine.Engine) {
	svc.objects = map[lockKey]*object{}
	if e != nil {
		e.CreateSystemTable(sql.ID("locks"), svc.makeLocksTable)
	}
}

// canIncreaseLock tests if an existing lock on the object can be increased to a higher lock level.
func canIncreaseLock(obj *object, ll LockLevel) bool {
	if obj.firstWaiter != nil {
		return false
	} else if len(obj.locks) == 1 {
		return true
	}
	return lockSharing[obj.level][ll]
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

func (svc *LockService) waitForLock(ctx context.Context, obj *object, ls *LockerState,
	ll LockLevel) {

	ls.waitLevel = ll
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
		svc.mutex.Unlock()
		unlocked := <-ls.waitCh
		svc.mutex.Lock()

		if unlocked || lockSharing[obj.level][ll] {
			break
		}
	}

	// Remove the locker from the queue of waiters, and notify the next waiter, if there is one,
	// so that it can try to share the lock.
	obj.firstWaiter = ls.nextWaiter
	if obj.firstWaiter == nil {
		obj.lastWaiter = nil
	} else {
		obj.firstWaiter.waitCh <- false
	}
}

// LockSchema locks the schema (specified by sn) for lkr at the specified lock level. It may
// block waiting for a lock.
func (svc *LockService) LockSchema(ctx context.Context, lkr Locker, sn sql.SchemaName,
	ll LockLevel) error {

	return svc.lockObject(ctx, lkr, lockKey{sn.Database, sn.Schema}, ll)
}

// LockTable locks the table (specified by tn) for lkr at the specified lock level. It may
// block waiting for a lock.
func (svc *LockService) LockTable(ctx context.Context, lkr Locker, tn sql.TableName,
	ll LockLevel) error {

	return svc.lockObject(ctx, lkr, lockKey{tn.Database, tn.Table}, ll)
}

func (svc *LockService) lockObject(ctx context.Context, lkr Locker, key lockKey,
	ll LockLevel) error {

	ls := lkr.LockerState()
	if ls.released {
		return fmt.Errorf("service: locker may not be reused")
	}
	if ls.locks == nil {
		ls.locks = map[lockKey]*lock{}
		ls.waitCh = make(chan bool, 1)
		ls.locker = lkr
	}

	svc.mutex.Lock()
	defer svc.mutex.Unlock()

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

		return fmt.Errorf("service: unable to increase level of held lock")
	}

	obj, ok := svc.objects[key]
	if ok {
		if obj.firstWaiter == nil && lockSharing[obj.level][ll] {
			addLock(obj, ls, ll)
			return nil
		}

		svc.waitForLock(ctx, obj, ls, ll)
		addLock(obj, ls, ll)
		return nil
	}

	obj = &object{
		level: ll,
		key:   key,
		locks: map[*LockerState]*lock{},
	}
	svc.objects[key] = obj

	addLock(obj, ls, ll)
	return nil
}

func (svc *LockService) releaseLock(ls *LockerState, lk *lock) {
	obj := lk.obj
	delete(obj.locks, ls)

	if len(obj.locks) == 0 {
		// The object is no longer locked; notify the first waiter if there is one, otherwise
		// delete it from the map of locked objects.
		if obj.firstWaiter == nil {
			delete(svc.objects, obj.key)
		} else {
			obj.level = 0
			obj.firstWaiter.waitCh <- true
		}
	} else {
		// Recompute the maximum level of lock on the object.
		obj.level = 0
		for _, lk := range obj.locks {
			if lk.level > obj.level {
				obj.level = lk.level
			}
		}
		// Notify the first waiter, if there is one, so it can check if it can share the lock.
		if obj.firstWaiter != nil {
			obj.firstWaiter.waitCh <- false
		}
	}
}

// ReleaseLocks will release all locks held by lkr.
func (svc *LockService) ReleaseLocks(lkr Locker) error {
	ls := lkr.LockerState()
	if ls.released {
		return fmt.Errorf("service: locker may not be reused")
	}
	ls.released = true

	svc.mutex.Lock()
	defer svc.mutex.Unlock()

	for _, lk := range ls.locks {
		svc.releaseLock(ls, lk)
	}
	return nil
}

type Lock struct {
	Key    string
	Locker string
	Level  LockLevel
	Place  int // If waiting, place in the queue (one based). Otherwise, (the lock is held) zero.
}

// Locks returns all locks.
func (svc *LockService) Locks() []Lock {
	svc.mutex.Lock()
	defer svc.mutex.Unlock()

	var locks []Lock
	for _, o := range svc.objects {
		key := o.key.String()

		// Held locks.
		for ls, lk := range o.locks {
			locks = append(locks, Lock{
				Key:    key,
				Locker: ls.locker.String(),
				Level:  lk.level,
			})
		}

		// Waiting for a lock.
		ls := o.firstWaiter
		for pl := 1; ls != nil; pl += 1 {
			locks = append(locks, Lock{
				Key:    key,
				Locker: ls.locker.String(),
				Level:  ls.waitLevel,
				Place:  pl,
			})

			ls = ls.nextWaiter
		}
	}

	return locks
}

func (svc *LockService) makeLocksTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	values := [][]sql.Value{}

	for _, lk := range svc.Locks() {
		var place sql.Value
		if lk.Place > 0 {
			place = sql.Int64Value(lk.Place)
		}
		values = append(values, []sql.Value{
			sql.StringValue(lk.Key),
			sql.StringValue(lk.Locker),
			sql.StringValue(lk.Level.String()),
			sql.BoolValue(lk.Place == 0),
			place,
		})
	}

	return virtual.MakeTable(tn.String(),
		[]sql.Identifier{sql.ID("key"), sql.ID("locker"), sql.ID("level"), sql.ID("held"),
			sql.ID("place")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.BoolColType,
			sql.NullInt64ColType}, values), nil
}
