package util

import (
	"sync"
)

type RowLocks struct {
	mutex sync.Mutex
	locks map[string]*lock
}

type Locker struct {
	// Locks held by this Locker.
	locks map[string]*lock

	// A Locker can wait on only one lock at a time; nextWaiter is used to link the queue of
	// waiters together.
	nextWaiter *Locker
	// Notify a Locker to try to aquire the lock.
	waitCh chan struct{}
	// Waiting for a write lock.
	waitWrite bool
}

type lock struct {
	mutex sync.Mutex

	// count = 0: lock is available.
	// count = -1: write lock held
	// count > 0: number of read lockers
	count int

	// Waiters for a lock are maintained in a queue; firstWaiter is the next Locker allowed
	// to try to aquire a lock when notified; lastWaiter is where Lockers are added to the queue;
	// Locker.nextWaiter is used to link the queue of waiters together.
	firstWaiter *Locker
	lastWaiter  *Locker
}

func (rl *RowLocks) lookupLock(skey string) *lock {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	if rl.locks == nil {
		rl.locks = map[string]*lock{}
	}

	lk, ok := rl.locks[skey]
	if ok {
		return lk
	}
	lk = &lock{}
	rl.locks[skey] = lk
	return lk
}

func (rl *RowLocks) lock(lkr *Locker, key []byte, write bool) bool {
	if lkr.locks == nil {
		lkr.locks = map[string]*lock{}
	}

	skey := string(key)
	if lk, ok := lkr.locks[skey]; ok {
		// Already locked by this Locker at sufficient level.
		if !write {
			return true
		}

		lk.mutex.Lock()
		defer lk.mutex.Unlock()

		// Already locked by this Locker at sufficient level.
		if lk.count < 0 {
			return true
		}

		// This Locker holds the only read lock; convert it into a write lock.
		if lk.count == 1 && lk.firstWaiter == nil {
			lk.count = -1
			return true
		}

		// Other read locks, can't increase it to a write lock.
		return false
	}

	lk := rl.lookupLock(skey)

	lk.mutex.Lock()
	if lk.firstWaiter == nil {
		if write {
			if lk.count == 0 {
				lk.count = -1
				lkr.locks[skey] = lk
				lk.mutex.Unlock()
				return true
			}
		} else if lk.count >= 0 {
			lk.count += 1
			lkr.locks[skey] = lk
			lk.mutex.Unlock()
			return true
		}
	}

	lkr.nextWaiter = nil
	if lk.lastWaiter != nil {
		lk.lastWaiter.nextWaiter = lkr
	} else {
		lk.firstWaiter = lkr
	}
	lk.lastWaiter = lkr

	if lkr.waitCh == nil {
		lkr.waitCh = make(chan struct{}, 1)
	}
	lkr.waitWrite = write

	lk.mutex.Unlock()
	<-lkr.waitCh
	lk.mutex.Lock()

	if write && lk.count != 0 {
		panic("wait write lock: count != 0")
	}
	if !write && lk.count < 0 {
		panic("wait read lock: count < 0")
	}

	lk.firstWaiter = lkr.nextWaiter
	if lk.firstWaiter == nil {
		lk.lastWaiter = nil
	} else if !write && !lk.firstWaiter.waitWrite {
		lk.firstWaiter.waitCh <- struct{}{}
	}

	if write {
		lk.count = -1
	} else {
		lk.count += 1
	}
	lkr.locks[skey] = lk
	lk.mutex.Unlock()
	return true
}

func (rl *RowLocks) RLock(lkr *Locker, key []byte) {
	rl.lock(lkr, key, false)
}

func (rl *RowLocks) WLock(lkr *Locker, key []byte) bool {
	return rl.lock(lkr, key, true)
}

func (lk *lock) unlock() {
	lk.mutex.Lock()
	defer lk.mutex.Unlock()

	if lk.count > 0 {
		lk.count -= 1
	} else if lk.count == -1 {
		lk.count = 0
	} else {
		panic("unlock: count not >= 0 and not == -1")
	}

	if lk.firstWaiter != nil && lk.count == 0 {
		lk.firstWaiter.waitCh <- struct{}{}
	}
}

func (lkr *Locker) Unlock() {
	for _, lk := range lkr.locks {
		lk.unlock()
	}
	lkr.locks = nil
}
