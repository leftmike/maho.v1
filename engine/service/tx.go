package service

import (
	"fmt"
	"sync"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type TransactionService struct {
	mutex        sync.Mutex
	lockService  LockService
	lastTID      uint64
	transactions map[*Transaction]struct{}
}

type Transaction struct {
	ts          *TransactionService
	lockerState LockerState
	contexts    map[Database]interface{}
	tid         uint64
	sid         uint64
}

type Database interface {
	Begin(tx *Transaction) interface{}
	Commit(ses engine.Session, tctx interface{}) error
	Rollback(tctx interface{}) error
	NextStmt(tctx interface{})
}

func (ts *TransactionService) Init() {
	ts.transactions = map[*Transaction]struct{}{}
	ts.lockService.Init()
}

func (ts *TransactionService) removeTransaction(tx *Transaction) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	delete(ts.transactions, tx)
	ts.lockService.ReleaseLocks(tx)
}

// Begin a new transaction.
func (ts *TransactionService) Begin(sid uint64) *Transaction {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	ts.lastTID += 1
	tx := &Transaction{
		ts:       ts,
		contexts: map[Database]interface{}{},
		tid:      ts.lastTID,
		sid:      sid,
	}
	ts.transactions[tx] = struct{}{}
	return tx
}

func (ts *TransactionService) Transactions() []engine.TransactionState {
	return nil // XXX
}

func (ts *TransactionService) Locks() []Lock {
	return ts.lockService.Locks()
}

func (tx *Transaction) LockerState() *LockerState {
	return &tx.lockerState
}

func (tx *Transaction) String() string {
	return fmt.Sprintf("transaction-%d", tx.tid)
}

func (tx *Transaction) forContexts(fn func(d Database, tctx interface{}) error) error {
	if tx.contexts == nil {
		panic("transaction used after commit or rollback")
	}

	var err error
	for d, tctx := range tx.contexts {
		cerr := fn(d, tctx)
		if cerr != nil {
			if err == nil {
				err = cerr
			} else {
				err = fmt.Errorf("%s; %s", err, cerr)
			}
		}
	}
	return err
}

func (tx *Transaction) Commit(ses engine.Session) error {
	err := tx.forContexts(func(d Database, tctx interface{}) error {
		return d.Commit(ses, tctx)
	})
	tx.contexts = nil
	tx.ts.removeTransaction(tx)
	return err
}

func (tx *Transaction) Rollback() error {
	err := tx.forContexts(func(d Database, tctx interface{}) error {
		return d.Rollback(tctx)
	})
	tx.contexts = nil
	tx.ts.removeTransaction(tx)
	return err
}

func (tx *Transaction) NextStmt() {
	tx.forContexts(func(d Database, tctx interface{}) error {
		d.NextStmt(tctx)
		return nil
	})
}

func (tx *Transaction) LockTable(ses engine.Session, db, tbl sql.Identifier, ll LockLevel) error {
	return tx.ts.lockService.LockTable(ses, tx, db, tbl, ll)
}

func (tx *Transaction) getContext(d Database) interface{} {
	if tx.contexts == nil {
		panic("transaction used after commit or rollback")
	}

	tctx, ok := tx.contexts[d]
	if !ok {
		tctx = d.Begin(tx)
		tx.contexts[d] = tctx
	}
	return tctx
}

func GetTxContext(tx engine.Transaction, d Database) interface{} {
	return tx.(*Transaction).getContext(d)
}
