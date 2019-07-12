package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/virtual"
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
	Commit(ctx context.Context, tctx interface{}) error
	Rollback(tctx interface{}) error
	NextStmt(tctx interface{})
}

func (ts *TransactionService) Init(e engine.Engine) {
	ts.transactions = map[*Transaction]struct{}{}
	ts.lockService.Init(e)
	if e != nil {
		e.CreateSystemTable(sql.ID("transactions"), ts.makeTransactionsTable)
	}
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

func (tx *Transaction) Commit(ctx context.Context) error {
	err := tx.forContexts(func(d Database, tctx interface{}) error {
		return d.Commit(ctx, tctx)
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

func (tx *Transaction) LockSchema(ctx context.Context, sn sql.SchemaName, ll LockLevel) error {
	return tx.ts.lockService.LockSchema(ctx, tx, sn, ll)
}

func (tx *Transaction) LockTable(ctx context.Context, tn sql.TableName, ll LockLevel) error {
	return tx.ts.lockService.LockTable(ctx, tx, tn, ll)
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

func (ts *TransactionService) makeTransactionsTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	values := [][]sql.Value{}

	for tx := range ts.transactions {
		values = append(values, []sql.Value{
			sql.StringValue(fmt.Sprintf("transaction-%d", tx.tid)),
			sql.StringValue(fmt.Sprintf("session-%d", tx.sid)),
		})
	}

	return virtual.MakeTable(tn.String(),
		[]sql.Identifier{sql.ID("transaction"), sql.ID("session")},
		[]sql.ColumnType{sql.StringColType, sql.StringColType}, values), nil
}
