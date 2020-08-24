package engine

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type action interface {
	execute(ctx context.Context, e *Engine, tx *transaction) (int64, error)
}

type actionKey struct {
	tn   sql.TableName
	name sql.Identifier
}

type transaction struct {
	e       *Engine
	tx      Transaction
	actions map[actionKey]action
}

func (e *Engine) Begin(sesid uint64) sql.Transaction {
	return &transaction{
		e:  e,
		tx: e.st.Begin(sesid),
	}
}

func (tx *transaction) Commit(ctx context.Context) error {
	err := tx.executeActions(ctx)
	if err != nil {
		tx.tx.Rollback()
		return err
	}
	err = tx.tx.Commit(ctx)
	tx.tx = nil
	return err
}

func (tx *transaction) Rollback() error {
	err := tx.tx.Rollback()
	tx.tx = nil
	return err
}

func (tx *transaction) NextStmt(ctx context.Context) error {
	err := tx.executeActions(ctx)
	if err != nil {
		tx.tx.Rollback()
		return err
	}
	tx.tx.NextStmt()
	return nil
}

func (tx *transaction) addAction(tn sql.TableName, nam sql.Identifier, newAct func() action,
	addAct func(act action)) {

	actKey := actionKey{tn, nam}
	act, ok := tx.actions[actKey]
	if !ok {
		act = newAct()
		if tx.actions == nil {
			tx.actions = map[actionKey]action{}
		}
		tx.actions[actKey] = act
	}

	addAct(act)
}

func (tx *transaction) executeActions(ctx context.Context) error {
	for len(tx.actions) > 0 {
		actions := tx.actions
		tx.actions = nil

		for _, act := range actions {
			_, err := act.execute(ctx, tx.e, tx)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
