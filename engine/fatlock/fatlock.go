package fatlock

import (
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type LockLevel int

const (
	ACCESS LockLevel = iota + 1
	ROW_MODIFY
	METADATA_MODIFY
	EXCLUSIVE
)

func LockTable(tx *engine.Transaction, db, tbl sql.Identifier, ll LockLevel) error {
	// XXX
	return nil
}

func ReleaseLocks(tx *engine.Transaction) error {
	// XXX
	return nil
}
