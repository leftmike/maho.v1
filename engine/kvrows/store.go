package kvrows

import (
	"context"
	"fmt"
)

type Relation interface {
	TxKey() TransactionKey
	CurrentStatement() uint64
	MapID() uint64
	GetTransactionState(txKey TransactionKey) TransactionState
}

type ErrBlockingProposals struct {
	TxKeys []TransactionKey
	Keys   []Key
}

func (err *ErrBlockingProposals) Error() string {
	return fmt.Sprintf("kvrows: %d blocking proposals", len(err.TxKeys))
}

type Store interface {
	// ReadValue will return the most recent version and value of key in mid.
	ReadValue(ctx context.Context, mid uint64, key Key) (uint64, []byte, error)

	// ListValues will return all of the keys and values in mid.
	ListValues(ctx context.Context, mid uint64) ([]Key, [][]byte, error)

	// WriteValue will atomic check that the most recent version of key in mid is key.Version;
	// if it is, the value will be updated to val and the version to ver, which must be
	// greater than key.Version.
	WriteValue(ctx context.Context, mid uint64, key Key, ver uint64, val []byte) error

	// ScanRelation will return a list of keys and values starting from seek if specified. Use
	// prefix to limit the results to only keys having that prefix. The maximum version of keys
	// visible is specified by maxVer. Use num to limit the number of results returned. Which
	// map to scan, as well as the active transaction, is passed as rel.
	//
	// * If the scan was successful, and there are potentially more keys available, err will
	//   be nil. To continue scanning, use next as seek in the next call to ScanRelation.
	// * If the scan was successful, but there are no more keys available, err will be io.EOF,
	//   and next will be nil.
	// * If the scan encountered a proposed write by a different transaction which is potentially
	//   still active, err will be an instance of ErrBlockingProposals. The key of the proposed
	//   write will be returned as next. Note that zero or more valid keys and values, which
	//   were scanned before the proposed write, will also be returned.
	ScanRelation(ctx context.Context, rel Relation, maxVer uint64, prefix []byte, num int,
		seek []byte) (keys []Key, vals [][]byte, next []byte, err error)

	DeleteRelation(ctx context.Context, rel Relation, keys []Key) error
	UpdateRelation(ctx context.Context, rel Relation, keys []Key, vals []byte) error
	InsertRelation(ctx context.Context, rel Relation, keys []Key, vals []byte) error

	/*
		// WriteRows will update, delete, or insert one or more rows for the
		// transaction specified by txKey.
		//
		// For update and delete, the keys, including version, must be the same as
		// returned from ReadRows. For insert, the version must be zero.
		//
		// For insert, the keys must not be visible.
		//
		// For update, delete, and insert:
		// * If making proposed writes for all keys was successful, error will be nil.
		// * If one or more keys had existing proposed writes by other transactions,
		//   error will be an instance of kvrows.ProposedWrites. The keys and the
		//   transaction keys of all the existing proposed writes will be in the
		//   instance.
		// * If there is an existing conflicting value for a key, error will be an
		//   instance of kvrows.ConflictingWrite. The key of the conflicting write will
		//   be in the instance.
		WriteRows(txKey TransactionKey, sid uint64, mid uint64, keys []Key, rows [][]byte) error
	*/
}
