package kvrows

import (
	"context"
	"fmt"
)

type GetTxState func(tid TransactionID) (TransactionState, uint64)

type ErrBlockingProposal struct {
	TID TransactionID
	Key Key
}

func (err *ErrBlockingProposal) Error() string {
	return fmt.Sprintf("kvrows: blocking proposal: %v by %v", err.Key, err.TID)
}

type Store interface {
	// ReadValue will return the most recent version and value of key in mid.
	ReadValue(ctx context.Context, mid uint64, key Key) (uint64, []byte, error)

	// ListValues will return all of the keys and values in mid.
	ListValues(ctx context.Context, mid uint64) ([]Key, [][]byte, error)

	// WriteValue will atomically check that the most recent version of key in mid is
	// key.Version; if it is, the value will be updated to val and the version to ver, which
	// must be greater than key.Version.
	WriteValue(ctx context.Context, mid uint64, key Key, ver uint64, val []byte) error

	// ScanRelation will return a list of keys and values starting from seek, if specified. The
	// maximum version of keys visible is specified by maxVer. Use num to limit the number of
	// results returned. The map to scan is specified as mid.
	//
	// * If the scan was successful, and there are potentially more keys available, err will
	//   be nil. To continue scanning, use next as seek in the next call to ScanRelation.
	// * If the scan was successful, but there are no more keys available, err will be io.EOF,
	//   and next will be nil.
	// * If the scan encountered a proposed write by a different transaction which is potentially
	//   still active, err will be an instance of ErrBlockingProposal. The key of the proposed
	//   write will be returned as next. Note that zero or more valid keys and values, which
	//   were scanned before the proposed write, will also be returned.
	ScanRelation(ctx context.Context, getState GetTxState, tid TransactionID,
		sid, mid, maxVer uint64, num int, seek []byte) (keys []Key, vals [][]byte, next []byte,
		err error)

	// ModifyRelation will delete, if vals is nil, or update one or more keys for the map
	// specified by mid. The keys must all exist and have visible values. Each key must exactly
	// match the latest visible version of the key. To do this, use the keys returned from
	// ScanRelation.
	//
	// The operation is atomic: either all of the modifications will be proposed or none of them.
	// A transaction key must already exist; it is passed as tid.
	//
	// If a modification encountered a proposed write by a different transaction which is
	// potentially still active, err will be an instance of ErrBlockingProposal.
	//
	// XXX: updating a key doesn't require the entire value; it would potentially be more
	// efficient to just pass the delta.
	ModifyRelation(ctx context.Context, getState GetTxState, tid TransactionID, sid, mid uint64,
		keys []Key, vals [][]byte) error

	// InsertRelation will insert new key(s) and value(s) into the map specified by mid.
	// None of the keys can have visible values.
	//
	// The operation is atomic: either all of the values will be proposed for the keys or none of
	// them. A transaction key must already exist; it is passed as tid.
	//
	// If an insert encountered a proposed write by a different transaction which is
	// potentially still active, err will be an instance of ErrBlockingProposal.
	InsertRelation(ctx context.Context, getState GetTxState, tid TransactionID, sid, mid uint64,
		keys [][]byte, vals [][]byte) error

	// CleanKeys makes proposals by committed transactions durable and deletes proposals by
	// aborted transactions; it only does this for the keys specified.
	CleanKeys(ctx context.Context, getState GetTxState, mid uint64, keys [][]byte) error

	// CleanRelation checks all of the keys in a relation; all proposals by committed
	// transactions are made durable and all proposals by aborted transactions are deleted.
	// Bad keys and proposals cause an error unless bad is true, in which case they are
	// deleted.
	CleanRelation(ctx context.Context, getState GetTxState, mid uint64, bad bool) error
}
