package kvrows

import (
	"context"
	"fmt"
)

type GetTxState func(tid TransactionID) (TransactionState, uint64)

type ListKeyValue func(key []byte, ver uint64, val []byte) (bool, error)

type ScanKeyValue func(key []byte, ver uint64, val []byte) (bool, error)

type ModifyKeyValue func(key []byte, ver uint64, val []byte) ([]byte, error)

type ErrBlockingProposal struct {
	TID TransactionID
	Key []byte
}

func (err *ErrBlockingProposal) Error() string {
	return fmt.Sprintf("kvrows: blocking proposal: %v by %v", err.Key, err.TID)
}

type Store interface {
	// ReadValue will return the most recent version and value of key in mid.
	ReadValue(ctx context.Context, mid uint64, key []byte) (uint64, []byte, error)

	// ListValues will call listKeyValue for all of the keys and values in mid.
	ListValues(ctx context.Context, mid uint64, listKeyValue ListKeyValue) error

	// WriteValue will atomically check that the most recent version of key in mid is
	// ver; if it is, the value will be updated to val and the version to ver+1.
	WriteValue(ctx context.Context, mid uint64, key []byte, ver uint64, val []byte) error

	// CleanKeys makes proposals by committed transactions durable and deletes proposals by
	// aborted transactions; it only does this for the keys specified.
	CleanKeys(ctx context.Context, getState GetTxState, mid uint64, keys [][]byte) error

	// CleanMap checks all of the keys in a map; all proposals by committed
	// transactions are made durable and all proposals by aborted transactions are deleted.
	// Bad keys and proposals cause an error unless bad is true, in which case they are
	// deleted.
	CleanMap(ctx context.Context, getState GetTxState, mid uint64, bad bool) error

	ScanMap(ctx context.Context, getState GetTxState, tid TransactionID, sid, mid uint64,
		seek []byte, scanKeyValue ScanKeyValue) (next []byte, err error)
	ModifyMap(ctx context.Context, getState GetTxState, tid TransactionID, sid, mid uint64,
		key []byte, ver uint64, modifyKeyValue ModifyKeyValue) error
	DeleteMap(ctx context.Context, getState GetTxState, tid TransactionID, sid, mid uint64,
		key []byte, ver uint64) error
	InsertMap(ctx context.Context, getState GetTxState, tid TransactionID, sid, mid uint64,
		key, val []byte) error
}
