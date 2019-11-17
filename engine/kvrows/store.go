package kvrows

type ProposedWrites struct {
	Keys   []Key
	TxKeys []byte
}

func (pw ProposedWrites) Error() string {
	return "proposed write"
}

type ConflictingWrite Key

func (cw ConflictingWrite) Error() string {
	return "conflicting write"
}

type TransactionInfo struct {
	MID       uint64
	Key       []byte
	StmtCount uint64
	Epoch     uint64
	TID       uint64
}

type Store interface {
	// ReadValue will return the most recent version and value of key in mid.
	ReadValue(mid uint64, key Key) (uint64, []byte, error)

	// ListValues will return all of the keys and values in mid.
	ListValues(mid uint64) ([]Key, [][]byte, error)

	// WriteValue will atomic check that the most recent version of key in mid is key.Version;
	// if it is, the value will be updated to val and the version to ver, which must be
	// greater than key.Version.
	WriteValue(mid uint64, key Key, ver uint64, val []byte) error

	// ReadRows will return a list of keys and values following lastKey. Use
	// prefix to limit the result to only keys having that prefix. The maximum
	// version of keys visible is specified by ver. To include proposed writes
	// for a specific transaction, specify txKey.
	//
	// * If the scan was successful, and there are potential more keys available,
	//   error will be nil.
	// * If the scan was successful, but there are no more keys available, error
	//   will be io.EOF.
	// * If the scan encountered a proposed write by a different transaction,
	//   error will be an instance of kvrows.ProposedWrites. The key and the
	//   transaction key of the proposed write are in the instance. Note that
	//   zero or more valid keys and values, which were scanned before the proposed
	//    write, will also be returned.
	ReadRows(txInfo *TransactionInfo, mid uint64, prefix []byte, lastKey []byte, ver uint64) ([]Key,
		[][]byte, error)

	// WriteRows will update, delete, or insert one or more rows for the
	// transaction specified by txInfo.
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
	WriteRows(txInfo *TransactionInfo, mid uint64, keys []Key, rows [][]byte) error
}
