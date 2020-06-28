package kvrows

//go:generate protoc --go_opt=paths=source_relative --go_out=. data.proto

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/encode"
	"github.com/leftmike/maho/storage/tblstore"
)

const (
	transactionsMID = 1

	ProposalVersion = math.MaxUint64
)

var (
	errTransactionComplete = errors.New("keyval: transaction already completed")
	versionKey             = []byte{0, 0, 0, 0, 0, 0, 0, 0, 'v', 'e', 'r', 's', 'i', 'o', 'n'}
	epochKey               = []byte{0, 0, 0, 0, 0, 0, 0, 0, 'e', 'p', 'o', 'c', 'h'}
)

type Updater interface {
	Iterate(key []byte) (Iterator, error)
	Get(key []byte, fn func(val []byte) error) error
	Set(key, val []byte) error
	Commit() error
	Rollback()
}

type Iterator interface {
	Item(fn func(key, val []byte) error) error
	Close()
}

type KV interface {
	Iterate(key []byte) (Iterator, error)
	Get(key []byte, fn func(val []byte) error) error
	Update() (Updater, error)
}

type kvStore struct {
	kv           KV
	mutex        sync.Mutex
	transactions map[uint64]*TransactionData
	lastTID      uint64
	ver          uint64
	epoch        uint64
	commitMutex  sync.Mutex
}

type tableStruct struct {
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []sql.ColumnKey
	mid         int64
}

type transaction struct {
	sesid       uint64
	st          *kvStore
	ver         uint64
	tid         uint64
	sid         uint32
	updatedKeys [][]byte
}

type table struct {
	st *kvStore
	tx *transaction
	ts *tableStruct
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewBadgerStore(dataDir string) (storage.Store, error) {
	kv, err := MakeBadgerKV(dataDir)
	if err != nil {
		return nil, err
	}

	kvst, init, err := makeStore(kv)
	if err != nil {
		return nil, err
	}

	return tblstore.NewStore("kvrows", kvst, init)
}

func getUint64(kv KV, key []byte) (uint64, error) {
	var u64 uint64
	err := kv.Get(key,
		func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("keyval: key %v: len(val) != 8: %d", key, len(val))
			}
			u64 = binary.BigEndian.Uint64(val)
			return nil
		})
	return u64, err
}

func loadTransactions(kv KV) (map[uint64]*TransactionData, error) {
	it, err := kv.Iterate(encode.EncodeUint64(make([]byte, 0, 8), transactionsMID))
	if err != nil {
		return nil, err
	}
	defer it.Close()

	transactions := map[uint64]*TransactionData{}
	for {
		err = it.Item(
			func(key, val []byte) error {
				if len(key) < 8 {
					return fmt.Errorf("kvrows: key too short: %v", key)
				}
				if binary.BigEndian.Uint64(key[:8]) != transactionsMID {
					return io.EOF
				}
				if len(key) != 16 {
					return fmt.Errorf("kvrows: transaction key wrong length: %v", key)
				}
				tid := binary.BigEndian.Uint64(key[8:])

				var td TransactionData
				err := proto.Unmarshal(val, &td)
				if err != nil {
					return err
				}

				transactions[tid] = &td
				return nil
			})
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
	}

	return transactions, nil
}

func setTransactionData(upd Updater, tid uint64, td *TransactionData) error {
	val, err := proto.Marshal(td)
	if err != nil {
		return err
	}
	return upd.Set(
		encode.EncodeUint64(encode.EncodeUint64(make([]byte, 0, 16), transactionsMID), tid), val)
}

func makeStore(kv KV) (*kvStore, bool, error) {
	var init bool
	ver, err := getUint64(kv, versionKey)
	if err == io.EOF {
		init = true
	} else if err != nil {
		return nil, false, err
	}

	epoch, err := getUint64(kv, epochKey)
	if err != nil && err != io.EOF {
		return nil, false, err
	}
	epoch += 1

	transactions, err := loadTransactions(kv)
	if err != nil {
		return nil, false, err
	}

	kvst := &kvStore{
		kv:           kv,
		transactions: transactions,
		ver:          ver,
		epoch:        epoch,
	}

	upd, err := kvst.kv.Update()
	if err != nil {
		return nil, false, err
	}
	err = kvst.startupStore(upd)
	if err != nil {
		upd.Rollback()
		return nil, false, err
	}
	err = upd.Commit()
	if err != nil {
		return nil, false, err
	}

	return kvst, init, nil
}

func (kvst *kvStore) startupStore(upd Updater) error {
	err := upd.Set(epochKey, encode.EncodeUint64(make([]byte, 0, 8), kvst.epoch))
	if err != nil {
		return err
	}

	for tid, td := range kvst.transactions {
		if tid > kvst.lastTID {
			kvst.lastTID = tid
		}
		if td.State == TransactionState_Active {
			td.State = TransactionState_Aborted
			err = setTransactionData(upd, tid, td)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (ts *tableStruct) Table(ctx context.Context, tx storage.Transaction) (storage.Table,
	error) {

	etx := tx.(*transaction)
	return &table{
		st: etx.st,
		tx: etx,
		ts: ts,
	}, nil
}

func (ts *tableStruct) Columns() []sql.Identifier {
	return ts.columns
}

func (ts *tableStruct) ColumnTypes() []sql.ColumnType {
	return ts.columnTypes
}

func (ts *tableStruct) PrimaryKey() []sql.ColumnKey {
	return ts.primary
}

func (ts *tableStruct) makeKey(row []sql.Value) []byte {
	buf := encode.EncodeUint64(make([]byte, 0, 8), uint64(ts.mid))
	if row != nil {
		buf = append(buf, encode.MakeKey(ts.primary, row)...)
	}
	return buf
}

func (kvst *kvStore) MakeTableStruct(tn sql.TableName, mid int64, cols []sql.Identifier,
	colTypes []sql.ColumnType, primary []sql.ColumnKey) (tblstore.TableStruct, error) {

	if len(primary) == 0 {
		panic(fmt.Sprintf("kvrows: table %s: missing required primary key", tn))
	}

	ts := tableStruct{
		tn:          tn,
		columns:     cols,
		columnTypes: colTypes,
		primary:     primary,
		mid:         mid,
	}
	return &ts, nil
}

func (kvst *kvStore) setTransactionData(tid uint64, td *TransactionData) error {
	upd, err := kvst.kv.Update()
	if err != nil {
		return err
	}
	err = setTransactionData(upd, tid, td)
	if err != nil {
		upd.Rollback()
		return err
	}
	return upd.Commit()
}

func (kvst *kvStore) Begin(sesid uint64) tblstore.Transaction {
	kvst.mutex.Lock()
	kvst.lastTID += 1
	tid := kvst.lastTID
	ver := kvst.ver

	td := &TransactionData{
		State: TransactionState_Active,
		Epoch: kvst.epoch,
	}
	kvst.transactions[tid] = td
	kvst.mutex.Unlock()

	err := kvst.setTransactionData(tid, td)
	if err != nil {
		panic(fmt.Sprintf("kvrows: unable to set transaction data: %s", err))
	}

	return &transaction{
		st:    kvst,
		sesid: sesid,
		tid:   tid,
		ver:   ver,
		sid:   1,
	}
}

func (kvst *kvStore) getTxState(tid uint64) (TransactionState, uint64) {
	kvst.mutex.Lock()
	defer kvst.mutex.Unlock()

	txd := kvst.transactions[tid]
	return txd.State, txd.Version
}

func (kvst *kvStore) commit(ctx context.Context, tid uint64) error {
	kvst.commitMutex.Lock()
	defer kvst.commitMutex.Unlock()

	ver := kvst.ver + 1
	td := &TransactionData{
		State:   TransactionState_Committed,
		Epoch:   kvst.epoch,
		Version: ver,
	}

	upd, err := kvst.kv.Update()
	if err != nil {
		return kvst.rollback(tid)
	}
	err = setTransactionData(upd, tid, td)
	if err == nil {
		err = upd.Set(versionKey, encode.EncodeUint64(make([]byte, 0, 8), ver))
		if err == nil {
			err = upd.Commit()
		}
	}
	if err != nil {
		upd.Rollback()
		return kvst.rollback(tid)
	}

	kvst.mutex.Lock()
	kvst.transactions[tid] = td
	kvst.ver = ver
	kvst.mutex.Unlock()

	return err
}

func (kvst *kvStore) rollback(tid uint64) error {
	kvst.mutex.Lock()
	td := kvst.transactions[tid]
	td.State = TransactionState_Aborted
	kvst.mutex.Unlock()

	upd, err := kvst.kv.Update()
	if err == nil {
		err = setTransactionData(upd, tid, td)
		if err != nil {
			upd.Rollback()
		} else {
			err = upd.Commit()
		}
	}

	return err
}

func (kvtx *transaction) Commit(ctx context.Context) error {
	if kvtx.st == nil {
		return errTransactionComplete
	}

	err := kvtx.st.commit(ctx, kvtx.tid)
	kvtx.st = nil
	// XXX: cleanup proposals
	return err
}

func (kvtx *transaction) Rollback() error {
	if kvtx.st == nil {
		return errTransactionComplete
	}

	err := kvtx.st.rollback(kvtx.tid)
	kvtx.st = nil
	// XXX: cleanup proposals
	return err
}

func (kvtx *transaction) NextStmt() {
	kvtx.sid += 1
}

func (kvtx *transaction) Changes(cfn func(mid int64, key string, row []sql.Value) bool) {
	panic("changes not implemented")
}

func (kvt *table) Columns(ctx context.Context) []sql.Identifier {
	return kvt.ts.columns
}

func (kvt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return kvt.ts.columnTypes
}

func (kvt *table) PrimaryKey(ctx context.Context) []sql.ColumnKey {
	return kvt.ts.primary
}

func (kvt *table) unmarshalProposal(key, val []byte) (*ProposalData, error) {
	var pd ProposalData
	err := proto.Unmarshal(val, &pd)
	if err != nil || len(pd.Updates) == 0 {
		return nil, fmt.Errorf("kvrows: %s: unable to unmarshal proposal at %v: %v",
			kvt.ts.tn, key, val)
	}

	return &pd, nil
}

func (kvt *table) decodeRow(key, val []byte) ([]sql.Value, error) {
	row := encode.DecodeRowValue(val)
	if row == nil {
		return nil,
			fmt.Errorf("kvrows: %s: unable to decode proposed row at %v: %v",
				kvt.ts.tn, key, val)
	}

	return row, nil
}

func (kvt *table) getProposedRow(key, val []byte) ([]sql.Value, bool, error) {
	pd, err := kvt.unmarshalProposal(key, val)
	if err != nil {
		return nil, false, err
	}

	if pd.TID == kvt.tx.tid {
		for _, pu := range pd.Updates {
			if pu.SID < kvt.tx.sid {
				var row []sql.Value
				if len(pu.Value) > 0 {
					row, err = kvt.decodeRow(key, pu.Value)
					if err != nil {
						return nil, false, err
					}
				}
				return row, true, nil
			}
		}
	} else {
		state, commitVer := kvt.st.getTxState(pd.TID)
		if state == TransactionState_Committed && commitVer <= kvt.tx.ver {
			var row []sql.Value
			if len(pd.Updates[0].Value) > 0 {
				row, err = kvt.decodeRow(key, pd.Updates[0].Value)
				if err != nil {
					return nil, false, err
				}
			}
			return row, true, nil
		}
	}

	return nil, false, nil
}

func (kvt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (sql.Rows, error) {
	minKey := kvt.ts.makeKey(minRow)
	var maxKey []byte
	if maxRow != nil {
		maxKey = kvt.ts.makeKey(maxRow)
	}

	it, err := kvt.st.kv.Iterate(minKey)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	kvr := &rows{
		tbl: kvt,
	}

	var prevKey []byte
	var skipping bool
	for {
		err = it.Item(
			func(key, val []byte) error {
				if len(key) < 16 {
					return fmt.Errorf("kvrows: %s: key too short: %v", kvt.ts.tn, key)
				}
				ver := ^binary.BigEndian.Uint64(key[len(key)-8:])
				key = key[:len(key)-8]

				if maxKey == nil {
					if !bytes.Equal(minKey[:8], key[:8]) {
						return io.EOF
					}
				} else if bytes.Compare(maxKey, key) < 0 {
					return io.EOF
				}

				if skipping {
					// XXX: maybe use iterator Seek to <key> <ver:0>?
					if !bytes.Equal(prevKey, key) {
						skipping = false
					}
				}

				if !skipping {
					if ver == ProposalVersion {
						var err error
						var row []sql.Value
						row, skipping, err = kvt.getProposedRow(key, val)
						if err != nil {
							return err
						}
						if row != nil {
							kvr.rows = append(kvr.rows, row)
						}
					} else if ver <= kvt.tx.ver {
						if len(val) > 0 {
							row, err := kvt.decodeRow(key, val)
							if err != nil {
								return err
							}
							kvr.rows = append(kvr.rows, row)
						}
						skipping = true
					}

					if skipping {
						prevKey = append(make([]byte, 0, len(key)), key...)
					}
				}
				return nil
			})
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	return kvr, nil
}

func makeKeyVersion(key []byte, ver uint64) []byte {
	buf := append(make([]byte, 0, len(key)+8), key...)
	return encode.EncodeUint64(buf, ^ver)
}

func (kvt *table) prepareUpdate(upd Updater, updateKey []byte) (*ProposalData, bool, error) {
	var pd *ProposalData
	err := upd.Get(makeKeyVersion(updateKey, ProposalVersion),
		func(val []byte) error {
			var err error
			pd, err = kvt.unmarshalProposal(updateKey, val)
			return err
		})
	if err == io.EOF {
		return &ProposalData{TID: kvt.tx.tid}, false, nil
	} else if err != nil {
		return nil, false, err
	}

	pu := pd.Updates[0]
	if pd.TID == kvt.tx.tid {
		if pu.SID == kvt.tx.sid {
			return nil, false, fmt.Errorf("kvrows: %s: multiple updates of %v",
				kvt.ts.tn, updateKey)
		}
		return pd, len(pu.Value) != 0, nil
	} else {
		state, ver := kvt.st.getTxState(pd.TID)
		if state == TransactionState_Active {
			return nil, false, fmt.Errorf("kvrows: %s: conflict with proposed version of %v",
				kvt.ts.tn, updateKey)
		} else if state == TransactionState_Committed {
			if ver > kvt.tx.ver {
				return nil, false, fmt.Errorf("kvrows: %s: conflict with newer version of %v",
					kvt.ts.tn, updateKey)
			}
			err := upd.Set(makeKeyVersion(updateKey, ver), pu.Value)
			if err != nil {
				return nil, false, err
			}
			return &ProposalData{TID: kvt.tx.tid}, len(pu.Value) != 0, nil
		}

		// Proposal was aborted; look for highest versioned value.
	}

	it, err := upd.Iterate(makeKeyVersion(updateKey, ProposalVersion-1))
	if err != nil {
		return nil, false, err
	}
	defer it.Close()

	var existing bool
	err = it.Item(
		func(key, val []byte) error {
			if len(key) < 16 {
				return fmt.Errorf("kvrows: %s: key too short: %v", kvt.ts.tn, key)
			}
			ver := ^binary.BigEndian.Uint64(key[len(key)-8:])
			key = key[:len(key)-8]

			if !bytes.Equal(updateKey, key) {
				return io.EOF
			}

			if ver > kvt.tx.ver {
				return fmt.Errorf("kvrows: %s: conflict with newer version of %v",
					kvt.ts.tn, updateKey)
			}

			existing = len(val) > 0
			return nil
		})
	if err == io.EOF {
		return &ProposalData{TID: kvt.tx.tid}, false, nil
	} else if err != nil {
		return nil, false, err
	}

	return &ProposalData{TID: kvt.tx.tid}, existing, nil
}

func (kvt *table) proposeUpdate(upd Updater, updateKey []byte, row []sql.Value,
	mustExist bool) error {

	pd, exists, err := kvt.prepareUpdate(upd, updateKey)
	if err != nil {
		return err
	}
	if mustExist {
		if !exists {
			panic(fmt.Sprintf("kvrows: %s: row missing for update at %v", kvt.ts.tn, updateKey))
		}
	} else {
		if exists {
			return fmt.Errorf("kvrows: %s: existing row with duplicate primary key at %v",
				kvt.ts.tn, updateKey)
		}
	}

	kvt.tx.updatedKeys = append(kvt.tx.updatedKeys, updateKey)

	var rowValue []byte
	if len(row) > 0 {
		rowValue = encode.EncodeRowValue(row)
	}
	pd.Updates = append([]*ProposedUpdate{
		&ProposedUpdate{
			SID:   kvt.tx.sid,
			Value: rowValue,
		},
	}, pd.Updates...)

	val, err := proto.Marshal(pd)
	if err != nil {
		return err
	}
	return upd.Set(makeKeyVersion(updateKey, ProposalVersion), val)
}

func (kvt *table) Insert(ctx context.Context, row []sql.Value) error {
	upd, err := kvt.st.kv.Update()
	if err != nil {
		return err
	}

	err = kvt.proposeUpdate(upd, kvt.ts.makeKey(row), row, false)
	if err != nil {
		upd.Rollback()
		return err
	}
	return upd.Commit()
}

func (kvr *rows) Columns() []sql.Identifier {
	return kvr.tbl.ts.columns
}

func (kvr *rows) Close() error {
	kvr.tbl = nil
	kvr.rows = nil
	kvr.idx = 0
	return nil
}

func (kvr *rows) Next(ctx context.Context, dest []sql.Value) error {
	if kvr.idx == len(kvr.rows) {
		return io.EOF
	}
	copy(dest, kvr.rows[kvr.idx])
	kvr.idx += 1
	return nil
}

func (kvr *rows) Delete(ctx context.Context) error {
	if kvr.idx == 0 {
		panic(fmt.Sprintf("kvrows: table %s no row to delete", kvr.tbl.ts.tn))
	}

	upd, err := kvr.tbl.st.kv.Update()
	if err != nil {
		return err
	}

	err = kvr.tbl.proposeUpdate(upd, kvr.tbl.ts.makeKey(kvr.rows[kvr.idx-1]), nil, true)
	if err != nil {
		upd.Rollback()
		return err
	}
	return upd.Commit()
}

func (kvr *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	if kvr.idx == 0 {
		panic(fmt.Sprintf("kvrows: table %s no row to update", kvr.tbl.ts.tn))
	}

	var primaryUpdated bool
	for _, update := range updates {
		for _, ck := range kvr.tbl.ts.primary {
			if ck.Number() == update.Index {
				primaryUpdated = true
			}
		}
	}

	updateRow := append(make([]sql.Value, 0, len(kvr.rows[kvr.idx-1])), kvr.rows[kvr.idx-1]...)
	for _, update := range updates {
		updateRow[update.Index] = update.Value
	}

	if primaryUpdated {
		kvr.Delete(ctx)
		return kvr.tbl.Insert(ctx, updateRow)
	}

	upd, err := kvr.tbl.st.kv.Update()
	if err != nil {
		return err
	}

	err = kvr.tbl.proposeUpdate(upd, kvr.tbl.ts.makeKey(updateRow), updateRow, true)
	if err != nil {
		upd.Rollback()
		return err
	}
	return upd.Commit()
}
