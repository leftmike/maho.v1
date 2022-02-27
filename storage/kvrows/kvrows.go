package kvrows

//go:generate protoc --go_opt=paths=source_relative --go_out=. txdata.proto

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
	log "github.com/sirupsen/logrus"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/encode"
	"github.com/leftmike/maho/util"
)

const (
	transactionsRID = (1 << 16) | storage.PrimaryIID

	ProposalVersion = math.MaxUint64
)

var (
	errTransactionComplete = errors.New("kvrows: transaction already completed")
	epochKey               = []byte{0, 0, 0, 0, 0, 0, 0, 0, 'e', 'p', 'o', 'c', 'h'}
)

type Updater interface {
	Iterate(key []byte) (Iterator, error)
	Get(key []byte, fn func(val []byte) error) error
	Set(key, val []byte) error
	Commit(sync bool) error
	Rollback()
}

type Iterator interface {
	Item(fn func(key, val []byte) error) error
	Close()
}

type KV interface {
	Iterate(key []byte) (Iterator, error)
	Update(key []byte, fn func(val []byte) ([]byte, error)) error

	// XXX: remove
	Get(key []byte, fn func(val []byte) error) error
	Updater() (Updater, error)
}

type kvStore struct {
	kv           KV
	mutex        sync.Mutex
	transactions map[uint64]*TransactionData
	lastTXID     uint64
	ver          uint64
	epoch        uint64
	commitMutex  sync.Mutex
}

type transaction struct {
	sesid       uint64
	st          *kvStore
	ver         uint64
	txid        uint64
	sid         uint32
	updatedKeys [][]byte
}

type table struct {
	st  *kvStore
	tl  *storage.TableLayout
	tn  sql.TableName
	tid int64
	tx  *transaction
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

type indexRows struct {
	tbl  *table
	il   storage.IndexLayout
	idx  int
	rows [][]sql.Value
}

func NewBadgerStore(dataDir string, logger *log.Logger) (*storage.Store, error) {
	kv, err := MakeBadgerKV(dataDir, logger)
	if err != nil {
		return nil, err
	}

	kvst, init, err := makeStore(kv)
	if err != nil {
		return nil, err
	}

	return storage.NewStore("kvrows", kvst, init)
}

func NewPebbleStore(dataDir string, logger *log.Logger) (*storage.Store, error) {
	kv, err := MakePebbleKV(dataDir, logger)
	if err != nil {
		return nil, err
	}

	kvst, init, err := makeStore(kv)
	if err != nil {
		return nil, err
	}

	return storage.NewStore("kvrows", kvst, init)
}

func loadTransactions(kv KV) (map[uint64]*TransactionData, error) {
	it, err := kv.Iterate(util.EncodeUint64(make([]byte, 0, 8), transactionsRID))
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
				if binary.BigEndian.Uint64(key[:8]) != transactionsRID {
					return io.EOF
				}
				if len(key) != 16 {
					return fmt.Errorf("kvrows: transaction key wrong length: %v", key)
				}
				txid := binary.BigEndian.Uint64(key[8:])

				var td TransactionData
				err := proto.Unmarshal(val, &td)
				if err != nil {
					return err
				}

				transactions[txid] = &td
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

func makeStore(kv KV) (*kvStore, bool, error) {
	var init bool
	var epoch uint64
	err := kv.Update(epochKey,
		func(val []byte) ([]byte, error) {
			if len(val) == 8 {
				epoch = binary.BigEndian.Uint64(val)
			} else if len(val) == 0 {
				init = true
			} else {
				return nil, fmt.Errorf("kvrows: key %v: len(val) != 8: %d", epochKey, len(val))
			}
			epoch += 1

			return util.EncodeUint64(make([]byte, 0, 8), epoch), nil
		})
	if err != nil {
		return nil, false, err
	}

	transactions, err := loadTransactions(kv)
	if err != nil {
		return nil, false, err
	}

	kvst := &kvStore{
		kv:           kv,
		transactions: transactions,
		epoch:        epoch,
	}

	err = kvst.startupStore()
	if err != nil {
		return nil, false, err
	}
	return kvst, init, nil
}

func (kvst *kvStore) startupStore() error {
	for txid, td := range kvst.transactions {
		if txid > kvst.lastTXID {
			kvst.lastTXID = txid
		}
		if td.State == TransactionState_Active {
			td.State = TransactionState_Aborted
			err := kvst.setTransactionData(txid, td)
			if err != nil {
				return err
			}
		} else if td.State == TransactionState_Committed && td.Version >= kvst.ver {
			kvst.ver = td.Version + 1
		}
	}

	return nil
}

func (kvst *kvStore) Table(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	tid int64, tt *engine.TableType, tl *storage.TableLayout) (storage.Table, error) {

	if len(tt.PrimaryKey()) == 0 {
		panic(fmt.Sprintf("kvrows: table %s: missing required primary key", tn))
	}

	etx := tx.(*transaction)
	return &table{
		st:  etx.st,
		tl:  tl,
		tn:  tn,
		tid: tid,
		tx:  etx,
	}, nil
}

func (kvst *kvStore) setTransactionData(txid uint64, td *TransactionData) error {
	return kvst.kv.Update(
		util.EncodeUint64(util.EncodeUint64(make([]byte, 0, 16), transactionsRID), txid),
		func(val []byte) ([]byte, error) {
			// XXX: check val
			return proto.Marshal(td)
		})
}

func (kvst *kvStore) Begin(sesid uint64) engine.Transaction {
	kvst.mutex.Lock()
	kvst.lastTXID += 1
	txid := kvst.lastTXID
	ver := kvst.ver

	td := &TransactionData{
		State: TransactionState_Active,
		Epoch: kvst.epoch,
	}
	kvst.transactions[txid] = td
	kvst.mutex.Unlock()

	err := kvst.setTransactionData(txid, td)
	if err != nil {
		panic(fmt.Sprintf("kvrows: unable to set transaction data: %s", err))
	}

	return &transaction{
		st:    kvst,
		sesid: sesid,
		txid:  txid,
		ver:   ver,
		sid:   1,
	}
}

func (kvst *kvStore) getTxState(txid uint64) (TransactionState, uint64) {
	kvst.mutex.Lock()
	defer kvst.mutex.Unlock()

	txd := kvst.transactions[txid]
	return txd.State, txd.Version
}

func (kvst *kvStore) commit(ctx context.Context, txid uint64) error {
	kvst.commitMutex.Lock()
	defer kvst.commitMutex.Unlock()

	ver := kvst.ver + 1
	td := &TransactionData{
		State:   TransactionState_Committed,
		Epoch:   kvst.epoch,
		Version: ver,
	}

	err := kvst.setTransactionData(txid, td)
	if err != nil {
		return kvst.rollback(txid)
	}

	kvst.mutex.Lock()
	kvst.transactions[txid] = td
	kvst.ver = ver
	kvst.mutex.Unlock()

	return err
}

func (kvst *kvStore) rollback(txid uint64) error {
	kvst.mutex.Lock()
	td := kvst.transactions[txid]
	td.State = TransactionState_Aborted
	kvst.mutex.Unlock()

	return kvst.setTransactionData(txid, td)
}

func (kvtx *transaction) Commit(ctx context.Context) error {
	if kvtx.st == nil {
		return errTransactionComplete
	}

	err := kvtx.st.commit(ctx, kvtx.txid)
	kvtx.st = nil
	// XXX: cleanup proposals
	return err
}

func (kvtx *transaction) Rollback() error {
	if kvtx.st == nil {
		return errTransactionComplete
	}

	err := kvtx.st.rollback(kvtx.txid)
	kvtx.st = nil
	// XXX: cleanup proposals
	return err
}

func (kvtx *transaction) NextStmt() {
	kvtx.sid += 1
}

func (kvt *table) unmarshalRowData(key, val []byte) (*RowData, error) {
	var rd RowData
	err := proto.Unmarshal(val, &rd)
	if err != nil || (rd.Proposal == nil && len(rd.Rows) == 0) ||
		(rd.Proposal != nil && len(rd.Proposal.Updates) == 0) {
		return nil, fmt.Errorf("kvrows: %s: unable to unmarshal row data at %v: %v", kvt.tn, key,
			val)
	}

	return &rd, nil
}

func (kvt *table) decodeRow(key, val []byte) ([]sql.Value, error) {
	row := encode.DecodeRowValue(val)
	if row == nil {
		return nil,
			fmt.Errorf("kvrows: %s: unable to decode row at %v: %v", kvt.tn, key, val)
	}

	return row, nil
}

func (kvt *table) makeKey(key []sql.ColumnKey, iid int64, row []sql.Value) []byte {
	buf := util.EncodeUint64(make([]byte, 0, 8), uint64((kvt.tid<<16)|iid))
	if row != nil {
		buf = append(buf, encode.MakeKey(key, row)...)
	}
	return buf
}

func (kvt *table) makeIndexKey(il storage.IndexLayout, row []sql.Value) []byte {
	return il.MakeKey(kvt.makeKey(il.Key, il.IID, row), row)
}

func (kvt *table) makePrimaryKey(row []sql.Value) []byte {
	return kvt.makeKey(kvt.tl.PrimaryKey(), storage.PrimaryIID, row)
}

func (kvt *table) fetchRows(ctx context.Context, minKey, maxKey []byte) ([][]sql.Value, error) {
	it, err := kvt.st.kv.Iterate(minKey)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var vals [][]sql.Value
	for {
		err = it.Item(
			func(key, val []byte) error {
				if len(key) < 8 {
					return fmt.Errorf("kvrows: %s: key too short: %v", kvt.tn, key)
				}

				if maxKey == nil {
					if !bytes.Equal(minKey[:8], key[:8]) {
						return io.EOF
					}
				} else if bytes.Compare(maxKey, key) < 0 {
					return io.EOF
				}

				rd, err := kvt.unmarshalRowData(key, val)
				if err != nil {
					return err
				}

				if rd.Proposal != nil {
					if rd.Proposal.TXID == kvt.tx.txid {
						for _, pu := range rd.Proposal.Updates {
							if pu.SID < kvt.tx.sid {
								if len(pu.Value) > 0 {
									row, err := kvt.decodeRow(key, pu.Value)
									if err != nil {
										return err
									}
									vals = append(vals, row)
								}
								return nil
							}
						}
					} else {
						state, ver := kvt.st.getTxState(rd.Proposal.TXID)
						if state == TransactionState_Committed && ver <= kvt.tx.ver {
							if len(rd.Proposal.Updates[0].Value) > 0 {
								row, err := kvt.decodeRow(key, rd.Proposal.Updates[0].Value)
								if err != nil {
									return err
								}
								vals = append(vals, row)
							}
							return nil
						}
					}
				}

				for _, rv := range rd.Rows {
					if rv.Version <= kvt.tx.ver {
						if len(rv.Value) > 0 {
							row, err := kvt.decodeRow(key, rv.Value)
							if err != nil {
								return err
							}
							vals = append(vals, row)
						}
						return nil
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

	return vals, nil
}

func (kvt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (engine.Rows, error) {
	minKey := kvt.makePrimaryKey(minRow)
	var maxKey []byte
	if maxRow != nil {
		maxKey = kvt.makePrimaryKey(maxRow)
	}

	vals, err := kvt.fetchRows(ctx, minKey, maxKey)
	if err != nil {
		return nil, err
	}

	return &rows{
		tbl:  kvt,
		rows: vals,
	}, nil
}

func (kvt *table) IndexRows(ctx context.Context, iidx int,
	minRow, maxRow []sql.Value) (engine.IndexRows, error) {

	indexes := kvt.tl.Indexes()
	if iidx >= len(indexes) {
		panic(fmt.Sprintf("kvrows: table: %s: %d indexes: out of range: %d", kvt.tn, len(indexes),
			iidx))
	}

	il := indexes[iidx]

	var minKey []byte
	if minRow != nil {
		minKey = kvt.makeIndexKey(il, il.RowToIndexRow(minRow))
	} else {
		minKey = kvt.makeIndexKey(il, nil)
	}

	var maxKey []byte
	if maxRow != nil {
		maxKey = kvt.makeIndexKey(il, il.RowToIndexRow(maxRow))
	}

	vals, err := kvt.fetchRows(ctx, minKey, maxKey)
	if err != nil {
		return nil, err
	}

	return &indexRows{
		tbl:  kvt,
		il:   il,
		rows: vals,
	}, nil
}

func (kvt *table) proposeUpdate(upd Updater, updateKey []byte, row []sql.Value,
	mustExist bool) error {

	var rd *RowData
	err := upd.Get(updateKey,
		func(val []byte) error {
			var err error
			rd, err = kvt.unmarshalRowData(updateKey, val)
			return err
		})
	if err == io.EOF {
		rd = &RowData{}
	} else if err != nil {
		return err
	}

	var exists bool
	if rd.Proposal != nil {
		if rd.Proposal.TXID == kvt.tx.txid {
			if rd.Proposal.Updates[0].SID == kvt.tx.sid {
				return fmt.Errorf("kvrows: %s: multiple updates of %v", kvt.tn, updateKey)
			}
			exists = (len(rd.Proposal.Updates[0].Value) != 0)
		} else {
			state, ver := kvt.st.getTxState(rd.Proposal.TXID)
			if state == TransactionState_Active {
				return fmt.Errorf("kvrows: %s: conflict with proposed version of %v", kvt.tn,
					updateKey)
			} else if state == TransactionState_Committed {
				if ver > kvt.tx.ver {
					return fmt.Errorf("kvrows: %s: conflict with newer version of %v", kvt.tn,
						updateKey)
				}

				exists = (len(rd.Proposal.Updates[0].Value) != 0)
				rd.Rows = append([]*RowValue{
					&RowValue{
						Version: ver,
						Value:   rd.Proposal.Updates[0].Value,
					},
				}, rd.Rows...)
				rd.Proposal = nil
			} else { // state == TransactionState_Aborted
				if len(rd.Rows) == 0 {
					exists = false
				} else if rd.Rows[0].Version > kvt.tx.ver {
					return fmt.Errorf("kvrows: %s: conflict with newer version of %v", kvt.tn,
						updateKey)
				} else {
					exists = (len(rd.Rows[0].Value) != 0)
				}
				rd.Proposal = nil
			}
		}
	} else {
		if len(rd.Rows) == 0 {
			exists = false
		} else if rd.Rows[0].Version > kvt.tx.ver {
			return fmt.Errorf("kvrows: %s: conflict with newer version of %v", kvt.tn,
				updateKey)
		} else {
			exists = (len(rd.Rows[0].Value) != 0)
		}
	}

	if mustExist {
		if !exists {
			panic(fmt.Sprintf("kvrows: %s: row missing for update at %v", kvt.tn, updateKey))
		}
	} else {
		if exists {
			return fmt.Errorf("kvrows: %s: existing row with duplicate primary key at %v",
				kvt.tn, updateKey)
		}
	}

	kvt.tx.updatedKeys = append(kvt.tx.updatedKeys, updateKey)

	var rowValue []byte
	if len(row) > 0 {
		rowValue = encode.EncodeRowValue(row)
	}
	if rd.Proposal == nil {
		rd.Proposal = &ProposalData{
			TXID: kvt.tx.txid,
			Updates: []*ProposedUpdate{
				&ProposedUpdate{
					SID:   kvt.tx.sid,
					Value: rowValue,
				},
			},
		}
	} else {
		rd.Proposal.Updates = []*ProposedUpdate{
			&ProposedUpdate{
				SID:   kvt.tx.sid,
				Value: rowValue,
			},
			rd.Proposal.Updates[0],
		}
	}

	val, err := proto.Marshal(rd)
	if err != nil {
		return err
	}
	return upd.Set(updateKey, val)

}

func (kvt *table) Insert(ctx context.Context, rows [][]sql.Value) error {
	upd, err := kvt.st.kv.Updater()
	if err != nil {
		return err
	}

	for _, row := range rows {
		err = kvt.proposeUpdate(upd, kvt.makePrimaryKey(row), row, false)
		if err != nil {
			upd.Rollback()
			return err
		}

		for _, il := range kvt.tl.Indexes() {
			indexRow := il.RowToIndexRow(row)
			err = kvt.proposeUpdate(upd, kvt.makeIndexKey(il, indexRow), indexRow, false)
			if err != nil {
				upd.Rollback()
				return err
			}
		}
	}

	return upd.Commit(false)
}

func (kvt *table) fillIndex(ctx context.Context, il storage.IndexLayout,
	rows [][]sql.Value) error {

	upd, err := kvt.st.kv.Updater()
	if err != nil {
		return err
	}

	for _, row := range rows {
		indexRow := il.RowToIndexRow(row)
		err = kvt.proposeUpdate(upd, kvt.makeIndexKey(il, indexRow), indexRow, false)
		if err != nil {
			upd.Rollback()
			return err
		}
	}

	return upd.Commit(false)
}

func (kvt *table) FillIndex(ctx context.Context, iidx int) error {
	indexes := kvt.tl.Indexes()
	if iidx >= len(indexes) {
		panic(fmt.Sprintf("kvrows: table: %s: %d indexes: out of range: %d", kvt.tn, len(indexes),
			iidx))
	}
	il := indexes[iidx]

	rows, err := kvt.fetchRows(ctx, kvt.makePrimaryKey(nil), nil)
	if err != nil {
		return err
	}

	for len(rows) > 0 {
		var rowsChunk [][]sql.Value
		if len(rows) > 1024 {
			rowsChunk = rows[:1024]
			rows = rows[1024:]
		} else {
			rowsChunk = rows
			rows = nil
		}

		err = kvt.fillIndex(ctx, il, rowsChunk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (kvr *rows) NumColumns() int {
	return kvr.tbl.tl.NumColumns()
}

func (kvr *rows) Close() error {
	kvr.tbl = nil
	kvr.rows = nil
	kvr.idx = 0
	return nil
}

func (kvr *rows) Next(ctx context.Context) ([]sql.Value, error) {
	if kvr.idx == len(kvr.rows) {
		return nil, io.EOF
	}

	kvr.idx += 1
	return kvr.rows[kvr.idx-1], nil
}

func (kvt *table) deleteRow(ctx context.Context, row []sql.Value) error {
	upd, err := kvt.st.kv.Updater()
	if err != nil {
		return err
	}

	err = kvt.proposeUpdate(upd, kvt.makePrimaryKey(row), nil, true)
	if err != nil {
		upd.Rollback()
		return err
	}

	for _, il := range kvt.tl.Indexes() {
		indexRow := il.RowToIndexRow(row)
		err = kvt.proposeUpdate(upd, kvt.makeIndexKey(il, indexRow), nil, true)
		if err != nil {
			upd.Rollback()
			return err
		}
	}

	return upd.Commit(false)
}

func (kvr *rows) Delete(ctx context.Context) error {
	if kvr.idx == 0 {
		panic(fmt.Sprintf("kvrows: table %s no row to delete", kvr.tbl.tn))
	}

	return kvr.tbl.deleteRow(ctx, kvr.rows[kvr.idx-1])
}

func (kvt *table) updateIndexes(upd Updater, updatedCols []int,
	row, updateRow []sql.Value) error {

	indexes, updated := kvt.tl.IndexesUpdated(updatedCols)
	for idx := range indexes {
		il := indexes[idx]
		if updated[idx] {
			err := kvt.proposeUpdate(upd, kvt.makeIndexKey(il, il.RowToIndexRow(row)), nil,
				true)
			if err != nil {
				return err
			}

			indexUpdateRow := il.RowToIndexRow(updateRow)
			err = kvt.proposeUpdate(upd, kvt.makeIndexKey(il, indexUpdateRow), indexUpdateRow,
				false)
			if err != nil {
				return err
			}
		} else {
			indexUpdateRow := il.RowToIndexRow(updateRow)
			err := kvt.proposeUpdate(upd, kvt.makeIndexKey(il, indexUpdateRow), indexUpdateRow,
				true)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (kvt *table) updateRow(ctx context.Context, updatedCols []int,
	row, updateRow []sql.Value) error {

	upd, err := kvt.st.kv.Updater()
	if err != nil {
		return err
	}

	if kvt.tl.PrimaryUpdated(updatedCols) {
		err = kvt.proposeUpdate(upd, kvt.makePrimaryKey(row), nil, true)
		if err != nil {
			upd.Rollback()
			return err
		}

		err = kvt.proposeUpdate(upd, kvt.makePrimaryKey(updateRow), updateRow, false)
		if err != nil {
			upd.Rollback()
			return err
		}
	} else {
		err = kvt.proposeUpdate(upd, kvt.makePrimaryKey(updateRow), updateRow, true)
		if err != nil {
			upd.Rollback()
			return err
		}
	}

	err = kvt.updateIndexes(upd, updatedCols, row, updateRow)
	if err != nil {
		upd.Rollback()
		return err
	}

	return upd.Commit(false)
}

func (kvr *rows) Update(ctx context.Context, updatedCols []int, updateRow []sql.Value) error {
	if kvr.idx == 0 {
		panic(fmt.Sprintf("kvrows: table %s no row to update", kvr.tbl.tn))
	}

	return kvr.tbl.updateRow(ctx, updatedCols, kvr.rows[kvr.idx-1], updateRow)
}

func (kvir *indexRows) NumColumns() int {
	return len(kvir.il.Columns)
}

func (kvir *indexRows) Close() error {
	kvir.tbl = nil
	kvir.rows = nil
	kvir.idx = 0
	return nil
}

func (kvir *indexRows) Next(ctx context.Context) ([]sql.Value, error) {
	if kvir.idx == len(kvir.rows) {
		return nil, io.EOF
	}

	kvir.idx += 1
	return kvir.rows[kvir.idx-1], nil
}

func (kvir *indexRows) Delete(ctx context.Context) error {
	if kvir.idx == 0 {
		panic(fmt.Sprintf("kvrows: table %s no row to delete", kvir.tbl.tn))
	}

	row, err := kvir.getRow(ctx)
	if err != nil {
		return err
	}
	return kvir.tbl.deleteRow(ctx, row)
}

func (kvir *indexRows) Update(ctx context.Context, updatedCols []int,
	updateRow []sql.Value) error {

	if kvir.idx == 0 {
		panic(fmt.Sprintf("kvrows: table %s no row to update", kvir.tbl.tn))
	}

	row, err := kvir.getRow(ctx)
	if err != nil {
		return err
	}
	return kvir.tbl.updateRow(ctx, updatedCols, row, updateRow)
}

func (kvir *indexRows) getRow(ctx context.Context) ([]sql.Value, error) {
	row := make([]sql.Value, kvir.tbl.tl.NumColumns())
	kvir.il.IndexRowToRow(kvir.rows[kvir.idx-1], row)
	key := kvir.tbl.makePrimaryKey(row)

	vals, err := kvir.tbl.fetchRows(ctx, key, key)
	if err != nil {
		return nil, err
	}
	if len(vals) != 1 {
		return nil, fmt.Errorf("kvrows: table %s unable to get row", kvir.tbl.tn)
	}
	return vals[0], nil
}

func (kvir *indexRows) Row(ctx context.Context) ([]sql.Value, error) {
	if kvir.idx == 0 {
		panic(fmt.Sprintf("kvrows: table %s no row to get", kvir.tbl.tn))
	}

	return kvir.getRow(ctx)
}
