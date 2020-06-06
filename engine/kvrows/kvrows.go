package kvrows

//go:generate protoc --go_opt=paths=source_relative --go_out=. state.proto

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
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/encode"
	"github.com/leftmike/maho/engine/mideng"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
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

type state byte

const (
	activeState state = iota + 1
	committedState
	abortedState
)

type txState struct {
	state state
	ver   uint64
}

type kvStore struct {
	kv           KV
	mutex        sync.Mutex
	transactions map[uint64]*Transaction
	states       map[uint64]txState
	lastTID      uint64
	ver          uint64
	epoch        uint64
	commitMutex  sync.Mutex
}

type tableDef struct {
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []engine.ColumnKey
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
	td *tableDef
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewBadgerEngine(dataDir string) (engine.Engine, error) {
	kv, err := MakeBadgerKV(dataDir)
	if err != nil {
		return nil, err
	}

	kvst, init, err := makeEngine(kv)
	if err != nil {
		return nil, err
	}

	me, err := mideng.NewEngine("kvrows", kvst, init)
	if err != nil {
		return nil, err
	}
	ve := virtual.NewEngine(me)

	return ve, nil
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

func set(kv KV, key, val []byte) error {
	upd, err := kv.Update()
	if err != nil {
		return err
	}

	err = upd.Set(key, val)
	if err != nil {
		upd.Rollback()
		return err
	}

	return upd.Commit()
}

func loadTransactions(kv KV) (map[uint64]*Transaction, error) {

	return nil, nil
}

func makeEngine(kv KV) (*kvStore, bool, error) {
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

	err = set(kv, epochKey, encode.EncodeUint64(make([]byte, 0, 8), epoch))
	if err != nil {
		return nil, false, err
	}

	return &kvStore{
		kv:     kv,
		states: map[uint64]txState{},
		ver:    ver,
		epoch:  epoch,
	}, init, nil
}

func (td *tableDef) Table(ctx context.Context, tx engine.Transaction) (engine.Table, error) {
	etx := tx.(*transaction)
	return &table{
		st: etx.st,
		tx: etx,
		td: td,
	}, nil
}

func (td *tableDef) Columns() []sql.Identifier {
	return td.columns
}

func (td *tableDef) ColumnTypes() []sql.ColumnType {
	return td.columnTypes
}

func (td *tableDef) PrimaryKey() []engine.ColumnKey {
	return td.primary
}

func (td *tableDef) makeKey(row []sql.Value) []byte {
	buf := encode.EncodeUint64(make([]byte, 0, 8), uint64(td.mid))
	if row != nil {
		buf = append(buf, encode.MakeKey(td.primary, row)...)
	}
	return buf
}

func (kvst *kvStore) MakeTableDef(tn sql.TableName, mid int64, cols []sql.Identifier,
	colTypes []sql.ColumnType, primary []engine.ColumnKey) (mideng.TableDef, error) {

	if len(primary) == 0 {
		panic(fmt.Sprintf("kvrows: table %s: missing required primary key", tn))
	}

	td := tableDef{
		tn:          tn,
		columns:     cols,
		columnTypes: colTypes,
		primary:     primary,
		mid:         mid,
	}
	return &td, nil
}

func (kvst *kvStore) Begin(sesid uint64) mideng.Transaction {
	kvst.mutex.Lock()
	defer kvst.mutex.Unlock()

	kvst.lastTID += 1
	kvst.states[kvst.lastTID] = txState{state: activeState}

	return &transaction{
		st:    kvst,
		sesid: sesid,
		tid:   kvst.lastTID,
		ver:   kvst.ver,
		sid:   1,
	}
}

func (kvst *kvStore) getTxState(tid uint64) (state, uint64) {
	kvst.mutex.Lock()
	defer kvst.mutex.Unlock()

	txst := kvst.states[tid]
	return txst.state, txst.ver
}

func (kvst *kvStore) commit(ctx context.Context, tid uint64) error {
	kvst.commitMutex.Lock()
	defer kvst.commitMutex.Unlock()

	ver := kvst.ver + 1

	upd, err := kvst.kv.Update()
	if err != nil {
		kvst.rollback(tid)
		return err
	}
	err = upd.Set(versionKey, encode.EncodeUint64(make([]byte, 0, 8), ver))
	if err != nil {
		kvst.rollback(tid)
		upd.Rollback()
		return err
	}
	err = upd.Commit()
	if err != nil {
		kvst.rollback(tid)
		return err
	}

	kvst.mutex.Lock()
	kvst.states[tid] = txState{state: committedState, ver: ver}
	kvst.ver = ver
	kvst.mutex.Unlock()

	return nil
}

func (kvst *kvStore) rollback(tid uint64) error {
	kvst.mutex.Lock()
	defer kvst.mutex.Unlock()

	kvst.states[tid] = txState{state: abortedState}
	return nil
}

func (kvtx *transaction) Commit(ctx context.Context) error {
	if kvtx.st == nil {
		return errTransactionComplete
	}

	// XXX: if commit is successful, cleanup proposals

	return kvtx.st.commit(ctx, kvtx.tid)
}

func (kvtx *transaction) Rollback() error {
	if kvtx.st == nil {
		return errTransactionComplete
	}

	// XXX: cleanup proposals

	return kvtx.st.rollback(kvtx.tid)
}

func (kvtx *transaction) NextStmt() {
	kvtx.sid += 1
}

func (kvtx *transaction) Changes(cfn func(mid int64, key string, row []sql.Value) bool) {
	panic("changes not implemented")
}

func (kvt *table) Columns(ctx context.Context) []sql.Identifier {
	return kvt.td.columns
}

func (kvt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return kvt.td.columnTypes
}

func (kvt *table) PrimaryKey(ctx context.Context) []engine.ColumnKey {
	return kvt.td.primary
}

func (kvt *table) unmarshalProposal(key, val []byte) (*Proposal, error) {
	var psl Proposal
	err := proto.Unmarshal(val, &psl)
	if err != nil || len(psl.Updates) == 0 {
		return nil, fmt.Errorf("kvrows: %s: unable to unmarshal proposal at %v: %v",
			kvt.td.tn, key, val)
	}

	return &psl, nil
}

func (kvt *table) decodeRow(key, val []byte) ([]sql.Value, error) {
	row := encode.DecodeRowValue(val)
	if row == nil {
		return nil,
			fmt.Errorf("kvrows: %s: unable to decode proposed row at %v: %v",
				kvt.td.tn, key, val)
	}

	return row, nil
}

func (kvt *table) getProposedRow(key, val []byte) ([]sql.Value, bool, error) {
	psl, err := kvt.unmarshalProposal(key, val)
	if err != nil {
		return nil, false, err
	}

	if psl.TID == kvt.tx.tid {
		for _, pu := range psl.Updates {
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
	} else if state, commitVer := kvt.st.getTxState(psl.TID); state == committedState &&
		commitVer <= kvt.tx.ver {

		var row []sql.Value
		if len(psl.Updates[0].Value) > 0 {
			row, err = kvt.decodeRow(key, psl.Updates[0].Value)
			if err != nil {
				return nil, false, err
			}
		}
		return row, true, nil
	}

	return nil, false, nil
}

func (kvt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (engine.Rows, error) {
	minKey := kvt.td.makeKey(minRow)
	var maxKey []byte
	if maxRow != nil {
		maxKey = kvt.td.makeKey(maxRow)
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
					return fmt.Errorf("kvrows: %s: key too short: %v", kvt.td.tn, key)
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

func (kvt *table) prepareUpdate(upd Updater, updateKey []byte) (*Proposal, bool, error) {
	var psl *Proposal
	err := upd.Get(makeKeyVersion(updateKey, ProposalVersion),
		func(val []byte) error {
			var err error
			psl, err = kvt.unmarshalProposal(updateKey, val)
			return err
		})
	if err == io.EOF {
		return &Proposal{TID: kvt.tx.tid}, false, nil
	} else if err != nil {
		return nil, false, err
	}

	pu := psl.Updates[0]
	if psl.TID == kvt.tx.tid {
		if pu.SID == kvt.tx.sid {
			return nil, false, fmt.Errorf("kvrows: %s: multiple updates of %v",
				kvt.td.tn, updateKey)
		}
		return psl, len(pu.Value) != 0, nil
	} else {
		state, ver := kvt.st.getTxState(psl.TID)
		if state == activeState {
			return nil, false, fmt.Errorf("kvrows: %s: conflict with proposed version of %v",
				kvt.td.tn, updateKey)
		} else if state == committedState {
			if ver > kvt.tx.ver {
				return nil, false, fmt.Errorf("kvrows: %s: conflict with newer version of %v",
					kvt.td.tn, updateKey)
			}
			err := upd.Set(makeKeyVersion(updateKey, ver), pu.Value)
			if err != nil {
				return nil, false, err
			}
			return &Proposal{TID: kvt.tx.tid}, len(pu.Value) != 0, nil
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
				return fmt.Errorf("kvrows: %s: key too short: %v", kvt.td.tn, key)
			}
			ver := ^binary.BigEndian.Uint64(key[len(key)-8:])
			key = key[:len(key)-8]

			if !bytes.Equal(updateKey, key) {
				return io.EOF
			}

			if ver > kvt.tx.ver {
				return fmt.Errorf("kvrows: %s: conflict with newer version of %v",
					kvt.td.tn, updateKey)
			}

			existing = len(val) > 0
			return nil
		})
	if err == io.EOF {
		return &Proposal{TID: kvt.tx.tid}, false, nil
	} else if err != nil {
		return nil, false, err
	}

	return &Proposal{TID: kvt.tx.tid}, existing, nil
}

func (kvt *table) proposeUpdate(upd Updater, updateKey []byte, row []sql.Value,
	mustExist bool) error {

	psl, exists, err := kvt.prepareUpdate(upd, updateKey)
	if err != nil {
		return err
	}
	if mustExist {
		if !exists {
			panic(fmt.Sprintf("kvrows: %s: row missing for update at %v", kvt.td.tn, updateKey))
		}
	} else {
		if exists {
			return fmt.Errorf("kvrows: %s: existing row with duplicate primary key at %v",
				kvt.td.tn, updateKey)
		}
	}

	kvt.tx.updatedKeys = append(kvt.tx.updatedKeys, updateKey)

	var rowValue []byte
	if len(row) > 0 {
		rowValue = encode.EncodeRowValue(row)
	}
	psl.Updates = append([]*ProposedUpdate{
		&ProposedUpdate{
			SID:   kvt.tx.sid,
			Value: rowValue,
		},
	}, psl.Updates...)

	val, err := proto.Marshal(psl)
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

	err = kvt.proposeUpdate(upd, kvt.td.makeKey(row), row, false)
	if err != nil {
		upd.Rollback()
		return err
	}
	return upd.Commit()
}

func (kvr *rows) Columns() []sql.Identifier {
	return kvr.tbl.td.columns
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
		panic(fmt.Sprintf("kvrows: table %s no row to delete", kvr.tbl.td.tn))
	}

	upd, err := kvr.tbl.st.kv.Update()
	if err != nil {
		return err
	}

	err = kvr.tbl.proposeUpdate(upd, kvr.tbl.td.makeKey(kvr.rows[kvr.idx-1]), nil, true)
	if err != nil {
		upd.Rollback()
		return err
	}
	return upd.Commit()
}

func (kvr *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	if kvr.idx == 0 {
		panic(fmt.Sprintf("kvrows: table %s no row to update", kvr.tbl.td.tn))
	}

	var primaryUpdated bool
	for _, update := range updates {
		for _, ck := range kvr.tbl.td.primary {
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

	err = kvr.tbl.proposeUpdate(upd, kvr.tbl.td.makeKey(updateRow), updateRow, true)
	if err != nil {
		upd.Rollback()
		return err
	}
	return upd.Commit()
}
