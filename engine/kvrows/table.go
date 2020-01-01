package kvrows

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type table struct {
	kv       *KVRows
	tx       *transaction
	mid      uint64
	cols     []sql.Identifier
	colTypes []sql.ColumnType
	primary  []engine.ColumnKey
}

type rows struct {
	tbl         *table
	next        []byte
	idx         int
	rows        [][]sql.Value
	vers        []uint64
	noMore      bool
	needWait    bool
	blockingTID TransactionID
}

func (tbl *table) Columns(ctx context.Context) []sql.Identifier {
	return tbl.cols
}

func (tbl *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return tbl.colTypes
}

func (tbl *table) PrimaryKey(ctx context.Context) []engine.ColumnKey {
	return tbl.primary
}

func (tbl *table) Seek(ctx context.Context, row []sql.Value) (engine.Rows, error) {
	var next []byte
	if row != nil {
		next = MakeSQLKey(row, tbl.primary)
	}
	return &rows{
		tbl:  tbl,
		next: next,
	}, nil
}

func (tbl *table) Rows(ctx context.Context) (engine.Rows, error) {
	return tbl.Seek(ctx, nil)
}

func (tbl *table) Insert(ctx context.Context, row []sql.Value) error {
	return tbl.kv.st.InsertMap(ctx, tbl.kv.getState, tbl.tx.tid, tbl.tx.sid, tbl.mid,
		MakeSQLKey(row, tbl.primary), MakeRowValue(row))
}

func (r *rows) Columns() []sql.Identifier {
	return r.tbl.cols
}

func (r *rows) Close() error {
	r.noMore = true
	r.idx = 0
	r.rows = nil
	r.vers = nil
	return nil
}

func (r *rows) scanKeyValue(key []byte, ver uint64, val []byte) (bool, error) {
	dest := make([]sql.Value, len(r.tbl.cols))
	if !ParseRowValue(val, dest) {
		return false, fmt.Errorf("kvrows: key %v@%d: unable to parse row: %v", key, ver, val)
	}
	r.rows = append(r.rows, dest)
	r.vers = append(r.vers, ver)
	return len(r.rows) > 128, nil
}

func (r *rows) Next(ctx context.Context, dest []sql.Value) error {
	if r.idx >= len(r.rows) {
		if r.noMore {
			return io.EOF
		}
		if r.needWait {
			err := r.tbl.kv.waitOnTID(ctx, r.blockingTID)
			if err != nil {
				return err
			}
			r.needWait = false
		}

		kv := r.tbl.kv
		tx := r.tbl.tx

	scanAgain:
		r.rows = nil
		r.vers = nil
		next, err := r.tbl.kv.st.ScanMap(ctx, kv.getState, tx.tid, tx.sid, r.tbl.mid, r.next,
			r.scanKeyValue)
		if err != nil {
			if bp, ok := err.(*ErrBlockingProposal); ok {
				if len(r.rows) == 0 {
					err = r.tbl.kv.waitOnTID(ctx, bp.TID)
					if err != nil {
						return err
					}
					r.next = bp.Key
					goto scanAgain
				}
				r.needWait = true
				r.blockingTID = bp.TID
				next = bp.Key
			} else {
				return err
			}
		}
		if next == nil {
			r.noMore = true
		}
		if len(r.rows) == 0 {
			return io.EOF
		}

		r.idx = 0
		r.next = next
	}

	for jdx := range dest {
		dest[jdx] = r.rows[r.idx][jdx]
	}

	r.idx += 1
	return nil
}

func (r *rows) Delete(ctx context.Context) error {
	if r.idx == 0 {
		return fmt.Errorf("kvrows: table %d no row to delete", r.tbl.mid)
	}

	kv := r.tbl.kv
	tx := r.tbl.tx
	return kv.st.DeleteMap(ctx, kv.getState, tx.tid, tx.sid, r.tbl.mid,
		MakeSQLKey(r.rows[r.idx-1], r.tbl.primary), r.vers[r.idx-1])
}

type rowUpdates struct {
	updates []sql.ColumnUpdate
	rowLen  int
}

func (ru rowUpdates) modifyKeyValue(key []byte, ver uint64, val []byte) ([]byte, error) {
	dest := make([]sql.Value, ru.rowLen)
	if !ParseRowValue(val, dest) {
		return nil, fmt.Errorf("kvrows: key %v@%d: unable to parse row: %v", key, ver, val)
	}
	for _, update := range ru.updates {
		dest[update.Index] = update.Value
	}
	return MakeRowValue(dest), nil
}

func (r *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	if r.idx == 0 {
		return fmt.Errorf("kvrows: table %d no row to update", r.tbl.mid)
	}

	var primaryUpdated bool
	for _, update := range updates {
		for _, ck := range r.tbl.primary {
			if ck.Number() == update.Index {
				primaryUpdated = true
			}
		}
	}

	kv := r.tbl.kv
	tx := r.tbl.tx
	if primaryUpdated {
		err := kv.st.DeleteMap(ctx, kv.getState, tx.tid, tx.sid, r.tbl.mid,
			MakeSQLKey(r.rows[r.idx-1], r.tbl.primary), r.vers[r.idx-1])
		if err != nil {
			return err
		}

		row := make([]sql.Value, len(r.tbl.cols))
		for jdx := range row {
			row[jdx] = r.rows[r.idx-1][jdx]
		}
		for _, update := range updates {
			row[update.Index] = update.Value
		}

		return kv.st.InsertMap(ctx, kv.getState, tx.tid, tx.sid, r.tbl.mid,
			MakeSQLKey(row, r.tbl.primary), MakeRowValue(row))
	}

	return kv.st.ModifyMap(ctx, kv.getState, tx.tid, tx.sid, r.tbl.mid,
		MakeSQLKey(r.rows[r.idx-1], r.tbl.primary), r.vers[r.idx-1],
		rowUpdates{updates, len(r.tbl.cols)}.modifyKeyValue)
}
