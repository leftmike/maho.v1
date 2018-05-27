package memory

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/sql"
)

type tableImpl struct {
	// mutex sync.RWMutex
	columns     []sql.Identifier
	columnTypes []db.ColumnType
	rows        []*rowImpl
}

type rowImpl struct {
	version  version
	values   []sql.Value // values == nil means the row has been deleted
	previous *rowImpl
}

func (mt *tableImpl) Columns(tctx *tcontext) []sql.Identifier {
	return mt.columns
}

func (mt *tableImpl) ColumnTypes(tctx *tcontext) []db.ColumnType {
	return mt.columnTypes
}

func (mt *tableImpl) Insert(tctx *tcontext, values []sql.Value) (int, error) {
	mt.rows = append(mt.rows, &rowImpl{
		version:  makeVersion(tctx.tid, tctx.cid),
		values:   values,
		previous: nil,
	})
	return len(mt.rows) - 1, nil
}

func (mt *tableImpl) Next(tctx *tcontext, dest []sql.Value, idx int) (int, error) {
	for idx < len(mt.rows) {
		values := mt.rows[idx].getValues(tctx)
		if values != nil {
			copy(dest, values)
			idx += 1
			return idx, nil
		}
		idx += 1
	}

	return idx, io.EOF
}

func (mt *tableImpl) Delete(tctx *tcontext, idx int) error {
	row := mt.rows[idx].modifyValues(tctx, false)
	if row == nil {
		return fmt.Errorf("memory: update row: %d conflicting changes", idx)
	}
	mt.rows[idx] = row
	return nil
}

func (mt *tableImpl) Update(tctx *tcontext, updates []db.ColumnUpdate, idx int) error {
	row := mt.rows[idx].modifyValues(tctx, true)
	if row == nil {
		return fmt.Errorf("memory: update row: %d conflicting changes", idx)
	}
	mt.rows[idx] = row
	for _, up := range updates {
		row.values[up.Index] = up.Value
	}
	return nil
}

func (mt *tableImpl) CheckRow(s string, idx int, tid tid) error {
	if idx >= len(mt.rows) || mt.rows[idx] == nil {
		return fmt.Errorf("memory: %s: row: %d does not exist", s, idx)
	}
	row := mt.rows[idx]
	if !row.version.isTransaction() || row.version.getTID() != tid {
		return fmt.Errorf("memory: %s: row: %d not part of transaction: %d", s, idx, tid)
	}
	return nil
}

func (mt *tableImpl) CommitRow(idx int, v version) {
	mt.rows[idx].version = v
}

func (mt *tableImpl) RollbackRow(idx int) {
	mt.rows[idx] = mt.rows[idx].previous
}

func (mr *rowImpl) getValues(tctx *tcontext) []sql.Value {
	if mr == nil {
		return nil
	}
	if mr.version.isTransaction() {
		if mr.version.getTID() == tctx.tid {
			if mr.version.getCID() == tctx.cid {
				return nil // The current command in this transaction has already seen this row.
			}
			return mr.values
		}
		return mr.previous.getValues(tctx)
	}
	if mr.version > tctx.version {
		return mr.previous.getValues(tctx)
	}
	return mr.values
}

func (mr *rowImpl) modifyValues(tctx *tcontext, update bool) *rowImpl {
	if mr.version.isTransaction() {
		if mr.version.getTID() != tctx.tid {
			return nil
		}
	} else if mr.version > tctx.version {
		return nil
	} else {
		row := &rowImpl{
			version:  makeVersion(tctx.tid, tctx.cid),
			previous: mr,
		}
		if update {
			row.values = append([]sql.Value(nil), mr.values...)
		}
		return row
	}
	mr.version = makeVersion(tctx.tid, tctx.cid)
	return mr
}
