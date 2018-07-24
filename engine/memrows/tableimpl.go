package memrows

import (
	"fmt"
	"io"
	"sync"

	"github.com/leftmike/maho/sql"
)

type tableImpl struct {
	name           string
	createdVersion version
	droppedVersion version
	dropped        bool
	previous       *tableImpl

	mutex       sync.RWMutex
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	rows        []*rowImpl
}

type rowImpl struct {
	version  version
	values   []sql.Value // values == nil means the row has been deleted
	previous *rowImpl
}

func (mt *tableImpl) getColumns(tctx *tcontext) []sql.Identifier {
	return mt.columns
}

func (mt *tableImpl) getColumnTypes(tctx *tcontext) []sql.ColumnType {
	return mt.columnTypes
}

func (mt *tableImpl) insert(tctx *tcontext, values []sql.Value) (int, error) {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	mt.rows = append(mt.rows, &rowImpl{
		version:  makeVersion(tctx.tid, tctx.cid),
		values:   values,
		previous: nil,
	})
	return len(mt.rows) - 1, nil
}

func (mt *tableImpl) next(tctx *tcontext, dest []sql.Value, idx int) (int, error) {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

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

func (mt *tableImpl) deleteRow(tctx *tcontext, idx int) error {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	row := mt.rows[idx].modifyValues(tctx, false)
	if row == nil {
		return fmt.Errorf("memrows: table %s delete row: %d conflicting changes", mt.name, idx)
	}
	mt.rows[idx] = row
	return nil
}

func (mt *tableImpl) updateRow(tctx *tcontext, updates []sql.ColumnUpdate, idx int) error {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	row := mt.rows[idx].modifyValues(tctx, true)
	if row == nil {
		return fmt.Errorf("memrows: table %s update row: %d conflicting changes", mt.name, idx)
	}
	mt.rows[idx] = row
	for _, up := range updates {
		row.values[up.Index] = up.Value
	}
	return nil
}

func (mt *tableImpl) checkRows(tid tid, rows []int) error {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	for _, idx := range rows {
		if idx >= len(mt.rows) || mt.rows[idx] == nil {
			return fmt.Errorf("memrows: table %s row %d does not exist", mt.name, idx)
		}
		row := mt.rows[idx]
		if !row.version.isTransaction() || row.version.getTID() != tid {
			return fmt.Errorf("memrows: table %s row %d not part of transaction: %d", mt.name, idx,
				tid)
		}
	}
	return nil
}

func (mt *tableImpl) commitRows(v version, rows []int) {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	for _, idx := range rows {
		mt.rows[idx].version = v
	}
}

func (mt *tableImpl) rollbackRows(rows []int) {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	for _, idx := range rows {
		mt.rows[idx] = mt.rows[idx].previous
	}
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
			return nil // A different transaction has a pending modification.
		}
	} else if mr.version > tctx.version {
		return nil // A newer version (than that of this transaction) of this row exists.
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
