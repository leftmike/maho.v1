package basic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/mideng"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

var (
	errTransactionComplete = errors.New("basic: transaction already completed")
)

type basicEngine struct {
	mutex     sync.Mutex
	tableDefs map[uint64]*tableDef
	tree      *btree.BTree
	lastMID   uint64
}

type transaction struct {
	be   *basicEngine
	tree *btree.BTree
}

type tableDef struct {
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []engine.ColumnKey
	mid         uint64

	reverse uint32
	rowCols []int
}

type table struct {
	be  *basicEngine
	tx  *transaction
	def *tableDef
}

type rowItem struct {
	mid        uint64
	reverse    uint32
	numKeyCols uint8
	row        []sql.Value
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewEngine(dataDir string) (engine.Engine, error) {
	be := &basicEngine{
		tableDefs: map[uint64]*tableDef{},
		tree:      btree.New(16),
		lastMID:   63,
	}
	me, err := mideng.NewEngine("basic", be)
	if err != nil {
		return nil, err
	}
	ve := virtual.NewEngine(me)
	return ve, nil
}

func (td *tableDef) Table(ctx context.Context, tx engine.Transaction) (engine.Table, error) {
	etx := tx.(*transaction)
	return &table{
		be:  etx.be,
		tx:  etx,
		def: td,
	}, nil
}

func (_ *basicEngine) MakeTableDef(tn sql.TableName, mid uint64, cols []sql.Identifier,
	colTypes []sql.ColumnType, primary []engine.ColumnKey) (mideng.TableDef, error) {

	if len(primary) == 0 {
		panic(fmt.Sprintf("basic: table %s: missing required primary key", tn))
	}
	if len(primary) > 32 {
		panic(fmt.Sprintf("basic: table %s: primary key with too many columns", tn))
	}

	def := tableDef{
		tn:          tn,
		columns:     cols,
		columnTypes: colTypes,
		primary:     primary,
		mid:         mid,
	}

	def.reverse = 0
	def.rowCols = make([]int, len(cols))
	vn := len(primary)
	for cn := range cols {
		isValue := true

		for kn, ck := range primary {
			if ck.Number() == cn {
				def.rowCols[kn] = cn
				if ck.Reverse() {
					def.reverse |= 1 << kn
				}
				isValue = false
				break
			}
		}

		if isValue {
			def.rowCols[vn] = cn
			vn += 1
		}
	}

	return &def, nil
}

func (be *basicEngine) AllocateMID(ctx context.Context) (uint64, error) {
	be.lastMID += 1
	return be.lastMID, nil
}

func (be *basicEngine) Begin(sesid uint64) engine.Transaction {
	be.mutex.Lock()
	return &transaction{
		be:   be,
		tree: be.tree,
	}
}

func (btx *transaction) Commit(ctx context.Context) error {
	if btx.be == nil {
		return errTransactionComplete
	}

	btx.be.tree = btx.tree
	btx.be.mutex.Unlock()
	btx.be = nil
	btx.tree = nil
	return nil
}

func (btx *transaction) Rollback() error {
	if btx.be == nil {
		return errTransactionComplete
	}

	btx.be.mutex.Unlock()
	btx.be = nil
	btx.tree = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (btx *transaction) forWrite() {
	if btx.tree == btx.be.tree {
		btx.tree = btx.be.tree.Clone()
	}
}

func (def *tableDef) toItem(row []sql.Value) btree.Item {
	ri := rowItem{
		mid:        def.mid,
		reverse:    def.reverse,
		numKeyCols: uint8(len(def.primary)),
	}

	if row != nil {
		ri.row = make([]sql.Value, len(def.columns))
		for rdx := range def.rowCols {
			ri.row[rdx] = row[def.rowCols[rdx]]
		}
	}

	return ri
}

func (def *tableDef) toRow(ri rowItem) []sql.Value {
	if ri.row == nil {
		panic(fmt.Sprintf("basic: table %s contains nil row", def.tn))
	}
	row := make([]sql.Value, len(def.columns))
	for rdx := range def.rowCols {
		row[def.rowCols[rdx]] = ri.row[rdx]
	}
	return row
}

func (ri rowItem) compare(ri2 rowItem) int {
	if ri.mid < ri2.mid {
		return -1
	} else if ri.mid > ri2.mid {
		return 1
	} else if ri2.row == nil {
		if ri.row == nil {
			return 0
		}
		return -1
	} else if ri.row == nil {
		return -1
	}

	for kdx := uint8(0); kdx < ri.numKeyCols; kdx += 1 {
		cmp := sql.Compare(ri.row[kdx], ri2.row[kdx])
		if cmp == 0 {
			continue
		}
		if ri.reverse&(1<<kdx) != 0 {
			return -1 * cmp
		} else {
			return cmp
		}
	}

	return 0
}

func (ri rowItem) Less(item btree.Item) bool {
	return ri.compare(item.(rowItem)) < 0
}

func (bt *table) Columns(ctx context.Context) []sql.Identifier {
	return bt.def.columns
}

func (bt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return bt.def.columnTypes
}

func (bt *table) PrimaryKey(ctx context.Context) []engine.ColumnKey {
	return bt.def.primary
}

func (bt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (engine.Rows, error) {
	br := &rows{
		tbl: bt,
		idx: 0,
	}

	var maxItem btree.Item
	if maxRow != nil {
		maxItem = bt.def.toItem(maxRow)
	}

	bt.tx.tree.AscendGreaterOrEqual(bt.def.toItem(minRow),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.mid != bt.def.mid {
				return false
			}
			br.rows = append(br.rows, bt.def.toRow(ri))
			return true
		})
	return br, nil
}

func (bt *table) Insert(ctx context.Context, row []sql.Value) error {
	bt.tx.forWrite()

	if bt.tx.tree.Has(bt.def.toItem(row)) {
		return fmt.Errorf("basic: %s: existing row with duplicate primary key", bt.def.tn)
	}

	bt.tx.tree.ReplaceOrInsert(bt.def.toItem(row))
	return nil
}

func (br *rows) Columns() []sql.Identifier {
	return br.tbl.def.columns
}

func (br *rows) Close() error {
	br.tbl = nil
	br.rows = nil
	br.idx = 0
	return nil
}

func (br *rows) Next(ctx context.Context, dest []sql.Value) error {
	if br.idx == len(br.rows) {
		return io.EOF
	}

	copy(dest, br.rows[br.idx])
	br.idx += 1
	return nil
}

func (br *rows) Delete(ctx context.Context) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to delete", br.tbl.def.tn))
	}

	br.tbl.tx.tree.Delete(br.tbl.def.toItem(br.rows[br.idx-1]))
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to update", br.tbl.def.tn))
	}

	var primaryUpdated bool
	for _, update := range updates {
		for _, ck := range br.tbl.def.primary {
			if ck.Number() == update.Index {
				primaryUpdated = true
			}
		}
	}

	if primaryUpdated {
		br.Delete(ctx)

		for _, update := range updates {
			br.rows[br.idx-1][update.Index] = update.Value
		}

		return br.tbl.Insert(ctx, br.rows[br.idx-1])
	}

	row := append(make([]sql.Value, 0, len(br.rows[br.idx-1])), br.rows[br.idx-1]...)
	for _, update := range updates {
		row[update.Index] = update.Value
	}
	br.tbl.tx.tree.ReplaceOrInsert(br.tbl.def.toItem(row))
	return nil
}
