package memory

import (
	"fmt"
	"io"
	"math/rand"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/session"
	"github.com/leftmike/maho/sql"
)

type eng struct{}

type database struct {
	name   sql.Identifier
	nextID engine.TableID
	tables map[sql.Identifier]*table
}

type table struct {
	id          engine.TableID
	pageNum     engine.PageNum
	columns     []sql.Identifier
	columnTypes []db.ColumnType
	rows        [][]sql.Value
}

type rows struct {
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
	haveRow bool
}

func init() {
	engine.Register("memory", &eng{})
}

func (me *eng) AttachDatabase(name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	return nil, fmt.Errorf("memory: attach database not supported")
}

func (me *eng) CreateDatabase(name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	return &database{
		name:   name,
		nextID: 1,
		tables: map[sql.Identifier]*table{},
	}, nil
}

func (mdb *database) Message() string {
	return ""
}

func (mdb *database) LookupTable(ctx session.Context, tx engine.TransContext,
	tblname sql.Identifier) (db.Table, error) {

	tbl, ok := mdb.tables[tblname]
	if !ok {
		return nil, fmt.Errorf("memory: table %s not found in database %s", tblname, mdb.name)
	}
	return tbl, nil
}

func (mdb *database) CreateTable(ctx session.Context, tx engine.TransContext,
	tblname sql.Identifier, cols []sql.Identifier, colTypes []db.ColumnType) error {

	if _, dup := mdb.tables[tblname]; dup {
		return fmt.Errorf("memory: table %s already exists in database %s", tblname, mdb.name)
	}

	mdb.tables[tblname] = &table{
		id:          mdb.nextID,
		pageNum:     engine.PageNum(rand.Uint64()),
		columns:     cols,
		columnTypes: colTypes,
		rows:        nil,
	}
	mdb.nextID += 1
	return nil
}

func (mdb *database) DropTable(ctx session.Context, tx engine.TransContext, tblname sql.Identifier,
	exists bool) error {

	if _, ok := mdb.tables[tblname]; !ok {
		if exists {
			return nil
		}
		return fmt.Errorf("memory: table %s does not exist in database %s", tblname, mdb.name)
	}
	delete(mdb.tables, tblname)
	return nil
}

func (mdb *database) ListTables(ctx session.Context,
	tx engine.TransContext) ([]engine.TableEntry, error) {

	var tbls []engine.TableEntry
	for name, tbl := range mdb.tables {
		tbls = append(tbls, engine.TableEntry{
			Name:    name,
			ID:      tbl.id,
			PageNum: tbl.pageNum,
			Type:    engine.VirtualType,
		})
	}
	return tbls, nil
}

func (mdb *database) NewTransContext() engine.TransContext {
	return nil // XXX
}

func (mt *table) Columns() []sql.Identifier {
	return mt.columns
}

func (mt *table) ColumnTypes() []db.ColumnType {
	return mt.columnTypes
}

func (mt *table) Rows() (db.Rows, error) {
	return &rows{columns: mt.columns, rows: mt.rows}, nil
}

func (mt *table) Insert(row []sql.Value) error {
	mt.rows = append(mt.rows, row)
	return nil
}

func (mr *rows) Columns() []sql.Identifier {
	return mr.columns
}

func (mr *rows) Close() error {
	mr.index = len(mr.rows)
	mr.haveRow = false
	return nil
}

func (mr *rows) Next(ctx session.Context, dest []sql.Value) error {
	for mr.index < len(mr.rows) {
		if mr.rows[mr.index] != nil {
			copy(dest, mr.rows[mr.index])
			mr.index += 1
			mr.haveRow = true
			return nil
		}
		mr.index += 1
	}

	mr.haveRow = false
	return io.EOF
}

func (mr *rows) Delete(ctx session.Context) error {
	if !mr.haveRow {
		return fmt.Errorf("memory: no row to delete")
	}
	mr.haveRow = false
	mr.rows[mr.index-1] = nil
	return nil
}

func (mr *rows) Update(ctx session.Context, updates []db.ColumnUpdate) error {
	if !mr.haveRow {
		return fmt.Errorf("memory: no row to update")
	}
	row := mr.rows[mr.index-1]
	for _, up := range updates {
		row[up.Index] = up.Value
	}
	return nil
}
