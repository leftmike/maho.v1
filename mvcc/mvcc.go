package mvcc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

const (
	dbExtension = ".mahodb"
)

type mvcc struct {
	dir       string
	databases map[sql.Identifier]*database
}

type transaction struct {
	me *mvcc
}

/*
type basicTable struct {
	id          engine.TableID
	pageNum     engine.PageNum
	columns     []sql.Identifier
	columnTypes []db.ColumnType
	rows        [][]sql.Value
}

type basicRows struct {
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
	haveRow bool
}
*/

func init() {
	engine.Register("mvcc", &mvcc{})
}

func (me *mvcc) Start(dir string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("mvcc: start: %s is not a directory", dir)
	}
	me.dir = dir
	me.databases = map[sql.Identifier]*database{}

	matches, err := filepath.Glob(filepath.Join(me.dir, "*"+dbExtension))
	if err != nil {
		return err
	}
	for _, m := range matches {
		ndb, err := me.loadDatabase(m)
		if err != nil {
			return err
		}
		n := filepath.Base(m)
		me.databases[sql.ID(n[:strings.LastIndex(n, dbExtension)])] = ndb
	}

	// XXX: dir + "/maho.wal"
	return nil
}

func (me *mvcc) CreateDatabase(dbname sql.Identifier) error {
	if _, dup := me.databases[dbname]; dup {
		return fmt.Errorf("mvcc: database %s already exists", dbname)
	}
	ndb, err := me.createDatabase(filepath.Join(me.dir, dbname.String()+dbExtension))
	if err != nil {
		return err
	}
	me.databases[dbname] = ndb
	return nil
}

func (me *mvcc) ListDatabases() []sql.Identifier {
	var ids []sql.Identifier
	for id := range me.databases {
		ids = append(ids, id)
	}
	return ids
}

func (me *mvcc) Begin() (engine.Transaction, error) {
	return &transaction{me}, nil
}

func (mtx *transaction) LookupTable(ctx context.Context, dbname,
	tblname sql.Identifier) (db.Table, error) {

	return nil, fmt.Errorf("mvcc: lookup table not implemented")
	/*
		bdb, ok := btx.e.databases[dbname]
		if !ok {
			return nil, fmt.Errorf("basic: database %s not found", dbname)
		}
		tbl, ok := bdb.tables[tblname]
		if !ok {
			return nil, fmt.Errorf("basic: table %s not found in database %s", tblname, dbname)
		}
		return tbl, nil
	*/
}

func (mtx *transaction) CreateTable(ctx context.Context, dbname, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	return fmt.Errorf("mvcc: create table not implemented")
	/*
		bdb, ok := btx.e.databases[dbname]
		if !ok {
			return fmt.Errorf("basic: database %s not found", dbname)
		}
		if _, dup := bdb.tables[tblname]; dup {
			return fmt.Errorf("basic: table %s already exists in database %s", tblname, dbname)
		}

		bdb.tables[tblname] = &basicTable{
			id:          bdb.nextID,
			pageNum:     engine.PageNum(rand.Uint64()),
			columns:     cols,
			columnTypes: colTypes,
			rows:        nil,
		}
		bdb.nextID += 1
		return nil
	*/
}

func (mtx *transaction) DropTable(ctx context.Context, dbname, tblname sql.Identifier,
	exists bool) error {

	return fmt.Errorf("mvcc: drop table not implemented")
	/*
		bdb, ok := btx.e.databases[dbname]
		if !ok {
			return fmt.Errorf("basic: database %s not found", dbname)
		}
		if _, ok := bdb.tables[tblname]; !ok {
			if exists {
				return nil
			}
			return fmt.Errorf("basic: table %s does not exist in database %s", tblname, dbname)
		}
		delete(bdb.tables, tblname)
		return nil
	*/
}

func (mtx *transaction) ListTables(ctx context.Context,
	dbname sql.Identifier) ([]engine.TableEntry, error) {

	return nil, fmt.Errorf("mvcc: list tables not implemented")
	/*
		bdb, ok := btx.e.databases[dbname]
		if !ok {
			return nil, fmt.Errorf("basic: database %s not found", dbname)
		}
		var tbls []engine.TableEntry
		for name, tbl := range bdb.tables {
			tbls = append(tbls, engine.TableEntry{name, tbl.id, tbl.pageNum, engine.VirtualType})
		}
		return tbls, nil
	*/
}

func (mtx *transaction) Commit(ctx context.Context) error {
	return fmt.Errorf("mvcc: commit not implemented")
	/*
		btx.e = nil
		return nil
	*/
}

func (mtx *transaction) Rollback() error {
	return fmt.Errorf("mvcc: rollback not implemented")
	/*
		btx.e = nil
		return nil
	*/
}

/*
func (bt *basicTable) Columns() []sql.Identifier {
	return bt.columns
}

func (bt *basicTable) ColumnTypes() []db.ColumnType {
	return bt.columnTypes
}

func (bt *basicTable) Rows() (db.Rows, error) {
	return &basicRows{columns: bt.columns, rows: bt.rows}, nil
}

func (bt *basicTable) Insert(row []sql.Value) error {
	bt.rows = append(bt.rows, row)
	return nil
}

func (br *basicRows) Columns() []sql.Identifier {
	return br.columns
}

func (br *basicRows) Close() error {
	br.index = len(br.rows)
	br.haveRow = false
	return nil
}

func (br *basicRows) Next(ctx context.Context, dest []sql.Value) error {
	for br.index < len(br.rows) {
		if br.rows[br.index] != nil {
			copy(dest, br.rows[br.index])
			br.index += 1
			br.haveRow = true
			return nil
		}
		br.index += 1
	}

	br.haveRow = false
	return io.EOF
}

func (br *basicRows) Delete(ctx context.Context) error {
	if !br.haveRow {
		return fmt.Errorf("basic: no row to delete")
	}
	br.haveRow = false
	br.rows[br.index-1] = nil
	return nil
}

func (br *basicRows) Update(ctx context.Context, updates []db.ColumnUpdate) error {
	if !br.haveRow {
		return fmt.Errorf("basic: no row to update")
	}
	row := br.rows[br.index-1]
	for _, up := range updates {
		row[up.Index] = up.Value
	}
	return nil
}
*/
