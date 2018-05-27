package engine

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/session"
	"github.com/leftmike/maho/sql"
)

type MakeVirtual func(ctx session.Context, tctx interface{}, d Database,
	tblname sql.Identifier) (db.Table, error)

type TableMap map[sql.Identifier]MakeVirtual

var (
	virtualTables = TableMap{}
)

func CreateVirtualTable(tblname sql.Identifier, maker MakeVirtual) {
	mutex.Lock()
	defer mutex.Unlock()

	if _, ok := virtualTables[tblname]; ok {
		panic(fmt.Sprintf("virtual table already created: *.%s", tblname))
	}
	virtualTables[tblname] = maker
}

func CreateVirtualDatabase(name sql.Identifier, tables TableMap) {
	mutex.Lock()
	defer mutex.Unlock()

	_, ok := databases[name]
	if ok {
		panic(fmt.Sprintf("virtual database already created: %s", name))
	}
	databases[name] = &databaseEntry{
		database: &virtualDatabase{
			name:   name,
			tables: tables,
		},
		state: Running,
		name:  name,
		typ:   "virtual",
	}
}

func lookupVirtual(ctx session.Context, tctx interface{}, d Database,
	tblname sql.Identifier) (db.Table, error) {

	if maker, ok := virtualTables[tblname]; ok {
		return maker(ctx, tctx, d, tblname)
	}
	return nil, nil
}

type virtualDatabase struct {
	name   sql.Identifier
	tables TableMap
}

func (vdb *virtualDatabase) Message() string {
	return ""
}

func (vdb *virtualDatabase) LookupTable(ctx session.Context, tctx interface{},
	tblname sql.Identifier) (db.Table, error) {

	maker, ok := vdb.tables[tblname]
	if !ok {
		return nil, fmt.Errorf("virtual: table %s not found in database %s", tblname, vdb.name)
	}
	return maker(ctx, tctx, vdb, tblname)
}

func (vdb *virtualDatabase) CreateTable(ctx session.Context, tctx interface{},
	tblname sql.Identifier, cols []sql.Identifier, colTypes []db.ColumnType) error {

	return fmt.Errorf("virtual: database %s may not be modified", vdb.name)
}

func (vdb *virtualDatabase) DropTable(ctx session.Context, tctx interface{},
	tblname sql.Identifier, exists bool) error {

	return fmt.Errorf("virtual: database %s may not be modified", vdb.name)
}

func (vdb *virtualDatabase) ListTables(ctx session.Context, tctx interface{}) ([]TableEntry,
	error) {

	var tbls []TableEntry
	for nam := range vdb.tables {
		tbls = append(tbls, TableEntry{nam, VirtualType})
	}
	return tbls, nil
}

func (vdb *virtualDatabase) Begin() interface{} {
	return nil
}

func (vdb *virtualDatabase) Commit(ctx session.Context, tctx interface{}) error {
	return nil
}

func (vdb *virtualDatabase) Rollback(tctx interface{}) error {
	return nil
}

type VirtualTable struct {
	Cols     []sql.Identifier
	ColTypes []db.ColumnType
	Values   [][]sql.Value
}

type virtualRows struct {
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
}

func (vt *VirtualTable) Columns() []sql.Identifier {
	return vt.Cols
}

func (vt *VirtualTable) ColumnTypes() []db.ColumnType {
	return vt.ColTypes
}

func (vt *VirtualTable) Rows() (db.Rows, error) {
	return &virtualRows{columns: vt.Cols, rows: vt.Values}, nil
}

func (vt *VirtualTable) Insert(row []sql.Value) error {
	return fmt.Errorf("virtual: table can not be modified")
}

func (vr *virtualRows) Columns() []sql.Identifier {
	return vr.columns
}

func (vr *virtualRows) Close() error {
	vr.index = len(vr.rows)
	return nil
}

func (vr *virtualRows) Next(ctx session.Context, dest []sql.Value) error {
	for vr.index < len(vr.rows) {
		if vr.rows[vr.index] != nil {
			copy(dest, vr.rows[vr.index])
			vr.index += 1
			return nil
		}
		vr.index += 1
	}

	return io.EOF
}

func (tr *virtualRows) Delete(ctx session.Context) error {
	return fmt.Errorf("virtual: table can not be modified")
}

func (tr *virtualRows) Update(ctx session.Context, updates []db.ColumnUpdate) error {
	return fmt.Errorf("virtual: table can not be modified")
}

var (
	idColType     = db.ColumnType{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true}
	int32ColType  = db.ColumnType{Type: sql.IntegerType, Size: 4, NotNull: true}
	int64ColType  = db.ColumnType{Type: sql.IntegerType, Size: 8, NotNull: true}
	boolColType   = db.ColumnType{Type: sql.BooleanType, NotNull: true}
	stringColType = db.ColumnType{Type: sql.CharacterType, Size: 4096, NotNull: true}
)

func listTables(ctx session.Context, tctx interface{}, d Database) ([]TableEntry, error) {
	tbls, err := d.ListTables(ctx, tctx)
	if err != nil {
		return nil, err
	}

	for nam := range virtualTables {
		tbls = append(tbls, TableEntry{nam, VirtualType})
	}
	return tbls, nil
}

func makeTablesVirtual(ctx session.Context, tctx interface{}, d Database,
	tblname sql.Identifier) (db.Table, error) {

	mutex.RLock()
	defer mutex.RUnlock()

	tbls, err := listTables(ctx, tctx, d)
	if err != nil {
		return nil, err
	}

	values := [][]sql.Value{}
	for _, te := range tbls {
		var ttype sql.Value

		switch te.Type {
		case PhysicalType:
			ttype = sql.StringValue("physical")
		case VirtualType:
			ttype = sql.StringValue("virtual")
		default:
			panic(fmt.Sprintf("missing case for table entry type: %v", te))
		}
		values = append(values, []sql.Value{sql.StringValue(te.Name.String()), ttype})
	}

	return &VirtualTable{
		Cols:     []sql.Identifier{sql.ID("table"), sql.ID("type")},
		ColTypes: []db.ColumnType{idColType, idColType},
		Values:   values,
	}, nil
}

var (
	columnsColumns = []sql.Identifier{sql.ID("table"), sql.ID("column"), sql.ID("type"),
		sql.ID("size"), sql.ID("fixed"), sql.ID("binary"), sql.ID("not_null"), sql.ID("default")}
	columnsColumnTypes = []db.ColumnType{idColType, idColType, idColType, int32ColType,
		boolColType, boolColType, boolColType, idColType}
)

func makeColumnsVirtual(ctx session.Context, tctx interface{}, d Database,
	tblname sql.Identifier) (db.Table, error) {

	mutex.RLock()
	defer mutex.RUnlock()

	tbls, err := listTables(ctx, tctx, d)
	if err != nil {
		return nil, err
	}

	values := [][]sql.Value{}
	for _, te := range tbls {
		var cols []sql.Identifier
		var colTypes []db.ColumnType

		if te.Name == sql.ID("db$columns") {
			cols = columnsColumns
			colTypes = columnsColumnTypes
		} else {
			tbl, err := lookupVirtual(ctx, tctx, d, te.Name)
			if err != nil {
				return nil, err
			} else if tbl == nil {
				tbl, err = d.LookupTable(ctx, tctx, te.Name)
				if err != nil {
					return nil, err
				}
			}
			cols = tbl.Columns()
			colTypes = tbl.ColumnTypes()
		}
		for i, ct := range colTypes {
			var def sql.Value
			if ct.Default != nil {
				def = sql.StringValue(ct.Default.String())
			}
			values = append(values,
				[]sql.Value{
					sql.StringValue(te.Name.String()),
					sql.StringValue(cols[i].String()),
					sql.StringValue(ct.Type.String()),
					sql.Int64Value(ct.Size),
					sql.BoolValue(ct.Fixed),
					sql.BoolValue(ct.Binary),
					sql.BoolValue(ct.NotNull),
					def,
				})
		}

	}
	return &VirtualTable{
		Cols:     columnsColumns,
		ColTypes: columnsColumnTypes,
		Values:   values,
	}, nil
}

func makeDatabasesVirtual(ctx session.Context, tctx interface{}, d Database,
	tblname sql.Identifier) (db.Table, error) {

	mutex.RLock()
	defer mutex.RUnlock()

	values := [][]sql.Value{}
	for id, de := range databases {
		var msg, path sql.Value
		if de.path != "" {
			path = sql.StringValue(de.path)
		}
		if de.err != nil {
			msg = sql.StringValue(de.err.Error())
		} else if de.database != nil {
			m := de.database.Message()
			if m != "" {
				msg = sql.StringValue(m)
			}
		}
		values = append(values, []sql.Value{
			sql.StringValue(id.String()),
			sql.StringValue(de.typ),
			sql.StringValue(de.state.String()),
			path,
			msg,
		})
	}
	return &VirtualTable{
		Cols: []sql.Identifier{sql.ID("database"), sql.ID("engine"), sql.ID("state"),
			sql.ID("path"), sql.ID("message")},
		ColTypes: []db.ColumnType{idColType, idColType, idColType, idColType, idColType},
		Values:   values,
	}, nil
}

func makeIdentifiersVirtual(ctx session.Context, tctx interface{}, d Database,
	tblname sql.Identifier) (db.Table, error) {

	values := [][]sql.Value{}

	for id, n := range sql.Names {
		values = append(values,
			[]sql.Value{
				sql.StringValue(n),
				sql.Int64Value(id),
				sql.BoolValue(id.IsReserved()),
			})
	}

	return &VirtualTable{
		Cols:     []sql.Identifier{sql.ID("name"), sql.ID("id"), sql.ID("reserved")},
		ColTypes: []db.ColumnType{idColType, int32ColType, boolColType},
		Values:   values,
	}, nil
}

func makeConfigVirtual(ctx session.Context, tctx interface{}, d Database,
	tblname sql.Identifier) (db.Table, error) {

	values := [][]sql.Value{}

	for _, v := range config.Vars() {
		values = append(values,
			[]sql.Value{
				sql.StringValue(v.Name()),
				sql.StringValue(v.By()),
				sql.StringValue(v.Val()),
			})
	}

	return &VirtualTable{
		Cols:     []sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("value")},
		ColTypes: []db.ColumnType{idColType, idColType, stringColType},
		Values:   values,
	}, nil
}

func makeEnginesVirtual(ctx session.Context, tctx interface{}, d Database,
	tblname sql.Identifier) (db.Table, error) {

	values := [][]sql.Value{}

	for nam := range engines {
		values = append(values, []sql.Value{sql.StringValue(nam)})
	}

	return &VirtualTable{
		Cols:     []sql.Identifier{sql.ID("name")},
		ColTypes: []db.ColumnType{idColType},
		Values:   values,
	}, nil
}

func init() {
	CreateVirtualTable(sql.ID("db$tables"), makeTablesVirtual)
	CreateVirtualTable(sql.ID("db$columns"), makeColumnsVirtual)
	CreateVirtualDatabase(sql.ID("system"), TableMap{
		sql.ID("databases"):   makeDatabasesVirtual,
		sql.ID("identifiers"): makeIdentifiersVirtual,
		sql.ID("config"):      makeConfigVirtual,
		sql.ID("engines"):     makeEnginesVirtual,
	})
}
