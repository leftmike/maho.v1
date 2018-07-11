package engine

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/sql"
)

type MakeVirtual func(ses db.Session, tctx interface{}, d Database,
	dbname, tblname sql.Identifier) (db.Table, error)

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

func lookupVirtual(ses db.Session, tctx interface{}, d Database, dbname,
	tblname sql.Identifier) (db.Table, error) {

	if maker, ok := virtualTables[tblname]; ok {
		return maker(ses, tctx, d, dbname, tblname)
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

func (vdb *virtualDatabase) LookupTable(ses db.Session, tctx interface{},
	tblname sql.Identifier) (db.Table, error) {

	maker, ok := vdb.tables[tblname]
	if !ok {
		return nil, fmt.Errorf("virtual: table %s.%s not found", vdb.name, tblname)
	}
	return maker(ses, tctx, vdb, vdb.name, tblname)
}

func (vdb *virtualDatabase) CreateTable(ses db.Session, tctx interface{}, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	return fmt.Errorf("virtual: database %s may not be modified", vdb.name)
}

func (vdb *virtualDatabase) DropTable(ses db.Session, tctx interface{}, tblname sql.Identifier,
	exists bool) error {

	return fmt.Errorf("virtual: database %s may not be modified", vdb.name)
}

func (vdb *virtualDatabase) ListTables(ses db.Session, tctx interface{}) ([]TableEntry, error) {

	var tbls []TableEntry
	for nam := range vdb.tables {
		tbls = append(tbls, TableEntry{nam, VirtualType})
	}
	return tbls, nil
}

func (vdb *virtualDatabase) Begin(lkr fatlock.Locker) interface{} {
	return nil
}

func (vdb *virtualDatabase) Commit(ses db.Session, tctx interface{}) error {
	return nil
}

func (vdb *virtualDatabase) Rollback(tctx interface{}) error {
	return nil
}

func (vdb *virtualDatabase) NextStmt(tctx interface{}) {}

type VirtualTable struct {
	Name     string
	Cols     []sql.Identifier
	ColTypes []db.ColumnType
	Values   [][]sql.Value
}

type virtualRows struct {
	name    string
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
}

func (vt *VirtualTable) Columns(ses db.Session) []sql.Identifier {
	return vt.Cols
}

func (vt *VirtualTable) ColumnTypes(ses db.Session) []db.ColumnType {
	return vt.ColTypes
}

func (vt *VirtualTable) Rows(ses db.Session) (db.Rows, error) {
	return &virtualRows{name: vt.Name, columns: vt.Cols, rows: vt.Values}, nil
}

func (vt *VirtualTable) Insert(ses db.Session, row []sql.Value) error {
	return fmt.Errorf("virtual: table %s can not be modified", vt.Name)
}

func (vr *virtualRows) Columns() []sql.Identifier {
	return vr.columns
}

func (vr *virtualRows) Close() error {
	vr.index = len(vr.rows)
	return nil
}

func (vr *virtualRows) Next(ses db.Session, dest []sql.Value) error {
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

func (vr *virtualRows) Delete(ses db.Session) error {
	return fmt.Errorf("virtual: table %s can not be modified", vr.name)
}

func (vr *virtualRows) Update(ses db.Session, updates []db.ColumnUpdate) error {
	return fmt.Errorf("virtual: table %s can not be modified", vr.name)
}

var (
	idColType     = db.ColumnType{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true}
	int32ColType  = db.ColumnType{Type: sql.IntegerType, Size: 4, NotNull: true}
	int64ColType  = db.ColumnType{Type: sql.IntegerType, Size: 8, NotNull: true}
	boolColType   = db.ColumnType{Type: sql.BooleanType, NotNull: true}
	stringColType = db.ColumnType{Type: sql.CharacterType, Size: 4096, NotNull: true}
)

func listTables(ses db.Session, tctx interface{}, d Database) ([]TableEntry, error) {
	tbls, err := d.ListTables(ses, tctx)
	if err != nil {
		return nil, err
	}

	for nam := range virtualTables {
		tbls = append(tbls, TableEntry{nam, VirtualType})
	}
	return tbls, nil
}

func makeTablesVirtual(ses db.Session, tctx interface{}, d Database,
	dbname, tblname sql.Identifier) (db.Table, error) {

	mutex.RLock()
	defer mutex.RUnlock()

	tbls, err := listTables(ses, tctx, d)
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
		Name:     fmt.Sprintf("%s.%s", dbname, tblname),
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

func makeColumnsVirtual(ses db.Session, tctx interface{}, d Database,
	dbname, tblname sql.Identifier) (db.Table, error) {

	mutex.RLock()
	defer mutex.RUnlock()

	tbls, err := listTables(ses, tctx, d)
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
			tbl, err := lookupVirtual(ses, tctx, d, dbname, te.Name)
			if err != nil {
				return nil, err
			} else if tbl == nil {
				tbl, err = d.LookupTable(ses, tctx, te.Name)
				if err != nil {
					return nil, err
				}
			}
			cols = tbl.Columns(ses)
			colTypes = tbl.ColumnTypes(ses)
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
		Name:     fmt.Sprintf("%s.%s", dbname, tblname),
		Cols:     columnsColumns,
		ColTypes: columnsColumnTypes,
		Values:   values,
	}, nil
}

func makeDatabasesVirtual(ses db.Session, tctx interface{}, d Database,
	dbname, tblname sql.Identifier) (db.Table, error) {

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
		Name:     fmt.Sprintf("%s.%s", dbname, tblname),
		Cols: []sql.Identifier{sql.ID("database"), sql.ID("engine"), sql.ID("state"),
			sql.ID("path"), sql.ID("message")},
		ColTypes: []db.ColumnType{idColType, idColType, idColType, idColType, idColType},
		Values:   values,
	}, nil
}

func makeIdentifiersVirtual(ses db.Session, tctx interface{}, d Database,
	dbname, tblname sql.Identifier) (db.Table, error) {

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
		Name:     fmt.Sprintf("%s.%s", dbname, tblname),
		Cols:     []sql.Identifier{sql.ID("name"), sql.ID("id"), sql.ID("reserved")},
		ColTypes: []db.ColumnType{idColType, int32ColType, boolColType},
		Values:   values,
	}, nil
}

func makeConfigVirtual(ses db.Session, tctx interface{}, d Database,
	dbname, tblname sql.Identifier) (db.Table, error) {

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
		Name:     fmt.Sprintf("%s.%s", dbname, tblname),
		Cols:     []sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("value")},
		ColTypes: []db.ColumnType{idColType, idColType, stringColType},
		Values:   values,
	}, nil
}

func makeEnginesVirtual(ses db.Session, tctx interface{}, d Database,
	dbname, tblname sql.Identifier) (db.Table, error) {

	values := [][]sql.Value{}

	for nam := range engines {
		values = append(values, []sql.Value{sql.StringValue(nam)})
	}

	return &VirtualTable{
		Name:     fmt.Sprintf("%s.%s", dbname, tblname),
		Cols:     []sql.Identifier{sql.ID("name")},
		ColTypes: []db.ColumnType{idColType},
		Values:   values,
	}, nil
}

func makeLocksVirtual(ses db.Session, tctx interface{}, d Database,
	dbname, tblname sql.Identifier) (db.Table, error) {

	values := [][]sql.Value{}

	for _, lk := range fatlock.Locks() {
		var place sql.Value
		if lk.Place > 0 {
			place = sql.Int64Value(lk.Place)
		}
		values = append(values, []sql.Value{
			sql.StringValue(lk.Key),
			sql.StringValue(lk.Locker),
			sql.StringValue(lk.Level.String()),
			sql.BoolValue(lk.Place == 0),
			place,
		})
	}

	return &VirtualTable{
		Name:     fmt.Sprintf("%s.%s", dbname, tblname),
		Cols: []sql.Identifier{sql.ID("key"), sql.ID("locker"), sql.ID("level"),
			sql.ID("held"), sql.ID("place")},
		ColTypes: []db.ColumnType{idColType, idColType, idColType, boolColType,
			db.ColumnType{Type: sql.IntegerType, Size: 4}},
		Values: values,
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
		sql.ID("locks"):       makeLocksVirtual,
	})
}
