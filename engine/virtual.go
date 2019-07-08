package engine

/*
import (
	"fmt"
	"io"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/sql"
)

type TableMap map[sql.Identifier]MakeVirtual

func (m *Manager) CreateVirtualTable(tblname sql.Identifier, maker MakeVirtual) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.virtualTables[tblname]; ok {
		panic(fmt.Sprintf("virtual table already created: *.%s", tblname))
	}
	m.virtualTables[tblname] = maker
}

func (m *Manager) CreateSystemTable(tblname sql.Identifier, maker MakeVirtual) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.systemTables[tblname]; ok {
		panic(fmt.Sprintf("system table already created: *.%s", tblname))
	}
	m.systemTables[tblname] = maker
}

func (m *Manager) createVirtualDatabase(name sql.Identifier, tables TableMap) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, ok := m.databases[name]
	if ok {
		panic(fmt.Sprintf("virtual database already created: %s", name))
	}
	m.databases[name] = &databaseEntry{
		database: &virtualDatabase{
			name:   name,
			tables: tables,
		},
		state: Running,
		name:  name,
		typ:   "virtual",
	}
}

func (m *Manager) lookupVirtual(ses Session, tx Transaction, d Database, dbname,
	tblname sql.Identifier) (Table, error) {

	if maker, ok := m.virtualTables[tblname]; ok {
		return maker(ses, tx, d, dbname, tblname)
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

func (vdb *virtualDatabase) LookupTable(ses Session, tx Transaction,
	tblname sql.Identifier) (Table, error) {

	maker, ok := vdb.tables[tblname]
	if !ok {
		return nil, fmt.Errorf("virtual: table %s.%s not found", vdb.name, tblname)
	}
	return maker(ses, tx, vdb, vdb.name, tblname)
}

func (vdb *virtualDatabase) CreateTable(ses Session, tx Transaction, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	return fmt.Errorf("virtual: database %s may not be modified", vdb.name)
}

func (vdb *virtualDatabase) DropTable(ses Session, tx Transaction, tblname sql.Identifier,
	exists bool) error {

	return fmt.Errorf("virtual: database %s may not be modified", vdb.name)
}

func (vdb *virtualDatabase) ListTables(ses Session, tx Transaction) ([]TableEntry, error) {

	var tbls []TableEntry
	for nam := range vdb.tables {
		tbls = append(tbls, TableEntry{nam, VirtualType})
	}
	return tbls, nil
}

func (vdb *virtualDatabase) CanClose(drop bool) bool {
	return false
}

func (vdb *virtualDatabase) Close(drop bool) error {
	return nil
}

type VirtualTable struct {
	Name     string
	Cols     []sql.Identifier
	ColTypes []sql.ColumnType
	Values   [][]sql.Value
}

type virtualRows struct {
	name    string
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
}

func (vt *VirtualTable) Columns(ses Session) []sql.Identifier {
	return vt.Cols
}

func (vt *VirtualTable) ColumnTypes(ses Session) []sql.ColumnType {
	return vt.ColTypes
}

func (vt *VirtualTable) Rows(ses Session) (Rows, error) {
	return &virtualRows{name: vt.Name, columns: vt.Cols, rows: vt.Values}, nil
}

func (vt *VirtualTable) Insert(ses Session, row []sql.Value) error {
	return fmt.Errorf("virtual: table %s can not be modified", vt.Name)
}

func (vr *virtualRows) Columns() []sql.Identifier {
	return vr.columns
}

func (vr *virtualRows) Close() error {
	vr.index = len(vr.rows)
	return nil
}

func (vr *virtualRows) Next(ses Session, dest []sql.Value) error {
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

func (vr *virtualRows) Delete(ses Session) error {
	return fmt.Errorf("virtual: table %s can not be modified", vr.name)
}

func (vr *virtualRows) Update(ses Session, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("virtual: table %s can not be modified", vr.name)
}

func (m *Manager) listTables(ses Session, tx Transaction, d Database) ([]TableEntry, error) {
	tbls, err := d.ListTables(ses, tx)
	if err != nil {
		return nil, err
	}

	for nam := range m.virtualTables {
		tbls = append(tbls, TableEntry{nam, VirtualType})
	}
	return tbls, nil
}

func (m *Manager) makeTablesVirtual(ses Session, tx Transaction, d Database, dbname,
	tblname sql.Identifier) (Table, error) {

	tbls, err := m.listTables(ses, tx, d)
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
		ColTypes: []sql.ColumnType{sql.IdColType, sql.IdColType},
		Values:   values,
	}, nil
}

var (
	columnsColumns = []sql.Identifier{sql.ID("table"), sql.ID("column"), sql.ID("type"),
		sql.ID("size"), sql.ID("fixed"), sql.ID("binary"), sql.ID("not_null"), sql.ID("default")}
	columnsColumnTypes = []sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType,
		sql.Int32ColType, sql.BoolColType, sql.BoolColType, sql.BoolColType, sql.NullStringColType}
)

func (m *Manager) makeColumnsVirtual(ses Session, tx Transaction, d Database, dbname,
	tblname sql.Identifier) (Table, error) {

	tbls, err := m.listTables(ses, tx, d)
	if err != nil {
		return nil, err
	}

	values := [][]sql.Value{}
	for _, te := range tbls {
		var cols []sql.Identifier
		var colTypes []sql.ColumnType

		if te.Name == sql.ID("db$columns") {
			cols = columnsColumns
			colTypes = columnsColumnTypes
		} else {
			tbl, err := m.lookupVirtual(ses, tx, d, dbname, te.Name)
			if err != nil {
				return nil, err
			} else if tbl == nil {
				tbl, err = d.LookupTable(ses, tx, te.Name)
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

func (m *Manager) makeDatabasesVirtual(ses Session, tx Transaction, d Database, dbname,
	tblname sql.Identifier) (Table, error) {

	values := [][]sql.Value{}
	for id, de := range m.databases {
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
		Name: fmt.Sprintf("%s.%s", dbname, tblname),
		Cols: []sql.Identifier{sql.ID("database"), sql.ID("engine"), sql.ID("state"),
			sql.ID("path"), sql.ID("message")},
		ColTypes: []sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType,
			sql.NullStringColType, sql.NullStringColType},
		Values: values,
	}, nil
}

func makeIdentifiersVirtual(ses Session, tx Transaction, d Database, dbname,
	tblname sql.Identifier) (Table, error) {

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
		ColTypes: []sql.ColumnType{sql.IdColType, sql.Int32ColType, sql.BoolColType},
		Values:   values,
	}, nil
}

func makeConfigVirtual(ses Session, tx Transaction, d Database, dbname,
	tblname sql.Identifier) (Table, error) {

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
		ColTypes: []sql.ColumnType{sql.IdColType, sql.IdColType, sql.StringColType},
		Values:   values,
	}, nil
}
*/
