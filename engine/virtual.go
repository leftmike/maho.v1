package engine

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/sql"
)

type MakeVirtual func(dbname, tblname sql.Identifier) (db.Table, error)

type tableMap map[sql.Identifier]MakeVirtual

var (
	virtualDatabases = map[sql.Identifier]tableMap{
		0: tableMap{},
	}
)

func duplicatePanic(dbname, tblname sql.Identifier) {
	if dbname == 0 {
		panic(fmt.Sprintf("virtual table already created: *.%s", tblname))
	} else {
		panic(fmt.Sprintf("virtual table already created: %s.%s", dbname, tblname))
	}
}

func duplicateCheck(dbname, tblname sql.Identifier) {
	tblmap, ok := virtualDatabases[dbname]
	if ok {
		if _, dup := tblmap[tblname]; dup {
			duplicatePanic(dbname, tblname)
		} else if dbname == 0 {
			for dbname, tblmap := range virtualDatabases {
				if dbname != 0 {
					if _, dup := tblmap[tblname]; dup {
						duplicatePanic(dbname, tblname)
					}
				}
			}
		}
	}
}

func CreateVirtual(dbname, tblname sql.Identifier, maker MakeVirtual) {
	duplicateCheck(dbname, tblname)
	tblmap, ok := virtualDatabases[dbname]
	if !ok {
		tblmap = tableMap{}
		virtualDatabases[dbname] = tblmap
	}
	tblmap[tblname] = maker
}

func lookupVirtual(dbname, tblname sql.Identifier) (db.Table, error) {
	tblmap, ok := virtualDatabases[dbname]
	if !ok {
		return nil, nil
	}
	if maker, ok := tblmap[tblname]; ok {
		return maker(dbname, tblname)
	}
	if dbname != 0 {
		if maker, ok := virtualDatabases[0][tblname]; ok {
			return maker(dbname, tblname)
		}
	}
	return nil, nil
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

func (vt *VirtualTable) DeleteRows() (db.Rows, error) {
	return nil, fmt.Errorf("table can not be modified")
}

func (vt *VirtualTable) UpdateRows() (db.Rows, error) {
	return nil, fmt.Errorf("table can not be modified")
}

func (vt *VirtualTable) Insert(row []sql.Value) error {
	return fmt.Errorf("table can not be modified")
}

func (vr *virtualRows) Columns() []sql.Identifier {
	return vr.columns
}

func (vr *virtualRows) Close() error {
	vr.index = len(vr.rows)
	return nil
}

func (vr *virtualRows) Next(dest []sql.Value) error {
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

func (tr *virtualRows) Delete() error {
	return fmt.Errorf("table can not be modified")
}

func (tr *virtualRows) Update(updates []db.ColumnUpdate) error {
	return fmt.Errorf("table can not be modified")
}

var (
	idColType    = db.ColumnType{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true}
	int32ColType = db.ColumnType{Type: sql.IntegerType, Size: 4, NotNull: true}
	int64ColType = db.ColumnType{Type: sql.IntegerType, Size: 8, NotNull: true}
	boolColType  = db.ColumnType{Type: sql.BooleanType, NotNull: true}
)

func listTables(dbname sql.Identifier) ([]TableEntry, error) {
	tblmap, ok := virtualDatabases[dbname]
	tbls, err := e.ListTables(dbname)
	if !ok && err != nil {
		return nil, err
	}

	if ok {
		for nam := range tblmap {
			tbls = append(tbls, TableEntry{nam, 0, 0, VirtualType})
		}
	}

	for nam := range virtualDatabases[0] {
		tbls = append(tbls, TableEntry{nam, 0, 0, VirtualType})
	}
	return tbls, nil
}

func MakeTablesVirtual(dbname, tblname sql.Identifier) (db.Table, error) {
	tbls, err := listTables(dbname)
	if err != nil {
		return nil, err
	}

	values := [][]sql.Value{}
	for _, te := range tbls {
		var tid, pn, ttype sql.Value

		if te.ID != 0 {
			tid = sql.Int64Value(te.ID)
		}
		if te.PageNum != 0 {
			pn = sql.Int64Value(te.PageNum)
		}

		switch te.Type {
		case PhysicalType:
			ttype = sql.StringValue("physical")
		case VirtualType:
			ttype = sql.StringValue("virtual")
		default:
			panic(fmt.Sprintf("missing case for table entry type: %v", te))
		}
		values = append(values, []sql.Value{sql.StringValue(te.Name.String()), tid, pn, ttype})
	}

	return &VirtualTable{
		Cols: []sql.Identifier{sql.ID("table"), sql.ID("id"), sql.ID("page_num"),
			sql.ID("type")},
		ColTypes: []db.ColumnType{idColType, int32ColType, int64ColType, idColType},
		Values:   values,
	}, nil
}

var (
	columnsColumns = []sql.Identifier{sql.ID("table"), sql.ID("column"), sql.ID("type"),
		sql.ID("size"), sql.ID("fixed"), sql.ID("binary"), sql.ID("not_null"), sql.ID("default")}
	columnsColumnTypes = []db.ColumnType{idColType, idColType, idColType, int32ColType,
		boolColType, boolColType, boolColType, idColType}
)

func MakeColumnsVirtual(dbname, tblname sql.Identifier) (db.Table, error) {
	tbls, err := listTables(dbname)
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
			tbl, err := LookupTable(dbname, te.Name)
			if err != nil {
				return nil, err
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

func MakeDatabasesVirtual(dbname, tblname sql.Identifier) (db.Table, error) {
	names, err := e.ListDatabases()
	if err != nil {
		return nil, err
	}

	values := [][]sql.Value{}
	for _, n := range names {
		values = append(values, []sql.Value{sql.StringValue(n)})
	}
	for id := range virtualDatabases {
		if id != 0 {
			values = append(values, []sql.Value{sql.StringValue(id.String())})
		}
	}
	return &VirtualTable{
		Cols:     []sql.Identifier{sql.ID("database")},
		ColTypes: []db.ColumnType{idColType},
		Values:   values,
	}, nil
}

func MakeIdentifiersVirtual(dbname, tblname sql.Identifier) (db.Table, error) {
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

func init() {
	CreateVirtual(0, sql.ID("db$tables"), MakeTablesVirtual)
	CreateVirtual(0, sql.ID("db$columns"), MakeColumnsVirtual)
	CreateVirtual(sql.ID("system"), sql.ID("databases"), MakeDatabasesVirtual)
	CreateVirtual(sql.ID("system"), sql.ID("identifiers"), MakeIdentifiersVirtual)
}
