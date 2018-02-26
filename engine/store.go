package engine

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/store"
)

type engineDatabase struct {
	engine *Engine
}

type engineTable struct {
	engine      *Engine
	name        sql.Identifier
	columns     []sql.Identifier
	columnTypes []db.ColumnType
}

var (
	storesColumns     = []sql.Identifier{sql.QuotedID("store")}
	storesColumnTypes = []db.ColumnType{
		{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true},
	}

	databasesColumns     = []sql.Identifier{sql.QuotedID("database"), sql.QuotedID("store")}
	databasesColumnTypes = []db.ColumnType{
		{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true},
		{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true},
	}

	tablesColumns = []sql.Identifier{sql.QuotedID("database"), sql.QuotedID("table"),
		sql.QuotedID("num_columns")}
	tablesColumnTypes = []db.ColumnType{
		{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true},
		{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true},
		{Type: sql.IntegerType, Size: 4, NotNull: true},
	}

	columnsColumns = []sql.Identifier{sql.QuotedID("database"), sql.QuotedID("table"),
		sql.QuotedID("column"), sql.QuotedID("type"), sql.QuotedID("size"),
		sql.QuotedID("fixed"), sql.QuotedID("binary"), sql.QuotedID("not_null"),
		sql.QuotedID("default")}

	columnsColumnTypes = []db.ColumnType{
		{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true},
		{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true},
		{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true},
		{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true},
		{Type: sql.IntegerType, Size: 4, NotNull: true},
		{Type: sql.BooleanType, NotNull: true},
		{Type: sql.BooleanType, NotNull: true},
		{Type: sql.BooleanType, NotNull: true},
		{Type: sql.CharacterType, Size: sql.MaxIdentifier},
	}

	identifiersColumns = []sql.Identifier{sql.QuotedID("name"), sql.QuotedID("identifier"),
		sql.QuotedID("reserved")}
	identifiersColumnTypes = []db.ColumnType{
		{Type: sql.CharacterType, Size: sql.MaxIdentifier, NotNull: true},
		{Type: sql.IntegerType, Size: 4, NotNull: true},
		{Type: sql.BooleanType, NotNull: true},
	}
)

type engineRows struct {
	columns     []sql.Identifier
	columnTypes []db.ColumnType
	rows        [][]sql.Value
	index       int
}

func (edb *engineDatabase) Name() sql.Identifier {
	return sql.ENGINE
}

func (edb *engineDatabase) Type() sql.Identifier {
	return sql.ENGINE
}

func (edb *engineDatabase) Table(name sql.Identifier) (db.Table, error) {
	var cols []sql.Identifier
	var colTypes []db.ColumnType

	if name == sql.STORES {
		cols = storesColumns
		colTypes = storesColumnTypes
	} else if name == sql.DATABASES {
		cols = databasesColumns
		colTypes = databasesColumnTypes
	} else if name == sql.TABLES {
		cols = tablesColumns
		colTypes = tablesColumnTypes
	} else if name == sql.COLUMNS {
		cols = columnsColumns
		colTypes = columnsColumnTypes
	} else if name == sql.IDENTIFIERS {
		cols = identifiersColumns
		colTypes = identifiersColumnTypes
	} else {
		return nil, fmt.Errorf("engine: table \"%s\" not found in database \"%s\"", name,
			sql.ENGINE)
	}

	return &engineTable{edb.engine, name, cols, colTypes}, nil
}

func (edb *engineDatabase) Tables() []sql.Identifier {
	return []sql.Identifier{sql.STORES, sql.DATABASES, sql.TABLES, sql.COLUMNS, sql.IDENTIFIERS}
}

func (et *engineTable) Name() sql.Identifier {
	return et.name
}

func (et *engineTable) Columns() []sql.Identifier {
	return et.columns
}

func (et *engineTable) ColumnTypes() []db.ColumnType {
	return et.columnTypes
}

func (et *engineTable) Rows() (db.Rows, error) {
	var rows [][]sql.Value

	switch et.name {
	case sql.STORES:
		rows = append(rows, []sql.Value{sql.StringValue(sql.ENGINE.String())})
		for _, s := range store.Stores() {
			rows = append(rows, []sql.Value{sql.StringValue(s)})
		}
	case sql.DATABASES:
		for _, s := range et.engine.databases {
			rows = append(rows,
				[]sql.Value{
					sql.StringValue(s.Name().String()),
					sql.StringValue(s.Type().String()),
				})
		}
	case sql.TABLES:
		for _, dbase := range et.engine.databases {
			names := dbase.Tables()
			for _, n := range names {
				tbl, err := dbase.Table(n)
				if err != nil {
					continue
				}
				rows = append(rows,
					[]sql.Value{
						sql.StringValue(dbase.Name().String()),
						sql.StringValue(n.String()),
						sql.Int64Value(len(tbl.Columns())),
					})
			}
		}
	case sql.COLUMNS:
		for _, dbase := range et.engine.databases {
			names := dbase.Tables()
			for _, n := range names {
				tbl, err := dbase.Table(n)
				if err != nil {
					continue
				}
				cols := tbl.Columns()
				for i, ct := range tbl.ColumnTypes() {
					var def sql.Value
					if ct.Default != nil {
						def = sql.StringValue(ct.Default.String())
					}
					rows = append(rows,
						[]sql.Value{
							sql.StringValue(dbase.Name().String()),
							sql.StringValue(n.String()),
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
		}
	case sql.IDENTIFIERS:
		for id, n := range sql.Names {
			rows = append(rows,
				[]sql.Value{
					sql.StringValue(n),
					sql.Int64Value(id),
					sql.BoolValue(id.IsReserved()),
				})
		}
	}

	return &engineRows{columns: et.columns, columnTypes: et.columnTypes, rows: rows}, nil
}

func (er *engineRows) Columns() []sql.Identifier {
	return er.columns
}

func (er *engineRows) ColumnTypes() []db.ColumnType {
	return er.columnTypes
}

func (er *engineRows) Close() error {
	er.index = len(er.rows)
	return nil
}

func (er *engineRows) Next(dest []sql.Value) error {
	if er.index == len(er.rows) {
		return io.EOF
	}
	copy(dest, er.rows[er.index])
	er.index += 1
	return nil
}
