package engine

import (
	"fmt"
	"io"

	"maho/row"
	"maho/sql"
	"maho/store"
)

type engineDatabase struct {
	engine *Engine
}

type engineTable struct {
	engine    *Engine
	name      sql.Identifier
	columns   []row.Column
	columnMap store.ColumnMap
}

var (
	storesColumns = []row.Column{
		{Name: sql.QuotedID("store"), Type: sql.CharacterType, Size: sql.MaxIdentifier,
			NotNull: true},
	}
	databasesColumns = []row.Column{
		{Name: sql.QuotedID("database"), Type: sql.CharacterType, Size: sql.MaxIdentifier,
			NotNull: true},
		{Name: sql.QuotedID("store"), Type: sql.CharacterType, Size: sql.MaxIdentifier,
			NotNull: true},
	}
	tablesColumns = []row.Column{
		{Name: sql.QuotedID("database"), Type: sql.CharacterType, Size: sql.MaxIdentifier,
			NotNull: true},
		{Name: sql.QuotedID("table"), Type: sql.CharacterType, Size: sql.MaxIdentifier,
			NotNull: true},
		{Name: sql.QuotedID("num_columns"), Type: sql.IntegerType, Size: 4, NotNull: true},
	}
	columnsColumns = []row.Column{
		{Name: sql.QuotedID("database"), Type: sql.CharacterType, Size: sql.MaxIdentifier,
			NotNull: true},
		{Name: sql.QuotedID("table"), Type: sql.CharacterType, Size: sql.MaxIdentifier,
			NotNull: true},
		{Name: sql.QuotedID("column"), Type: sql.CharacterType, Size: sql.MaxIdentifier,
			NotNull: true},
		{Name: sql.QuotedID("type"), Type: sql.CharacterType, Size: sql.MaxIdentifier,
			NotNull: true},
		{Name: sql.QuotedID("size"), Type: sql.IntegerType, Size: 4, NotNull: true},
		{Name: sql.QuotedID("width"), Type: sql.IntegerType, Size: 1, NotNull: true},
		{Name: sql.QuotedID("fraction"), Type: sql.IntegerType, Size: 1, NotNull: true},
		{Name: sql.QuotedID("fixed"), Type: sql.BooleanType, NotNull: true},
		{Name: sql.QuotedID("binary"), Type: sql.BooleanType, NotNull: true},
		{Name: sql.QuotedID("not_null"), Type: sql.BooleanType, NotNull: true},
		{Name: sql.QuotedID("default"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
	}
	identifiersColumns = []row.Column{
		{Name: sql.QuotedID("name"), Type: sql.CharacterType, Size: sql.MaxIdentifier,
			NotNull: true},
		{Name: sql.QuotedID("identifier"), Type: sql.IntegerType, Size: 4, NotNull: true},
		{Name: sql.QuotedID("reserved"), Type: sql.BooleanType, NotNull: true},
	}
)

type engineRows struct {
	columns []row.Column
	rows    [][]sql.Value
	index   int
}

func (edb *engineDatabase) Name() sql.Identifier {
	return sql.ENGINE
}

func (edb *engineDatabase) Type() sql.Identifier {
	return sql.ENGINE
}

func (edb *engineDatabase) CreateTable(name sql.Identifier, cols []row.Column) error {
	return fmt.Errorf("engine: \"%s\" database can't be modified", sql.ENGINE)
}

func (edb *engineDatabase) DropTable(name sql.Identifier) error {
	return fmt.Errorf("engine: \"%s\" database can't be modified", sql.ENGINE)
}

func (edb *engineDatabase) Table(name sql.Identifier) (store.Table, error) {
	var cols []row.Column

	if name == sql.STORES {
		cols = storesColumns
	} else if name == sql.DATABASES {
		cols = databasesColumns
	} else if name == sql.TABLES {
		cols = tablesColumns
	} else if name == sql.COLUMNS {
		cols = columnsColumns
	} else if name == sql.IDENTIFIERS {
		cols = identifiersColumns
	} else {
		return nil, fmt.Errorf("engine: table \"%s\" not found in database \"%s\"", name,
			sql.ENGINE)
	}

	cmap := make(store.ColumnMap)
	for i, c := range cols {
		cmap[c.Name] = i
	}
	return &engineTable{edb.engine, name, cols, cmap}, nil
}

func (edb *engineDatabase) Tables() ([]sql.Identifier, [][]row.Column) {
	return []sql.Identifier{sql.STORES, sql.DATABASES, sql.TABLES, sql.COLUMNS, sql.IDENTIFIERS},
		[][]row.Column{storesColumns, databasesColumns, tablesColumns, columnsColumns,
			identifiersColumns}
}

func (et *engineTable) Name() sql.Identifier {
	return et.name
}

func (et *engineTable) Columns() []row.Column {
	return et.columns
}

func (et *engineTable) ColumnMap() store.ColumnMap {
	return et.columnMap
}

func (et *engineTable) Rows() (store.Rows, error) {
	var rows [][]sql.Value

	switch et.name {
	case sql.STORES:
		rows = append(rows, []sql.Value{sql.ENGINE.String()})
		for _, s := range store.Stores() {
			rows = append(rows, []sql.Value{s})
		}
	case sql.DATABASES:
		for _, s := range et.engine.databases {
			rows = append(rows, []sql.Value{s.Name(), s.Type()})
		}
	case sql.TABLES:
		for _, s := range et.engine.databases {
			names, cols := s.Tables()
			for i := range names {
				rows = append(rows, []sql.Value{s.Name(), names[i], len(cols[i])})
			}
		}
	case sql.COLUMNS:
		for _, s := range et.engine.databases {
			names, cols := s.Tables()
			for i := range names {
				for _, col := range cols[i] {
					rows = append(rows,
						[]sql.Value{s.Name(), names[i], col.Name, col.Type.String(), col.Size,
							col.Width, col.Fraction, col.Fixed, col.Binary, col.NotNull,
							col.Default})
				}
			}
		}
	case sql.IDENTIFIERS:
		for id, n := range sql.Names {
			rows = append(rows, []sql.Value{n, int(id), id.IsReserved()})
		}
	}

	return &engineRows{columns: et.columns, rows: rows}, nil
}

func (et *engineTable) Insert(row []sql.Value) error {
	return fmt.Errorf("engine: \"%s.%s\" table can't be modified", sql.ENGINE, et.name)
}

func (er *engineRows) Columns() []row.Column {
	return er.columns
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
