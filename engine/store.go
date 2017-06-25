package engine

import (
	"fmt"
	"io"
	"maho/sql"
	"maho/store"
)

type engineDatabase struct {
	engine *Engine
}

type engineTable struct {
	engine    *Engine
	name      sql.Identifier
	columns   []sql.Column
	columnMap store.ColumnMap
}

var (
	databasesColumns = []sql.Column{
		{Name: sql.QuotedId("database"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
		{Name: sql.QuotedId("type"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
	}
	tablesColumns = []sql.Column{
		{Name: sql.QuotedId("database"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
		{Name: sql.QuotedId("table"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
		{Name: sql.QuotedId("num_columns"), Type: sql.IntegerType, Size: 4},
	}
	columnsColumns = []sql.Column{
		{Name: sql.QuotedId("database"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
		{Name: sql.QuotedId("table"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
		{Name: sql.QuotedId("column"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
		{Name: sql.QuotedId("type"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
		{Name: sql.QuotedId("size"), Type: sql.IntegerType, Size: 4},
		{Name: sql.QuotedId("width"), Type: sql.IntegerType, Size: 1},
		{Name: sql.QuotedId("fraction"), Type: sql.IntegerType, Size: 1},
		{Name: sql.QuotedId("fixed"), Type: sql.BooleanType},
		{Name: sql.QuotedId("binary"), Type: sql.BooleanType},
		{Name: sql.QuotedId("not_null"), Type: sql.BooleanType},
		{Name: sql.QuotedId("default"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
	}
	identifiersColumns = []sql.Column{
		{Name: sql.QuotedId("name"), Type: sql.CharacterType, Size: sql.MaxIdentifier},
		{Name: sql.QuotedId("identifier"), Type: sql.IntegerType, Size: 4},
		{Name: sql.QuotedId("reserved"), Type: sql.BooleanType},
	}
)

type engineRows struct {
	columns []sql.Column
	rows    [][]sql.Value
	index   int
}

func (edb *engineDatabase) Name() sql.Identifier {
	return sql.ENGINE
}

func (edb *engineDatabase) Type() sql.Identifier {
	return sql.ENGINE
}

func (edb *engineDatabase) CreateTable(name sql.Identifier, cols []sql.Column) error {
	return fmt.Errorf("engine: \"%s\" database can't be modified", sql.ENGINE)
}

func (edb *engineDatabase) DropTable(name sql.Identifier) error {
	return fmt.Errorf("engine: \"%s\" database can't be modified", sql.ENGINE)
}

func (edb *engineDatabase) Table(name sql.Identifier) (store.Table, error) {
	var cols []sql.Column

	if name == sql.DATABASES {
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

func (edb *engineDatabase) Tables() ([]sql.Identifier, [][]sql.Column) {
	return []sql.Identifier{sql.DATABASES, sql.TABLES, sql.COLUMNS, sql.IDENTIFIERS},
		[][]sql.Column{databasesColumns, tablesColumns, columnsColumns, identifiersColumns}
}

func (et *engineTable) Name() sql.Identifier {
	return et.name
}

func (et *engineTable) Columns() []sql.Column {
	return et.columns
}

func (et *engineTable) ColumnMap() store.ColumnMap {
	return et.columnMap
}

func (et *engineTable) Rows() (store.Rows, error) {
	var rows [][]sql.Value

	switch et.name {
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

func (er *engineRows) Columns() []sql.Column {
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
