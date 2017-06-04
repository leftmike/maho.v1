package engine

import (
	"fmt"
	"io"
	"maho/sql"
	"maho/store"
	"maho/store/basic"
)

type database struct {
	name  sql.Identifier
	store store.Store
}

var (
	databases       = make(map[sql.Identifier]*database)
	defaultDatabase sql.Identifier
)

func Start(id sql.Identifier, name string) error {
	if id == sql.ENGINE {
		return fmt.Errorf("engine: \"%s\" not allowed as database name", id)
	}
	s, err := basic.Make(name)
	if err != nil {
		return err
	}

	databases[id] = &database{id, s}
	defaultDatabase = id
	return nil
}

func init() {
	databases[sql.ENGINE] = &database{sql.ENGINE, &engineStore{}}
}

type engineStore struct{}

type engineTable struct {
	name    sql.Identifier
	columns []sql.Column
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
	}
)

type engineRows struct {
	columns []sql.Column
	rows    [][]store.Value
	index   int
}

func (es *engineStore) Type() sql.Identifier {
	return sql.ENGINE
}
func (es *engineStore) CreateTable(name sql.Identifier, cols []sql.Column) error {
	return fmt.Errorf("engine: \"%s\" database can't be modified", sql.ENGINE)
}

func (es *engineStore) Table(name sql.Identifier) (store.Table, error) {
	if name == sql.DATABASES {
		return &engineTable{sql.DATABASES, databasesColumns}, nil
	} else if name == sql.TABLES {
		return &engineTable{sql.TABLES, tablesColumns}, nil
	} else if name == sql.COLUMNS {
		return &engineTable{sql.COLUMNS, columnsColumns}, nil
	}

	return nil, fmt.Errorf("engine: table \"%s\" not found in database \"%s\"", name, sql.ENGINE)
}

func (es *engineStore) Tables() ([]sql.Identifier, [][]sql.Column) {
	return []sql.Identifier{sql.DATABASES, sql.TABLES, sql.COLUMNS},
		[][]sql.Column{databasesColumns, tablesColumns, columnsColumns}
}

func (et *engineTable) Name() sql.Identifier {
	return et.name
}

func (et *engineTable) Columns() []sql.Column {
	return et.columns
}

func (et *engineTable) Rows() (store.Rows, error) {
	var rows [][]store.Value

	switch et.name {
	case sql.DATABASES:
		for _, db := range databases {
			rows = append(rows, []store.Value{db.name, db.store.Type()})
		}
	case sql.TABLES:
		for _, db := range databases {
			names, cols := db.store.Tables()
			for i := range names {
				rows = append(rows, []store.Value{db.name, names[i], len(cols[i])})
			}
		}
	case sql.COLUMNS:
		for _, db := range databases {
			names, cols := db.store.Tables()
			for i := range names {
				for _, col := range cols[i] {
					rows = append(rows,
						[]store.Value{db.name, names[i], col.Name, col.Type.String(), col.Size,
							col.Width, col.Fraction, col.Fixed, col.Binary})
				}
			}
		}
	}

	return &engineRows{columns: et.columns, rows: rows}, nil
}

func (er *engineRows) Columns() []sql.Column {
	return er.columns
}

func (er *engineRows) Close() error {
	er.index = len(er.rows)
	return nil
}

func (er *engineRows) Next(dest []store.Value) error {
	if er.index == len(er.rows) {
		return io.EOF
	}
	copy(dest, er.rows[er.index])
	er.index += 1
	return nil
}
