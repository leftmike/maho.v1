package engine

//go:generate protoc --go_opt=paths=source_relative --go_out=. typemd.proto

import (
	"fmt"

	"github.com/golang/protobuf/proto"

	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type constraint struct {
	name   sql.Identifier
	typ    sql.ConstraintType
	colNum int
}

type checkConstraint struct {
	name      sql.Identifier
	check     sql.CExpr
	checkExpr string
}

type foreignKey struct {
	name     sql.Identifier
	keyCols  []int
	refTable sql.TableName
	refIndex sql.Identifier
}

type TableType struct {
	ver         int64
	cols        []sql.Identifier
	colTypes    []sql.ColumnType
	primary     []sql.ColumnKey
	indexes     []sql.IndexType
	constraints []constraint
	checks      []checkConstraint
	foreignKeys []foreignKey
	//foreignRefs []foreignRef
	triggers []trigger
	events   int64
}

func MakeTableType(cols []sql.Identifier, colTypes []sql.ColumnType,
	primary []sql.ColumnKey) *TableType {

	return &TableType{
		ver:      1,
		cols:     cols,
		colTypes: colTypes,
		primary:  primary,
	}
}

func (tt *TableType) Version() int64 {
	return tt.ver
}

func (tt *TableType) Columns() []sql.Identifier {
	return tt.cols
}

func (tt *TableType) ColumnTypes() []sql.ColumnType {
	return tt.colTypes
}

func (tt *TableType) PrimaryKey() []sql.ColumnKey {
	return tt.primary
}

func (tt *TableType) Indexes() []sql.IndexType {
	return tt.indexes
}

func (tt *TableType) addTrigger(events int64, trig sql.Trigger) {
	typ := trig.Type()
	_, ok := TriggerDecoders[typ]
	if !ok {
		panic(fmt.Sprintf("engine: missing trigger decoder: %s", typ))
	}

	tt.triggers = append(tt.triggers,
		trigger{
			typ:    typ,
			events: events,
			trig:   trig,
		})
	tt.events |= events
}

func addColumn(cols []int, num int) []int {
	for _, col := range cols {
		if col == num {
			return cols
		}
	}

	return append(cols, num)
}

func (tt *TableType) makeIndexType(nam sql.Identifier, key []sql.ColumnKey,
	unique bool) sql.IndexType {

	if !unique {
		for _, ck := range tt.PrimaryKey() {
			if !sql.ColumnInKey(key, ck) {
				key = append(key, ck)
			}
		}
	}

	var cols []int
	for _, ck := range key {
		cols = append(cols, ck.Column())
	}

	for _, ck := range tt.PrimaryKey() {
		cols = addColumn(cols, ck.Column())
	}

	return sql.IndexType{
		Name:    nam,
		Key:     key,
		Columns: cols,
		Unique:  unique,
	}
}

func (tt *TableType) AddIndex(idxname sql.Identifier, unique bool,
	key []sql.ColumnKey) (*TableType, sql.IndexType, bool) {

	var it sql.IndexType

	for _, it := range tt.indexes {
		if it.Name == idxname {
			return nil, it, true
		}
	}

	tt.ver += 1
	it = tt.makeIndexType(idxname, key, unique)
	tt.indexes = append(tt.indexes, it)
	return tt, it, false
}

func (tt *TableType) RemoveIndex(idxname sql.Identifier) (*TableType, int) {
	var rdx int
	indexes := make([]sql.IndexType, 0, len(tt.indexes))
	for idx, it := range tt.indexes {
		if it.Name != idxname {
			indexes = append(indexes, it)
		} else {
			rdx = idx
		}
	}

	if len(indexes) == len(tt.indexes) {
		return nil, -1
	}

	tt.ver += 1
	tt.indexes = indexes
	return tt, rdx
}

func encodeColumnKey(key []sql.ColumnKey) []*ColumnKey {
	mdk := make([]*ColumnKey, 0, len(key))
	for _, k := range key {
		mdk = append(mdk,
			&ColumnKey{
				Number:  int32(k.Column()),
				Reverse: k.Reverse(),
			})
	}
	return mdk
}

func encodeForeignKey(fk foreignKey) *ForeignKey {
	keyCols := make([]int32, 0, len(fk.keyCols))
	for _, col := range fk.keyCols {
		keyCols = append(keyCols, int32(col))
	}
	return &ForeignKey{
		Name:       fk.name.String(),
		KeyColumns: keyCols,
		ReferenceTable: &TableName{
			Database: fk.refTable.Database.String(),
			Schema:   fk.refTable.Schema.String(),
			Table:    fk.refTable.Table.String(),
		},
		ReferenceIndex: fk.refIndex.String(),
	}
}

func (tt *TableType) Encode() ([]byte, error) {
	cols := tt.Columns()
	colTypes := tt.ColumnTypes()

	var md TableTypeMetadata
	md.Version = tt.ver
	md.Columns = make([]*ColumnMetadata, 0, len(cols))
	for cdx := range cols {
		md.Columns = append(md.Columns,
			&ColumnMetadata{
				Name:        cols[cdx].String(),
				Type:        DataType(colTypes[cdx].Type),
				Size:        colTypes[cdx].Size,
				Fixed:       colTypes[cdx].Fixed,
				NotNull:     colTypes[cdx].NotNull,
				Default:     expr.Encode(colTypes[cdx].Default),
				DefaultExpr: colTypes[cdx].DefaultExpr,
			})
	}

	md.Primary = encodeColumnKey(tt.primary)

	md.Indexes = make([]*IndexMetadata, 0, len(tt.indexes))
	for _, it := range tt.indexes {
		md.Indexes = append(md.Indexes,
			&IndexMetadata{
				Name:   it.Name.String(),
				Key:    encodeColumnKey(it.Key),
				Unique: it.Unique,
			})
	}

	md.Constraints = make([]*ConstraintMetadata, 0, len(tt.constraints))
	for _, con := range tt.constraints {
		md.Constraints = append(md.Constraints,
			&ConstraintMetadata{
				Name:   con.name.String(),
				Type:   ConstraintType(con.typ),
				ColNum: int32(con.colNum),
			})
	}

	md.Checks = make([]*CheckConstraint, 0, len(tt.checks))
	for _, chk := range tt.checks {
		md.Checks = append(md.Checks,
			&CheckConstraint{
				Name:      chk.name.String(),
				Check:     expr.Encode(chk.check),
				CheckExpr: chk.checkExpr,
			})
	}

	md.ForeignKeys = make([]*ForeignKey, 0, len(tt.foreignKeys))
	for _, fk := range tt.foreignKeys {
		md.ForeignKeys = append(md.ForeignKeys, encodeForeignKey(fk))
	}

	md.Triggers = make([]*TriggerMetadata, 0, len(tt.triggers))
	for _, trig := range tt.triggers {
		buf, err := trig.trig.Encode()
		if err != nil {
			return nil, err
		}
		md.Triggers = append(md.Triggers,
			&TriggerMetadata{
				Type:    trig.typ,
				Events:  trig.events,
				Trigger: buf,
			})
	}

	return proto.Marshal(&md)
}

func decodeColumnKey(mdk []*ColumnKey) []sql.ColumnKey {
	if len(mdk) == 0 {
		return nil
	}

	key := make([]sql.ColumnKey, 0, len(mdk))
	for _, k := range mdk {
		key = append(key, sql.MakeColumnKey(int(k.Number), k.Reverse))
	}
	return key
}

func decodeForeignKey(fk *ForeignKey) foreignKey {
	keyCols := make([]int, 0, len(fk.KeyColumns))
	for _, col := range fk.KeyColumns {
		keyCols = append(keyCols, int(col))
	}
	return foreignKey{
		name:    sql.QuotedID(fk.Name),
		keyCols: keyCols,
		refTable: sql.TableName{
			Database: sql.QuotedID(fk.ReferenceTable.Database),
			Schema:   sql.QuotedID(fk.ReferenceTable.Schema),
			Table:    sql.QuotedID(fk.ReferenceTable.Table),
		},
		refIndex: sql.QuotedID(fk.ReferenceIndex),
	}
}

func DecodeTableType(tn sql.TableName, buf []byte) (*TableType, error) {
	var md TableTypeMetadata
	err := proto.Unmarshal(buf, &md)
	if err != nil {
		return nil, fmt.Errorf("engine: table %s: %s", tn, err)
	}

	cols := make([]sql.Identifier, 0, len(md.Columns))
	colTypes := make([]sql.ColumnType, 0, len(md.Columns))
	for cdx := range md.Columns {
		cols = append(cols, sql.QuotedID(md.Columns[cdx].Name))
		dflt, err := expr.Decode(md.Columns[cdx].Default)
		if err != nil {
			return nil, fmt.Errorf("engine: table %s: %s", tn, err)
		}
		colTypes = append(colTypes,
			sql.ColumnType{
				Type:        sql.DataType(md.Columns[cdx].Type),
				Size:        md.Columns[cdx].Size,
				Fixed:       md.Columns[cdx].Fixed,
				NotNull:     md.Columns[cdx].NotNull,
				Default:     dflt,
				DefaultExpr: md.Columns[cdx].DefaultExpr,
			})
	}

	primary := decodeColumnKey(md.Primary)

	indexes := make([]sql.IndexType, 0, len(md.Indexes))
	for _, it := range md.Indexes {
		indexes = append(indexes,
			sql.IndexType{
				Name:   sql.QuotedID(it.Name),
				Key:    decodeColumnKey(it.Key),
				Unique: it.Unique,
			})
	}

	constraints := make([]constraint, 0, len(md.Constraints))
	for _, con := range md.Constraints {
		constraints = append(constraints,
			constraint{
				name:   sql.QuotedID(con.Name),
				typ:    sql.ConstraintType(con.Type),
				colNum: int(con.ColNum),
			})
	}

	checks := make([]checkConstraint, 0, len(md.Checks))
	for _, chk := range md.Checks {
		check, err := expr.Decode(chk.Check)
		if err != nil {
			return nil, fmt.Errorf("engine: table %s: %s", tn, err)
		}
		checks = append(checks,
			checkConstraint{
				name:      sql.QuotedID(chk.Name),
				check:     check,
				checkExpr: chk.CheckExpr,
			})
	}

	foreignKeys := make([]foreignKey, 0, len(md.ForeignKeys))
	for _, fk := range md.ForeignKeys {
		foreignKeys = append(foreignKeys, decodeForeignKey(fk))
	}

	var events int64
	triggers := make([]trigger, 0, len(md.Triggers))
	for _, tmd := range md.Triggers {
		td, ok := TriggerDecoders[tmd.Type]
		if !ok {
			return nil, fmt.Errorf("engine: missing trigger decoder: %s", tmd.Type)
		}
		trig, err := td(tmd.Trigger)
		if err != nil {
			return nil, err
		}
		triggers = append(triggers,
			trigger{
				typ:    tmd.Type,
				events: tmd.Events,
				trig:   trig,
			})
		events |= tmd.Events
	}

	return &TableType{
		ver:         md.Version,
		cols:        cols,
		colTypes:    colTypes,
		primary:     primary,
		indexes:     indexes,
		constraints: constraints,
		checks:      checks,
		foreignKeys: foreignKeys,
		triggers:    triggers,
		events:      events,
	}, nil
}
