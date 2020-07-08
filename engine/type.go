package engine

//go:generate protoc --go_opt=paths=source_relative --go_out=. metadata.proto

import (
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

type TableType struct {
	ver         uint32
	cols        []sql.Identifier
	colTypes    []sql.ColumnType
	primary     []sql.ColumnKey
	constraints []constraint
	checks      []checkConstraint
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

func (tt *TableType) Columns() []sql.Identifier {
	return tt.cols
}

func (tt *TableType) ColumnTypes() []sql.ColumnType {
	return tt.colTypes
}

func (tt *TableType) PrimaryKey() []sql.ColumnKey {
	return tt.primary
}

func (tt *TableType) Version() uint32 {
	return tt.ver
}

func (tt *TableType) Encode() ([]byte, error) {
	cols := tt.Columns()
	colTypes := tt.ColumnTypes()

	var md TableMetadata
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

	primary := tt.PrimaryKey()
	md.Primary = make([]*ColumnKey, 0, len(primary))
	for _, pk := range primary {
		md.Primary = append(md.Primary,
			&ColumnKey{
				Number:  int32(pk.Number()),
				Reverse: pk.Reverse(),
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

	return proto.Marshal(&md)
}

func DecodeTableType(tn sql.TableName, buf []byte) (*TableType, error) {
	var md TableMetadata
	err := proto.Unmarshal(buf, &md)
	if err != nil {
		return nil, err
	}

	cols := make([]sql.Identifier, 0, len(md.Columns))
	colTypes := make([]sql.ColumnType, 0, len(md.Columns))
	for cdx := range md.Columns {
		cols = append(cols, sql.QuotedID(md.Columns[cdx].Name))
		dflt, err := expr.Decode(md.Columns[cdx].Default)
		if err != nil {
			return nil, err
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

	primary := make([]sql.ColumnKey, 0, len(md.Primary))
	for _, pk := range md.Primary {
		primary = append(primary, sql.MakeColumnKey(int(pk.Number), pk.Reverse))
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
			return nil, err
		}
		checks = append(checks,
			checkConstraint{
				name:      sql.QuotedID(chk.Name),
				check:     check,
				checkExpr: chk.CheckExpr,
			})
	}

	return &TableType{
		ver:         md.Version,
		cols:        cols,
		colTypes:    colTypes,
		primary:     primary,
		constraints: constraints,
		checks:      checks,
	}, nil
}
