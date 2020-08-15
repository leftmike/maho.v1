package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type ForeignKey struct {
	Name          sql.Identifier
	OutgoingTable sql.TableName
	OutgoingCols  []sql.Identifier
	IncomingTable sql.TableName
	IncomingCols  []sql.Identifier
}

func (fk ForeignKey) String() string {
	s := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (", fk.Name)
	for i, c := range fk.OutgoingCols {
		if i > 0 {
			s += ", "
		}
		s += c.String()
	}
	s += fmt.Sprintf(") REFERENCES %s (", fk.IncomingTable)
	for i, c := range fk.IncomingCols {
		if i > 0 {
			s += ", "
		}
		s += c.String()
	}
	s += ")"
	return s
}

func (fk ForeignKey) Plan(ses *evaluate.Session, tx sql.Transaction) (interface{}, error) {

	fk.OutgoingTable = ses.ResolveTableName(fk.OutgoingTable)
	fk.IncomingTable = ses.ResolveTableName(fk.IncomingTable)

	if fk.OutgoingTable.Database != fk.IncomingTable.Database {
		return nil, fmt.Errorf(
			"engine: table %s: foreign key reference not within same database: %s",
			fk.OutgoingTable, fk.IncomingTable)
	}

	return &fk, nil
}

/*
func (stmt *CreateTable) prepareForeignConstraint(ctx context.Context, e sql.Engine,
	tx sql.Transaction, fc foreignConstraint) error {

	fk := fc.foreignKey
	_, tt, err := e.LookupTable(ctx, tx, fk.Table)
	if err != nil {
		return err
	}

	var refCols []int
	if len(fk.IncomingCols) == 0 {
		for _, ck := range tt.PrimaryKey() {
			refCols = append(refCols, ck.Column())
		}
	} else {
		refColumns := tt.Columns()
		for _, col := range fk.IncomingCols {
			num, ok := columnNumber(col, refColumns)
			if !ok {
				return fmt.Errorf("engine: table %s: reference column %s to table %s not found",
					stmt.Table, col, fk.Table)
			}
			refCols = append(refCols, num)
		}
	}

	if len(refCols) != len(fk.OutgoingCols) {
		return fmt.Errorf("engine: table %s: foreign constraint %s: different column counts",
			stmt.Table, fc.name)
	}

	var keyCols []int
	refColTypes := tt.ColumnTypes()
	for idx, col := range fk.OutgoingCols {
		num, ok := columnNumber(col, stmt.Columns)
		if !ok {
			return fmt.Errorf("engine: table %s: foreign key column %s not found", stmt.Table, col)
		}
		if stmt.columnTypes[num].Type != refColTypes[refCols[idx]].Type {
			return fmt.Errorf(
				"engine: table %s: foreign key column %s and reference column %s type mismatch",
				stmt.Table, col, tt.Columns()[refCols[idx]])
		}
		keyCols = append(keyCols, num)
	}

		stmt.constraints = append(stmt.constraints,
			sql.Constraint{
				Type: sql.ForeignConstraint,
				Name: fc.name,
				ForeignKey: sql.ForeignKey{
					OutgoingCols: keyCols,
					Table:       fk.Table,
					IncomingCols:   refCols,
				},
			})
	return nil
}
*/

func (fk *ForeignKey) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	return -1, nil
}
