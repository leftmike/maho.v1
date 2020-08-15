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
	s += fmt.Sprintf(") REFERENCES %s", fk.IncomingTable)
	if len(fk.IncomingCols) > 0 {
		s += " ("
		for i, c := range fk.IncomingCols {
			if i > 0 {
				s += ", "
			}
			s += c.String()
		}
		s += ")"
	}
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

func (fk *ForeignKey) Prepare(ott, itt sql.TableType, ofkr *sql.OutgoingFKRef,
	ifkr *sql.IncomingFKRef) error {

	var inCols []int
	var outCols []int
	var index sql.Identifier
	if len(fk.IncomingCols) == 0 {
		pk := itt.PrimaryKey()
		if len(pk) != len(fk.OutgoingCols) {
			return fmt.Errorf(
				"engine: table %s: foreign key constraint %s: different column counts",
				fk.OutgoingTable, fk.Name)
		}

		for _, ck := range pk {
			inCols = append(inCols, ck.Column())
		}

		for _, col := range fk.OutgoingCols {
			num, ok := columnNumber(col, ott.Columns())
			if !ok {
				return fmt.Errorf("engine: table %s: foreign key column %s not found",
					fk.OutgoingTable, col)
			}
			outCols = append(outCols, num)
		}
	} else {
		panic("not implemented yet")
		/*
			if len(fk.OutgoingCols) != len(fk.IncomingCols) {
				return -1, fmt.Errorf(
					"engine: table %s: foreign key constraint %s: different column counts",
					fk.OutgoingTable, fk.Name)
			}

			for _, it := range itt.Indexes() {
				if len(it.Key) != len(fk.IncomingCols) {
					continue
				}

			}
			_ = index
		*/
	}

	*ofkr = sql.OutgoingFKRef{
		Name:    fk.Name,
		Columns: outCols,
		Table:   fk.IncomingTable,
		Index:   index,
	}

	*ifkr = sql.IncomingFKRef{
		Name:         fk.Name,
		OutgoingCols: outCols,
		Table:        fk.OutgoingTable,
		IncomingCols: inCols,
	}

	return nil
}

func (fk *ForeignKey) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	_, ott, err := e.LookupTable(ctx, tx, fk.OutgoingTable)
	if err != nil {
		return -1, err
	}
	_, itt, err := e.LookupTable(ctx, tx, fk.IncomingTable)
	if err != nil {
		return -1, err
	}

	var ofkr sql.OutgoingFKRef
	var ifkr sql.IncomingFKRef
	err = fk.Prepare(ott, itt, &ofkr, &ifkr)
	if err != nil {
		return -1, err
	}

	err = e.AddOutgoingFKRef(ctx, tx, fk.OutgoingTable, ofkr)
	if err != nil {
		return -1, err
	}

	err = e.AddIncomingFKRef(ctx, tx, fk.IncomingTable, ifkr)
	if err != nil {
		return -1, err
	}

	return -1, nil
}
