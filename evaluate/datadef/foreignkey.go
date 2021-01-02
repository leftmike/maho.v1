package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type ForeignKey struct {
	Name     sql.Identifier
	FKTable  sql.TableName
	FKCols   []sql.Identifier
	RefTable sql.TableName
	RefCols  []sql.Identifier
	OnDelete sql.RefAction
	OnUpdate sql.RefAction
}

func (fk ForeignKey) String() string {
	s := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (", fk.Name)
	for i, c := range fk.FKCols {
		if i > 0 {
			s += ", "
		}
		s += c.String()
	}
	s += fmt.Sprintf(") REFERENCES %s", fk.RefTable)
	if len(fk.RefCols) > 0 {
		s += " ("
		for i, c := range fk.RefCols {
			if i > 0 {
				s += ", "
			}
			s += c.String()
		}
		s += ")"
	}
	return s
}

func (fk *ForeignKey) plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) error {

	fk.RefTable = pctx.ResolveTableName(fk.RefTable)
	if fk.FKTable.Database != fk.RefTable.Database {
		return fmt.Errorf("engine: table %s: foreign key reference not within same database: %s",
			fk.FKTable, fk.RefTable)
	}

	return nil
}

func hasColumn(id sql.Identifier, cols []sql.Identifier) bool {
	for _, col := range cols {
		if id == col {
			return true
		}
	}
	return false
}

func matchIndexKey(cols []sql.Identifier, key []sql.ColumnKey, refCols []sql.Identifier) bool {
	for _, ck := range key {
		if !hasColumn(cols[ck.Column()], refCols) {
			return false
		}
	}
	return true
}

func findIndex(rtt sql.TableType, refCols []sql.Identifier) (sql.IndexType, bool) {
	for _, it := range rtt.Indexes() {
		if it.Hidden || !it.Unique || len(it.Key) != len(refCols) {
			continue
		}

		if matchIndexKey(rtt.Columns(), it.Key, refCols) {
			return it, true
		}
	}

	return sql.IndexType{}, false
}

func orderFKCols(fkCols []int, rkey []sql.ColumnKey, refColIDs, refCols []sql.Identifier) []int {
	nfkCols := make([]int, len(fkCols))
	for rdx, ck := range rkey {
		rcol := refColIDs[ck.Column()]
		idx, ok := columnNumber(rcol, refCols)
		if !ok {
			panic("engine: internal error: missing reference column in key")
		}
		nfkCols[rdx] = fkCols[idx]
	}

	return nfkCols
}

func (fk *ForeignKey) Prepare(fktt, rtt sql.TableType) ([]int, sql.Identifier, error) {
	ridx := sql.PRIMARY_QUOTED
	var rkey []sql.ColumnKey
	if len(fk.RefCols) == 0 {
		rkey = rtt.PrimaryKey()
	} else if matchIndexKey(rtt.Columns(), rtt.PrimaryKey(), fk.RefCols) {
		rkey = rtt.PrimaryKey()
	} else {
		it, ok := findIndex(rtt, fk.RefCols)
		if !ok {
			return nil, 0,
				fmt.Errorf("engine: table %s: no unique index for foreign key reference to %s",
					fk.FKTable, fk.RefTable)
		}

		ridx = it.Name
		rkey = it.Key
	}

	if len(rkey) != len(fk.FKCols) {
		return nil, 0,
			fmt.Errorf("engine: table %s: foreign key constraint %s: different column counts",
				fk.FKTable, fk.Name)
	}

	var fkCols []int
	fkColIDs := fktt.Columns()
	for _, col := range fk.FKCols {
		num, ok := columnNumber(col, fkColIDs)
		if !ok {
			return nil, 0,
				fmt.Errorf("engine: table %s: foreign key column %s not found", fk.FKTable,
					col)
		}
		fkCols = append(fkCols, num)
	}

	if len(fk.RefCols) > 1 {
		fkCols = orderFKCols(fkCols, rkey, rtt.Columns(), fk.RefCols)
	}

	fkColTypes := fktt.ColumnTypes()
	refColTypes := rtt.ColumnTypes()
	for cdx, num := range fkCols {
		fkct := fkColTypes[num]
		rct := refColTypes[rkey[cdx].Column()]
		if fkct.Type != rct.Type {
			return nil, 0,
				fmt.Errorf("engine: table %s: foreign key column type mismatch: %s and %s",
					fk.FKTable, fktt.Columns()[num], rtt.Columns()[rkey[cdx].Column()])
		}
	}

	return fkCols, ridx, nil
}

func (fk *ForeignKey) execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	fktt, err := tx.LookupTableType(ctx, fk.FKTable)
	if err != nil {
		return -1, err
	}
	rtt, err := tx.LookupTableType(ctx, fk.RefTable)
	if err != nil {
		return -1, err
	}

	fkCols, ridx, err := fk.Prepare(fktt, rtt)
	if err != nil {
		return -1, err
	}

	return -1, tx.AddForeignKey(ctx, fk.Name, fk.FKTable, fkCols, fk.RefTable, ridx,
		fk.OnDelete, fk.OnUpdate)
}
