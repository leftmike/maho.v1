package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

type transaction struct {
	e          *Engine
	tx         Transaction
	tables     map[sql.TableName]*table
	tableTypes map[sql.TableName]sql.TableType
	modified   []*table
}

func (e *Engine) Begin(sesid uint64) sql.Transaction {
	return &transaction{
		e:          e,
		tx:         e.st.Begin(sesid),
		tables:     map[sql.TableName]*table{},
		tableTypes: map[sql.TableName]sql.TableType{},
	}
}

func (tx *transaction) Commit(ctx context.Context) error {
	err := tx.stmtTriggers(ctx)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.tx.Commit(ctx)
	tx.tx = nil
	return err
}

func (tx *transaction) Rollback() error {
	err := tx.tx.Rollback()
	tx.tx = nil
	return err
}

func (tx *transaction) NextStmt(ctx context.Context) error {
	err := tx.stmtTriggers(ctx)
	if err != nil {
		return err
	}

	tx.tx.NextStmt()
	return nil
}

func (tx *transaction) CreateSchema(ctx context.Context, sn sql.SchemaName) error {
	if sn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", sn.Database)
	}
	if sn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s already exists", sn)
	}
	return tx.e.st.CreateSchema(ctx, tx.tx, sn)
}

func (tx *transaction) DropSchema(ctx context.Context, sn sql.SchemaName, ifExists bool) error {
	if sn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", sn.Database)
	}
	if sn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be dropped", sn)
	}
	return tx.e.st.DropSchema(ctx, tx.tx, sn, ifExists)
}

func (tx *transaction) LookupTableType(ctx context.Context, tn sql.TableName) (sql.TableType,
	error) {

	_, tt, err, ok := tx.e.lookupVirtualTable(ctx, tx, tn)
	if ok {
		return tt, err
	}

	tt, ok = tx.tableTypes[tn]
	if ok {
		return tt, nil
	}

	tt, err = tx.e.st.LookupTableType(ctx, tx.tx, tn)
	if err != nil {
		return nil, err
	}
	tx.tableTypes[tn] = tt
	return tt, nil
}

func (tx *transaction) LookupTable(ctx context.Context, tn sql.TableName, ttVer int64) (sql.Table,
	error) {

	vtbl, vtt, err, ok := tx.e.lookupVirtualTable(ctx, tx, tn)
	if ok {
		if vtt.Version() != ttVer {
			return nil, fmt.Errorf("engine: table %s: type version mismatch", tn)
		}
		return vtbl, err
	}

	tbl, ok := tx.tables[tn]
	if ok {
		if tbl.tt.Version() != ttVer {
			return nil, fmt.Errorf("engine: table %s: type version mismatch", tn)
		}
		return tbl, nil
	}

	stbl, tt, err := tx.e.st.LookupTable(ctx, tx.tx, tn)
	if err != nil {
		return nil, err
	}
	if tt.Version() != ttVer {
		return nil, fmt.Errorf("engine: table %s: type version mismatch", tn)
	}

	tbl = makeTable(tx, tn, stbl, tt)
	tx.tables[tn] = tbl
	tx.tableTypes[tn] = tt
	return tbl, nil
}

func (tx *transaction) CreateTable(ctx context.Context, tn sql.TableName, cols []sql.Identifier,
	colTypes []sql.ColumnType, colDefaults []sql.ColumnDefault, cons []sql.Constraint,
	ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}

	var primary []sql.ColumnKey
	for _, con := range cons {
		if con.Type == sql.PrimaryConstraint {
			primary = con.Key
			break
		} else if con.Name == sql.PRIMARY_QUOTED {
			return errors.New("engine: primary not allowed as constraint name")
		}
	}

	if len(primary) == 0 {
		rowID := sql.ID("rowid")

		for _, col := range cols {
			if col == rowID {
				return fmt.Errorf(
					"engine: unable to add %s column for table %s missing primary key", rowID, tn)
			}
		}

		primary = []sql.ColumnKey{
			sql.MakeColumnKey(len(cols), false),
		}
		cols = append(cols, rowID)

		dflt, ct, err := expr.Compile(ctx, nil, tx, nil, &expr.Call{Name: sql.ID("unique_rowid")})
		if err != nil {
			panic(fmt.Sprintf("unable to compile default for rowid: %s", err))
		}
		if ct.Type != sql.IntegerType {
			panic(fmt.Sprintf("default for rowid should be integer expression; got: %s", ct.Type))
		}
		colTypes = append(colTypes, sql.ColumnType{
			Type:    sql.IntegerType,
			Size:    8,
			NotNull: true,
		})
		colDefaults = append(colDefaults, sql.ColumnDefault{Default: dflt})
	}

	tt := MakeTableType(cols, colTypes, colDefaults, primary)
	for _, con := range cons {
		if con.Type == sql.CheckConstraint {
			cn, err := tt.constraintName(tn, con.Name, "check_")
			if err != nil {
				return err
			}

			tt.checks = append(tt.checks,
				checkConstraint{
					name:      cn,
					check:     con.Check,
					checkExpr: con.CheckExpr,
				})
		} else if con.Type == sql.UniqueConstraint {
			tt.indexes = append(tt.indexes, tt.makeIndexType(con.Name, con.Key, true, false))
		} else if con.Type == sql.PrimaryConstraint {
			// Used above; remove from constraints.
		} else {
			// sql.DefaultConstraint
			// sql.NotNullConstraint

			tt.constraints = append(tt.constraints,
				constraint{
					name:   con.Name,
					typ:    con.Type,
					colNum: con.ColNum,
				})
		}
	}

	for _, ck := range primary {
		col := ck.Column()
		if !colTypes[col].NotNull {
			colTypes[col].NotNull = true
			tt.constraints = append(tt.constraints,
				constraint{
					typ:    sql.NotNullConstraint,
					colNum: col,
				})
		}
	}

	return tx.e.st.CreateTable(ctx, tx.tx, tn, tt, ifNotExists)
}

func (tx *transaction) dropForeignRef(ctx context.Context, con sql.Identifier,
	fktn, rtn sql.TableName) error {

	rtt, err := tx.e.st.LookupTableType(ctx, tx.tx, rtn)
	if err != nil {
		return err
	}

	rtt = rtt.dropForeignRef(con, fktn)
	err = tx.e.st.UpdateType(ctx, tx.tx, rtn, rtt)
	if err != nil {
		return err
	}
	delete(tx.tables, rtn)
	delete(tx.tableTypes, rtn)

	tx.tx.NextStmt()
	return nil
}

func (tx *transaction) dropForeignKey(ctx context.Context, con sql.Identifier,
	fktn, rtn sql.TableName) error {

	fktt, err := tx.e.st.LookupTableType(ctx, tx.tx, fktn)
	if err != nil {
		return err
	}

	fktt = fktt.dropForeignKey(con, rtn)
	err = tx.e.st.UpdateType(ctx, tx.tx, fktn, fktt)
	if err != nil {
		return err
	}
	delete(tx.tables, fktn)
	delete(tx.tableTypes, fktn)

	tx.tx.NextStmt()
	return nil
}

func (tx *transaction) DropTable(ctx context.Context, tn sql.TableName, ifExists,
	cascade bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}

	tt, err := tx.e.st.LookupTableType(ctx, tx.tx, tn)
	if err != nil {
		if ifExists {
			return nil
		}
		return err
	}

	for _, fk := range tt.foreignKeys {
		err = tx.dropForeignRef(ctx, fk.name, tn, fk.refTable)
		if err != nil {
			return err
		}
	}

	if len(tt.foreignRefs) > 0 {
		if !cascade {
			return fmt.Errorf("engine: table %s: existing foreign references", tn)
		}

		for _, fr := range tt.foreignRefs {
			err = tx.dropForeignKey(ctx, fr.name, fr.tn, tn)
			if err != nil {
				return err
			}
		}
	}

	err = tx.e.st.DropTable(ctx, tx.tx, tn)
	if err != nil {
		return err
	}
	delete(tx.tables, tn)
	delete(tx.tableTypes, tn)
	return nil
}

func (tx *transaction) AddForeignKey(ctx context.Context, con sql.Identifier, fktn sql.TableName,
	fkCols []int, rtn sql.TableName, ridx sql.Identifier, onDel, onUpd sql.RefAction,
	check bool) error {

	if fktn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", fktn.Database)
	}
	if fktn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", fktn.Schema)
	}

	fktt, err := tx.e.st.LookupTableType(ctx, tx.tx, fktn)
	if err != nil {
		return err
	}

	if rtn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", rtn.Database)
	}
	if rtn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", rtn.Schema)
	}

	rtt, err := tx.e.st.LookupTableType(ctx, tx.tx, rtn)
	if err != nil {
		return err
	}

	err = addForeignKey(con, fktn, fkCols, fktt, rtn, ridx, rtt, onDel, onUpd)
	if err != nil {
		return err
	}

	err = tx.e.st.UpdateType(ctx, tx.tx, fktn, fktt)
	if err != nil {
		return err
	}

	if fktt != rtt {
		err = tx.e.st.UpdateType(ctx, tx.tx, rtn, rtt)
		if err != nil {
			return err
		}

		delete(tx.tables, rtn)
		delete(tx.tableTypes, rtn)
	}

	delete(tx.tables, fktn)
	delete(tx.tableTypes, fktn)

	if !check {
		return nil
	}

	sqlStmt := generateCheckSQL(fktn, fkCols, fktt, rtn, ridx, rtt)

	p := parser.NewParser(strings.NewReader(sqlStmt), sqlStmt)
	stmt, err := p.Parse()
	if err != nil {
		panic(fmt.Sprintf("engine: table %s: check foreign key: %s", fktn, err))
	}
	plan, err := stmt.Plan(ctx, planContext{tx.e}, tx, nil)
	if err != nil {
		panic(fmt.Sprintf("engine: table %s: check foreign key: %s", fktn, err))
	}
	rowsPlan := plan.(evaluate.RowsPlan)
	rows, err := rowsPlan.Rows(ctx, tx, nil)

	cntRow := []sql.Value{nil}
	err = rows.Next(ctx, cntRow)
	if err != nil {
		panic(fmt.Sprintf("engine: table %s: check foreign key match: %s", fktn, err))
	}
	rows.Close()
	cnt := cntRow[0].(sql.Int64Value)
	if cnt > 0 {
		return fmt.Errorf("engine: table %s: check foreign key failed", fktn)
	}

	return nil
}

func (tx *transaction) AddTrigger(ctx context.Context, tn sql.TableName, events int64,
	trig sql.Trigger) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}

	tt, err := tx.e.st.LookupTableType(ctx, tx.tx, tn)
	if err != nil {
		return err
	}

	tt.addTrigger(events, trig)
	tt.ver += 1
	err = tx.e.st.UpdateType(ctx, tx.tx, tn, tt)
	if err != nil {
		return err
	}
	delete(tx.tables, tn)
	delete(tx.tableTypes, tn)
	return nil
}

func (tx *transaction) DropConstraint(ctx context.Context, tn sql.TableName, con sql.Identifier,
	ifExists bool, col sql.Identifier, ct sql.ConstraintType) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}

	tt, err := tx.e.st.LookupTableType(ctx, tx.tx, tn)
	if err != nil {
		return err
	}

	if con != 0 {
		for _, fk := range tt.foreignKeys {
			if fk.name == con {
				err = tx.dropForeignRef(ctx, con, tn, fk.refTable)
				if err != nil {
					return err
				}

				err = tx.dropForeignKey(ctx, con, tn, fk.refTable)
				if err != nil {
					return err
				}

				return nil
			}
		}
	}

	found, err := tt.dropConstraint(tn, con, col, ct)
	if err != nil {
		if !found && ifExists && con != 0 {
			return nil
		}
		return err
	}

	err = tx.e.st.UpdateType(ctx, tx.tx, tn, tt)
	if err != nil {
		return err
	}

	delete(tx.tables, tn)
	delete(tx.tableTypes, tn)

	return nil
}

func (tx *transaction) CreateIndex(ctx context.Context, idxname sql.Identifier, tn sql.TableName,
	unique bool, key []sql.ColumnKey, ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}
	if idxname == sql.PRIMARY_QUOTED {
		return errors.New("engine: primary not allowed as index name")
	}

	tt, err := tx.e.st.LookupTableType(ctx, tx.tx, tn)
	if err != nil {
		return err
	}

	tt, it, iidx, found := tt.AddIndex(idxname, unique, key)
	if found {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("engine: table %s: index %s already exists", tn, idxname)
	}
	err = tx.e.st.AddIndex(ctx, tx.tx, tn, tt, it)
	if err != nil {
		return err
	}

	tx.tx.NextStmt()
	err = tx.e.st.FillIndex(ctx, tx.tx, tn, tt, iidx)
	if err != nil {
		return err
	}

	tx.tx.NextStmt()
	tt = tt.ShowIndex(idxname)
	err = tx.e.st.UpdateType(ctx, tx.tx, tn, tt)
	if err != nil {
		return err
	}

	delete(tx.tables, tn)
	delete(tx.tableTypes, tn)
	return nil
}

func (tx *transaction) DropIndex(ctx context.Context, idxname sql.Identifier, tn sql.TableName,
	ifExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}

	tt, err := tx.e.st.LookupTableType(ctx, tx.tx, tn)
	if err != nil {
		return err
	}

	tt, iidx := tt.RemoveIndex(idxname)
	if tt == nil {
		if ifExists {
			return nil
		}
		return fmt.Errorf("engine: table %s: index %s not found", tn, idxname)
	}

	err = tx.e.st.RemoveIndex(ctx, tx.tx, tn, tt, iidx)
	if err != nil {
		return err
	}
	delete(tx.tables, tn)
	delete(tx.tableTypes, tn)
	return nil
}
