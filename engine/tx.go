package engine

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type action interface {
	execute(ctx context.Context, e *Engine, tx *transaction) (int64, error)
}

type actionKey struct {
	tn   sql.TableName
	name sql.Identifier
}

type transaction struct {
	e       *Engine
	tx      Transaction
	actions map[actionKey]action
}

func (e *Engine) Begin(sesid uint64) sql.Transaction {
	return &transaction{
		e:  e,
		tx: e.st.Begin(sesid),
	}
}

func (tx *transaction) Commit(ctx context.Context) error {
	err := tx.executeActions(ctx)
	if err != nil {
		tx.tx.Rollback()
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
	err := tx.executeActions(ctx)
	if err != nil {
		tx.tx.Rollback()
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

	tt, err = tx.e.st.LookupTableType(ctx, tx.tx, tn)
	if err != nil {
		return nil, err
	}
	return tt, err
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

	stbl, stt, err := tx.e.st.LookupTable(ctx, tx.tx, tn)
	if err != nil {
		return nil, err
	}
	if stt.Version() != ttVer {
		return nil, fmt.Errorf("engine: table %s: type version mismatch", tn)
	}
	return makeTable(tx, tn, stbl, stt), nil
}

func (tx *transaction) CreateTable(ctx context.Context, tn sql.TableName, cols []sql.Identifier,
	colTypes []sql.ColumnType, cons []sql.Constraint, ifNotExists bool) error {

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

		dflt, err := expr.CompileExpr(&expr.Call{Name: sql.ID("unique_rowid")})
		if err != nil {
			panic(fmt.Sprintf("unable to compile default for rowid: %s", err))
		}
		colTypes = append(colTypes, sql.ColumnType{
			Type:    sql.IntegerType,
			Size:    8,
			NotNull: true,
			Default: dflt,
		})
	}

	tt := MakeTableType(cols, colTypes, primary)
	for _, con := range cons {
		if con.Type == sql.CheckConstraint {
			tt.checks = append(tt.checks,
				checkConstraint{
					name:      con.Name,
					check:     con.Check,
					checkExpr: con.CheckExpr,
				})
		} else if con.Type == sql.UniqueConstraint {
			it := tx.e.st.MakeIndexType(tt, con.Name, con.Key, true)
			tt.indexes = append(tt.indexes, it)
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

func (tx *transaction) DropTable(ctx context.Context, tn sql.TableName, ifExists bool) error {
	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}
	return tx.e.st.DropTable(ctx, tx.tx, tn, ifExists)
}

func (tx *transaction) AddForeignKey(ctx context.Context, con sql.Identifier, fktn sql.TableName,
	fkCols []int, rtn sql.TableName, ridx sql.Identifier) error {

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

	fktt.foreignKeys = append(fktt.foreignKeys,
		foreignKey{
			name:     con,
			keyCols:  fkCols,
			refTable: rtn,
			refIndex: ridx,
		})
	fktt.ver += 1
	err = tx.e.st.UpdateType(ctx, tx.tx, fktn, fktt)
	if err != nil {
		return err
	}

	if rtn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", rtn.Database)
	}
	if rtn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", rtn.Schema)
	}

	/*
		rtt, err := tx.e.st.LookupTableType(ctx, tx.tx, rtn)
		if err != nil {
			return err
		}
	*/
	// XXX

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

	tt, err := tx.e.st.LookupTableType(ctx, tx.tx, tn)
	if err != nil {
		return err
	}

	for _, it := range tt.indexes {
		if it.Name == idxname {
			if ifNotExists {
				return nil
			}
			return fmt.Errorf("engine: table %s: index %s already exists", tn, idxname)
		}
	}

	tt.ver += 1
	it := tx.e.st.MakeIndexType(tt, idxname, key, unique)
	tt.indexes = append(tt.indexes, it)
	return tx.e.st.AddIndex(ctx, tx.tx, tn, tt, it)
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
		if ifExists {
			return nil
		}
		return fmt.Errorf("engine: table %s: index %s not found", tn, idxname)
	}

	tt.ver += 1
	tt.indexes = indexes

	return tx.e.st.RemoveIndex(ctx, tx.tx, tn, tt, rdx)
}

func (tx *transaction) ListDatabases(ctx context.Context) ([]sql.Identifier, error) {
	return tx.e.st.ListDatabases(ctx, tx.tx)
}

func (tx *transaction) addAction(tn sql.TableName, nam sql.Identifier, newAct func() action,
	addAct func(act action)) {

	actKey := actionKey{tn, nam}
	act, ok := tx.actions[actKey]
	if !ok {
		act = newAct()
		if tx.actions == nil {
			tx.actions = map[actionKey]action{}
		}
		tx.actions[actKey] = act
	}

	addAct(act)
}

func (tx *transaction) executeActions(ctx context.Context) error {
	for len(tx.actions) > 0 {
		actions := tx.actions
		tx.actions = nil

		for _, act := range actions {
			_, err := act.execute(ctx, tx.e, tx)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
