package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type IndexKey struct {
	Unique  bool
	Columns []sql.Identifier
	Reverse []bool // ASC = false, DESC = true
}

func (ik IndexKey) String() string {
	s := "("
	for i := range ik.Columns {
		if i > 0 {
			s += ", "
		}
		if ik.Reverse[i] {
			s += fmt.Sprintf("%s DESC", ik.Columns[i])
		} else {
			s += fmt.Sprintf("%s ASC", ik.Columns[i])
		}
	}
	s += ")"
	return s
}

func (ik IndexKey) Equal(oik IndexKey) bool {
	if len(ik.Columns) != len(oik.Columns) {
		return false
	}

	for cdx := range ik.Columns {
		if ik.Columns[cdx] != oik.Columns[cdx] || ik.Reverse[cdx] != oik.Reverse[cdx] {
			return false
		}
	}
	return true
}

func columnNumber(nam sql.Identifier, columns []sql.Identifier) (int, bool) {
	for num, col := range columns {
		if nam == col {
			return num, true
		}
	}
	return -1, false
}

func indexKeyToColumnKeys(ik IndexKey, columns []sql.Identifier) ([]sql.ColumnKey, error) {
	var colKeys []sql.ColumnKey

	for cdx, col := range ik.Columns {
		num, ok := columnNumber(col, columns)
		if !ok {
			return nil, fmt.Errorf("unknown column %s", col)
		}
		colKeys = append(colKeys, sql.MakeColumnKey(num, ik.Reverse[cdx]))
	}

	return colKeys, nil
}

type Constraint struct {
	Type   sql.ConstraintType
	Name   sql.Identifier
	ColNum int
	Key    IndexKey
	Check  expr.Expr
}

func (c Constraint) String() string {
	switch c.Type {
	case sql.DefaultConstraint:
	case sql.NotNullConstraint:
	case sql.PrimaryConstraint:
		return fmt.Sprintf(", CONSTRAINT %s PRIMARY KEY %s", c.Name, c.Key)
	case sql.UniqueConstraint:
		return fmt.Sprintf(", CONSTRAINT %s UNIQUE %s", c.Name, c.Key)
	case sql.CheckConstraint:
		return fmt.Sprintf(", CONSTRAINT %s CHECK (%s)", c.Name, c.Check)
	default:
		panic(fmt.Sprintf("unexpected constraint type: %d", c.Type))
	}

	return ""
}

type ColumnType struct {
	Type    sql.DataType
	Size    uint32
	Fixed   bool
	NotNull bool
	Default expr.Expr
}

type CreateTable struct {
	Table       sql.TableName
	Columns     []sql.Identifier
	ColumnTypes []ColumnType
	columnTypes []sql.ColumnType
	IfNotExists bool
	Constraints []Constraint
	constraints []sql.Constraint
	ForeignKeys []*ForeignKey
	foreignKeys []evaluate.StmtPlan
}

func (stmt *CreateTable) String() string {
	s := "CREATE TABLE"
	if stmt.IfNotExists {
		s += " IF NOT EXISTS"
	}
	s = fmt.Sprintf("%s %s (", s, stmt.Table)

	for i, ct := range stmt.ColumnTypes {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s %s", stmt.Columns[i], sql.ColumnDataType(ct.Type, ct.Size, ct.Fixed))
		if ct.NotNull {
			s += " NOT NULL"
		}
		if ct.Default != nil {
			s += fmt.Sprintf(" DEFAULT %s", ct.Default)
		}
	}
	for _, c := range stmt.Constraints {
		s += c.String()
	}
	for _, fk := range stmt.ForeignKeys {
		s += ", "
		s += fk.String()
	}
	s += ")"
	return s
}

type tableCheck []sql.Identifier

func (tc tableCheck) CompileRef(r expr.Ref) (int, error) {
	if len(r) == 1 {
		for idx, col := range tc {
			if col == r[0] {
				return idx, nil
			}
		}
	}
	return -1, fmt.Errorf("engine: reference %s not found", r)
}

type columnCheck struct {
	col    sql.Identifier
	colNum int
}

func (cc columnCheck) CompileRef(r expr.Ref) (int, error) {
	if len(r) == 1 {
		if cc.col == r[0] {
			return cc.colNum, nil
		}
	}
	return -1, fmt.Errorf("engine: reference %s not found", r)
}

func (stmt *CreateTable) Resolve(ses *evaluate.Session) {
	stmt.Table = ses.ResolveTableName(stmt.Table)

	for _, fk := range stmt.ForeignKeys {
		fk.FKTable = stmt.Table
		fk.Resolve(ses)
	}
}

func (stmt *CreateTable) Plan(ctx context.Context, tx sql.Transaction) (evaluate.Plan, error) {
	for _, fk := range stmt.ForeignKeys {
		pfk, err := fk.Plan(ctx, tx)
		if err != nil {
			return nil, err
		}
		stmt.foreignKeys = append(stmt.foreignKeys, pfk)
	}

	for _, con := range stmt.Constraints {
		var key []sql.ColumnKey
		var check sql.CExpr
		var checkExpr string

		switch con.Type {
		case sql.PrimaryConstraint:
			fallthrough
		case sql.UniqueConstraint:
			var err error
			key, err = indexKeyToColumnKeys(con.Key, stmt.Columns)
			if err != nil {
				return nil, fmt.Errorf("engine: %s in key for table %s", err, stmt.Table)
			}
		case sql.CheckConstraint:
			var err error
			var cctx expr.CompileContext
			if con.ColNum >= 0 {
				cctx = columnCheck{stmt.Columns[con.ColNum], con.ColNum}
			} else {
				cctx = tableCheck(stmt.Columns)
			}
			check, err = expr.Compile(ctx, tx, cctx, con.Check)
			if err != nil {
				return nil, err
			}
			checkExpr = con.Check.String()
		}

		stmt.constraints = append(stmt.constraints,
			sql.Constraint{
				Type:      con.Type,
				Name:      con.Name,
				ColNum:    con.ColNum,
				Key:       key,
				Check:     check,
				CheckExpr: checkExpr,
			})
	}

	for _, ct := range stmt.ColumnTypes {
		var dflt sql.CExpr
		var dfltExpr string
		if ct.Default != nil {
			var err error
			dflt, err = expr.Compile(ctx, tx, nil, ct.Default)
			if err != nil {
				return nil, err
			}
			dfltExpr = ct.Default.String()
		}
		stmt.columnTypes = append(stmt.columnTypes,
			sql.ColumnType{
				Type:        ct.Type,
				Size:        ct.Size,
				Fixed:       ct.Fixed,
				NotNull:     ct.NotNull,
				Default:     dflt,
				DefaultExpr: dfltExpr,
			})
	}

	return stmt, nil
}

func (stmt *CreateTable) Explain() string {
	return stmt.String()
}

func (stmt *CreateTable) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	err := tx.CreateTable(ctx, stmt.Table, stmt.Columns, stmt.columnTypes, stmt.constraints,
		stmt.IfNotExists)
	if err != nil {
		return -1, err
	}

	for _, fk := range stmt.foreignKeys {
		err = tx.NextStmt(ctx)
		if err != nil {
			return -1, err
		}

		_, err = fk.Execute(ctx, tx)
		if err != nil {
			return -1, err
		}
	}

	return -1, nil
}

type CreateIndex struct {
	Index       sql.Identifier
	Table       sql.TableName
	Key         IndexKey
	IfNotExists bool
}

func (stmt *CreateIndex) String() string {
	s := "CREATE"
	if stmt.Key.Unique {
		s += " UNIQUE "
	}
	s += " INDEX"
	if stmt.IfNotExists {
		s += " IF NOT EXISTS"
	}
	s += fmt.Sprintf(" %s ON %s (%s)", stmt.Index, stmt.Table, stmt.Key)
	return s
}

func (stmt *CreateIndex) Resolve(ses *evaluate.Session) {
	stmt.Table = ses.ResolveTableName(stmt.Table)
}

func (stmt *CreateIndex) Plan(ctx context.Context, tx sql.Transaction) (evaluate.Plan, error) {
	return stmt, nil
}

func (stmt *CreateIndex) Explain() string {
	return stmt.String()
}

func (stmt *CreateIndex) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	tt, err := tx.LookupTableType(ctx, stmt.Table)
	if err != nil {
		return -1, err
	}

	colKeys, err := indexKeyToColumnKeys(stmt.Key, tt.Columns())
	if err != nil {
		return -1, fmt.Errorf("engine: %s in unique key for table %s", err, stmt.Table)
	}

	return -1, tx.CreateIndex(ctx, stmt.Index, stmt.Table, stmt.Key.Unique, colKeys,
		stmt.IfNotExists)
}

type CreateDatabase struct {
	Database sql.Identifier
	Options  map[sql.Identifier]string
}

func (stmt *CreateDatabase) String() string {
	s := fmt.Sprintf("CREATE DATABASE %s", stmt.Database)
	if len(stmt.Options) > 0 {
		s += " WITH"
		for opt, val := range stmt.Options {
			s = fmt.Sprintf("%s %s = %s", s, opt, val)
		}
	}
	return s
}

func (_ *CreateDatabase) Resolve(ses *evaluate.Session) {}

func (stmt *CreateDatabase) Plan(ctx context.Context, tx sql.Transaction) (evaluate.Plan, error) {
	return stmt, nil
}

func (stmt *CreateDatabase) Explain() string {
	return stmt.String()
}

func (stmt *CreateDatabase) Command(ctx context.Context, ses *evaluate.Session,
	e sql.Engine) (int64, error) {

	return -1, e.CreateDatabase(stmt.Database, stmt.Options)
}

type CreateSchema struct {
	Schema sql.SchemaName
}

func (stmt *CreateSchema) String() string {
	return fmt.Sprintf("CREATE SCHEMA %s", stmt.Schema)
}

func (stmt *CreateSchema) Resolve(ses *evaluate.Session) {
	stmt.Schema = ses.ResolveSchemaName(stmt.Schema)
}

func (stmt *CreateSchema) Plan(ctx context.Context, tx sql.Transaction) (evaluate.Plan, error) {
	return stmt, nil
}

func (stmt *CreateSchema) Explain() string {
	return stmt.String()
}

func (stmt *CreateSchema) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	return -1, tx.CreateSchema(ctx, stmt.Schema)
}
