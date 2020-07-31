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

type ForeignKey struct {
	KeyColumns []sql.Identifier
	RefTable   sql.TableName
	RefColumns []sql.Identifier
}

func (fk ForeignKey) String() string {
	s := "FOREIGN KEY ("
	for i, c := range fk.KeyColumns {
		if i > 0 {
			s += ", "
		}
		s += c.String()
	}
	s += fmt.Sprintf(") REFERENCES %s (", fk.RefTable)
	for i, c := range fk.RefColumns {
		if i > 0 {
			s += ", "
		}
		s += c.String()
	}
	s += ")"
	return s
}

type Constraint struct {
	Type       sql.ConstraintType
	Name       sql.Identifier
	ColNum     int
	Key        IndexKey
	Check      expr.Expr
	ForeignKey ForeignKey
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
	case sql.ForeignConstraint:
		return fmt.Sprintf(", CONSTRAINT %s %s", c.Name, c.ForeignKey.String())
	default:
		panic(fmt.Sprintf("unexpected constraint type: %d", c.Type))
	}

	return ""
}

type foreignConstraint struct {
	name       sql.Identifier
	foreignKey ForeignKey
}

type ColumnType struct {
	Type    sql.DataType
	Size    uint32
	Fixed   bool
	NotNull bool
	Default expr.Expr
}

type CreateTable struct {
	Table              sql.TableName
	Columns            []sql.Identifier
	ColumnTypes        []ColumnType
	columnTypes        []sql.ColumnType
	IfNotExists        bool
	Constraints        []Constraint
	constraints        []sql.Constraint
	foreignConstraints []foreignConstraint
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

func (stmt *CreateTable) Plan(ses *evaluate.Session, tx sql.Transaction) (interface{}, error) {
	stmt.Table = ses.ResolveTableName(stmt.Table)

	for _, con := range stmt.Constraints {
		if con.Type == sql.ForeignConstraint {
			fk := con.ForeignKey
			fk.RefTable = ses.ResolveTableName(fk.RefTable)
			stmt.foreignConstraints = append(stmt.foreignConstraints,
				foreignConstraint{
					name:       con.Name,
					foreignKey: fk,
				})
		} else {
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
				check, err = expr.Compile(ses, tx, cctx, con.Check)
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
	}

	for _, ct := range stmt.ColumnTypes {
		var dflt sql.CExpr
		var dfltExpr string
		if ct.Default != nil {
			var err error
			dflt, err = expr.Compile(ses, tx, nil, ct.Default)
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

func (stmt *CreateTable) prepareForeignConstraint(ctx context.Context, e sql.Engine,
	tx sql.Transaction, fc foreignConstraint) error {

	fk := fc.foreignKey
	_, tt, err := e.LookupTable(ctx, tx, fk.RefTable)
	if err != nil {
		return err
	}

	var refCols []int
	if len(fk.RefColumns) == 0 {
		for _, ck := range tt.PrimaryKey() {
			refCols = append(refCols, ck.Column())
		}
	} else {
		refColumns := tt.Columns()
		for _, col := range fk.RefColumns {
			num, ok := columnNumber(col, refColumns)
			if !ok {
				return fmt.Errorf("engine: table %s: reference column %s to table %s not found",
					stmt.Table, col, fk.RefTable)
			}
			refCols = append(refCols, num)
		}
	}

	if len(refCols) != len(fk.KeyColumns) {
		return fmt.Errorf("engine: table %s: foreign constraint %s: different column counts",
			stmt.Table, fc.name)
	}

	var keyCols []int
	refColTypes := tt.ColumnTypes()
	for idx, col := range fk.KeyColumns {
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
				KeyColumns: keyCols,
				RefTable:   fk.RefTable,
				RefColumns: refCols,
			},
		})
	return nil
}

func (stmt *CreateTable) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	for _, fc := range stmt.foreignConstraints {
		err := stmt.prepareForeignConstraint(ctx, e, tx, fc)
		if err != nil {
			return -1, err
		}
	}

	err := e.CreateTable(ctx, tx, stmt.Table, stmt.Columns, stmt.columnTypes, stmt.constraints,
		stmt.IfNotExists)
	if err != nil {
		return -1, err
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

func (stmt *CreateIndex) Plan(ses *evaluate.Session, tx sql.Transaction) (interface{}, error) {
	stmt.Table = ses.ResolveTableName(stmt.Table)
	return stmt, nil
}

func (stmt *CreateIndex) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	_, tt, err := e.LookupTable(ctx, tx, stmt.Table)
	if err != nil {
		return -1, err
	}

	colKeys, err := indexKeyToColumnKeys(stmt.Key, tt.Columns())
	if err != nil {
		return -1, fmt.Errorf("engine: %s in unique key for table %s", err, stmt.Table)
	}

	return -1, e.CreateIndex(ctx, tx, stmt.Index, stmt.Table, stmt.Key.Unique, colKeys,
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

func (stmt *CreateDatabase) Plan(ses *evaluate.Session, tx sql.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *CreateDatabase) Command(ses *evaluate.Session) error {
	return ses.Engine.CreateDatabase(stmt.Database, stmt.Options)
}

type CreateSchema struct {
	Schema sql.SchemaName
}

func (stmt *CreateSchema) String() string {
	return fmt.Sprintf("CREATE SCHEMA %s", stmt.Schema)
}

func (stmt *CreateSchema) Plan(ses *evaluate.Session, tx sql.Transaction) (interface{}, error) {
	stmt.Schema = ses.ResolveSchemaName(stmt.Schema)
	return stmt, nil
}

func (stmt *CreateSchema) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	return -1, e.CreateSchema(ctx, tx, stmt.Schema)
}
