package query

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/sql"
)

type FromItem interface {
	fmt.Stringer
	plan(ctx context.Context, pctx evaluate.PlanContext, tx sql.Transaction,
		cond expr.Expr) (rowsOp, *fromContext, error)
}

type FromTableAlias struct {
	sql.TableName
	Alias sql.Identifier
}

func (fta FromTableAlias) String() string {
	s := fta.TableName.String()
	if fta.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fta.Alias)
	}
	return s
}

func equalKeyExpr(fctx expr.CompileContext, cond expr.Expr,
	key []sql.ColumnKey, cols []int) []expr.ColExpr {

	ce := expr.EqualColExpr(fctx, cond)
	if len(key) != len(ce) {
		return nil
	}

	if cols != nil {
		for cdx := range ce {
			ce[cdx].Col = cols[ce[cdx].Col]
		}
	}

	for _, ck := range key {
		found := false
		for cdx := range ce {
			if ck.Column() == ce[cdx].Col {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}

	return ce
}

func (fta FromTableAlias) plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction, cond expr.Expr) (rowsOp, *fromContext, error) {

	tn := pctx.ResolveTableName(fta.TableName)
	tt, err := tx.LookupTableType(ctx, tn)
	if err != nil {
		return nil, nil, err
	}

	nam := tn.Table
	if fta.Alias != 0 {
		nam = fta.Alias
	}
	fctx := makeFromContext(nam, tt.Columns(), tt.ColumnTypes())

	if cond != nil && pctx.GetFlag(flags.PushdownWhere) {
		if colExpr := equalKeyExpr(fctx, cond, tt.PrimaryKey(), nil); colExpr != nil {
			valKey := make([]*sql.Value, len(tt.Columns()))
			for _, ce := range colExpr {
				if ce.Param > 0 {
					ptr, err := pctx.PlanParameter(ce.Param)
					if err != nil {
						return nil, nil, err
					}
					valKey[ce.Col] = ptr
				} else {
					val := ce.Val
					valKey[ce.Col] = &val
				}
			}

			return scanTableOp{
				tn:     tn,
				ttVer:  tt.Version(),
				cols:   tt.Columns(),
				valKey: valKey,
			}, fctx, nil
		}
	}

	rop, err := where(ctx, pctx, tx, scanTableOp{tn: tn, ttVer: tt.Version(), cols: tt.Columns()},
		fctx, cond)
	if err != nil {
		return nil, nil, err
	}

	return rop, fctx, nil
}

type scanTableOp struct {
	tn     sql.TableName
	ttVer  int64
	cols   []sql.Identifier
	valKey []*sql.Value
}

func (sto scanTableOp) Name() string {
	return "scan table"
}

func (sto scanTableOp) Columns() []string {
	var cols []string
	for _, col := range sto.cols {
		cols = append(cols, col.String())
	}
	return cols
}

func filterField(valKey []*sql.Value, cols []sql.Identifier) evaluate.FieldDescription {
	var desc string
	for col, ptr := range valKey {
		if ptr != nil {
			if desc != "" {
				desc += ", "
			}
			desc += fmt.Sprintf("%s = %s", cols[col], *ptr)
		}
	}

	return evaluate.FieldDescription{Field: "filter", Description: desc}
}

func (sto scanTableOp) Fields() []evaluate.FieldDescription {
	fd := []evaluate.FieldDescription{
		{Field: "table", Description: sto.tn.String()},
	}

	if sto.valKey != nil {
		fd = append(fd, filterField(sto.valKey, sto.cols))
	}
	return fd
}

func (_ scanTableOp) Children() []evaluate.ExplainTree {
	return nil
}

func (sto scanTableOp) rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	tbl, err := tx.LookupTable(ctx, sto.tn, sto.ttVer)
	if err != nil {
		return nil, err
	}

	var keyRow []sql.Value
	if sto.valKey != nil {
		keyRow = make([]sql.Value, len(sto.cols))
		for col, ptr := range sto.valKey {
			if ptr != nil {
				keyRow[col] = *ptr
			}
		}
	}
	return tbl.Rows(ctx, keyRow, keyRow)
}

type FromIndexAlias struct {
	sql.TableName
	Index sql.Identifier
	Alias sql.Identifier
}

func (fia FromIndexAlias) String() string {
	s := fmt.Sprintf("%s@%s", fia.TableName, fia.Index)
	if fia.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fia.Alias)
	}
	return s
}

func (fia FromIndexAlias) plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction, cond expr.Expr) (rowsOp, *fromContext, error) {

	tn := pctx.ResolveTableName(fia.TableName)
	tt, err := tx.LookupTableType(ctx, tn)
	if err != nil {
		return nil, nil, err
	}

	iidx := -1
	for idx, it := range tt.Indexes() {
		if it.Name == fia.Index {
			iidx = idx
			break
		}
	}
	if iidx < 0 {
		return nil, nil,
			fmt.Errorf("engine: table %s: index %s not found", fia.TableName, fia.Index)
	}

	it := tt.Indexes()[iidx]
	ttCols := tt.Columns()
	ttColTypes := tt.ColumnTypes()
	var cols []sql.Identifier
	var colTypes []sql.ColumnType
	for _, col := range it.Columns {
		cols = append(cols, ttCols[col])
		colTypes = append(colTypes, ttColTypes[col])
	}

	nam := fia.Index
	if fia.Alias != 0 {
		nam = fia.Alias
	}
	fctx := makeFromContext(nam, cols, colTypes)
	sio := scanIndexOp{
		tn:    tn,
		index: fia.Index,
		iidx:  iidx,
		ttVer: tt.Version(),
		cols:  cols,
	}

	if cond != nil && pctx.GetFlag(flags.PushdownWhere) {
		if colExpr := equalKeyExpr(fctx, cond, it.Key, it.Columns); colExpr != nil {
			valKey := make([]*sql.Value, len(tt.Columns()))
			for _, ce := range colExpr {
				if ce.Param > 0 {
					ptr, err := pctx.PlanParameter(ce.Param)
					if err != nil {
						return nil, nil, err
					}
					valKey[ce.Col] = ptr
				} else {
					val := ce.Val
					valKey[ce.Col] = &val
				}
			}

			sio.valKey = valKey
			sio.ttCols = ttCols
			return sio, fctx, nil
		}
	}

	rop, err := where(ctx, pctx, tx, sio, fctx, cond)
	if err != nil {
		return nil, nil, err
	}
	return rop, fctx, nil
}

type scanIndexOp struct {
	tn     sql.TableName
	index  sql.Identifier
	iidx   int
	ttVer  int64
	cols   []sql.Identifier
	ttCols []sql.Identifier
	valKey []*sql.Value
}

func (sio scanIndexOp) Name() string {
	return "scan index"
}

func (sio scanIndexOp) Columns() []string {
	var cols []string
	for _, col := range sio.cols {
		cols = append(cols, col.String())
	}
	return cols
}

func (sio scanIndexOp) Fields() []evaluate.FieldDescription {
	fd := []evaluate.FieldDescription{
		{Field: "table", Description: sio.tn.String()},
		{Field: "index", Description: sio.index.String()},
	}

	if sio.valKey != nil {
		fd = append(fd, filterField(sio.valKey, sio.ttCols))
	}
	return fd
}

func (_ scanIndexOp) Children() []evaluate.ExplainTree {
	return nil
}

func (sio scanIndexOp) rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	tbl, err := tx.LookupTable(ctx, sio.tn, sio.ttVer)
	if err != nil {
		return nil, err
	}

	var keyRow []sql.Value
	if sio.valKey != nil {
		keyRow = make([]sql.Value, len(sio.ttCols))
		for col, ptr := range sio.valKey {
			if ptr != nil {
				keyRow[col] = *ptr
			}
		}
	}
	return tbl.IndexRows(ctx, sio.iidx, keyRow, keyRow)
}

type FromStmt struct {
	Stmt          evaluate.Stmt
	Alias         sql.Identifier
	ColumnAliases []sql.Identifier
}

func (fs FromStmt) String() string {
	s := fmt.Sprintf("(%s) AS %s", fs.Stmt, fs.Alias)
	if fs.ColumnAliases != nil {
		s += " ("
		for i, col := range fs.ColumnAliases {
			if i > 0 {
				s += ", "
			}
			s += col.String()
		}
		s += ")"
	}
	return s
}

func (fs FromStmt) plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction, cond expr.Expr) (rowsOp, *fromContext, error) {

	plan, err := fs.Stmt.Plan(ctx, pctx, tx)
	if err != nil {
		return nil, nil, err
	}
	rowsPlan := plan.(evaluate.RowsPlan)

	cols := rowsPlan.Columns()
	if fs.ColumnAliases != nil {
		if len(fs.ColumnAliases) != len(cols) {
			return nil, nil, fmt.Errorf("engine: wrong number of column aliases")
		}
		cols = fs.ColumnAliases
	}
	fctx := makeFromContext(fs.Alias, cols, rowsPlan.ColumnTypes())

	if rp, ok := rowsPlan.(rowsOpPlan); ok {
		return rp.rop, fctx, nil
	}
	rop, err := where(ctx, pctx, tx, fromPlanOp{rowsPlan, fs.Stmt.String(), cols}, fctx, cond)
	if err != nil {
		return nil, nil, err
	}
	return rop, fctx, nil
}

type fromPlanOp struct {
	rp   evaluate.RowsPlan
	stmt string
	cols []sql.Identifier
}

func (_ fromPlanOp) Name() string {
	return "stmt"
}

func (fpo fromPlanOp) Columns() []string {
	var cols []string
	for _, col := range fpo.cols {
		cols = append(cols, col.String())
	}
	return cols
}

func (fpo fromPlanOp) Fields() []evaluate.FieldDescription {
	return []evaluate.FieldDescription{
		{Field: "stmt", Description: fpo.stmt},
	}
}

func (fpo fromPlanOp) Children() []evaluate.ExplainTree {
	return nil
}

func (fpo fromPlanOp) rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	return fpo.rp.Rows(ctx, tx)
}

type colRef struct {
	table  sql.Identifier
	column sql.Identifier
}

func (cr colRef) String() string {
	if cr.table == 0 {
		return cr.column.String()
	}
	return fmt.Sprintf("%s.%s", cr.table, cr.column)
}

type fromCol struct {
	colNum    int
	ct        sql.ColumnType
	ambiguous bool
}

type fromContext struct {
	colMap    map[sql.Identifier]fromCol
	colRefMap map[colRef]fromCol
	cols      []colRef
	colTypes  []sql.ColumnType
}

func makeFromContext(nam sql.Identifier, cols []sql.Identifier,
	colTypes []sql.ColumnType) *fromContext {

	fctx := &fromContext{colMap: map[sql.Identifier]fromCol{}, colRefMap: map[colRef]fromCol{}}
	for idx, col := range cols {
		fctx.colMap[col] = fromCol{colNum: idx, ct: colTypes[idx]}
		if nam != 0 {
			fctx.colRefMap[colRef{table: nam, column: col}] =
				fromCol{colNum: idx, ct: colTypes[idx]}
		}
		fctx.cols = append(fctx.cols, colRef{table: nam, column: col})
		fctx.colTypes = append(fctx.colTypes, colTypes[idx])
	}
	return fctx
}

func (fctx *fromContext) copy() *fromContext {
	nctx := fromContext{
		colMap:    map[sql.Identifier]fromCol{},
		colRefMap: map[colRef]fromCol{},
		cols:      fctx.cols,
		colTypes:  fctx.colTypes,
	}
	for col, fc := range fctx.colMap {
		nctx.colMap[col] = fc
	}
	for cr, fc := range fctx.colRefMap {
		nctx.colRefMap[cr] = fc
	}
	return &nctx
}

func (fctx *fromContext) addColumn(cr colRef, ct sql.ColumnType) {
	idx := len(fctx.cols)
	fctx.cols = append(fctx.cols, cr)
	fctx.colTypes = append(fctx.colTypes, ct)
	if _, ok := fctx.colMap[cr.column]; ok {
		fctx.colMap[cr.column] = fromCol{ambiguous: true}
	} else {
		fctx.colMap[cr.column] = fromCol{colNum: idx, ct: ct}
	}
	if _, ok := fctx.colRefMap[cr]; ok {
		fctx.colRefMap[cr] = fromCol{ambiguous: true}
	} else {
		fctx.colRefMap[cr] = fromCol{colNum: idx, ct: ct}
	}
}

func joinContextsOn(lctx, rctx *fromContext) *fromContext {
	// Create a new fromContext as a copy of the left context.
	fctx := lctx.copy()

	// Merge in the right context.
	for rdx, cr := range rctx.cols {
		fctx.addColumn(cr, rctx.colTypes[rdx])
	}
	return fctx
}

func (fctx *fromContext) usingIndex(col sql.Identifier, side string) (int, error) {
	fc, ok := fctx.colMap[col]
	if !ok {
		return -1, fmt.Errorf("engine: %s not found on %s side of join", col, side)
	}
	if fc.ambiguous {
		return -1, fmt.Errorf("engine: %s is ambigous on %s side of join", col, side)
	}
	return fc.colNum, nil
}

func joinContextsUsing(lctx, rctx *fromContext, useSet map[sql.Identifier]struct{}) (*fromContext,
	[]int) {

	// Create a new fromContext as a copy of the left context.
	fctx := lctx.copy()

	// Merge in the right context, skipping columns in use set.
	src2dest := make([]int, 0, len(rctx.cols)-len(useSet))
	for idx, cr := range rctx.cols {
		if _, ok := useSet[cr.column]; ok {
			continue
		}
		fctx.addColumn(cr, rctx.colTypes[idx])
		src2dest = append(src2dest, idx)
	}
	return fctx, src2dest
}

func (fctx *fromContext) CompileRef(r expr.Ref) (int, sql.ColumnType, error) {
	if len(r) == 1 {
		return fctx.colIndex(r[0], "reference")
	} else if len(r) == 2 {
		return fctx.tblColIndex(r[0], r[1], "reference")
	}
	return -1, sql.ColumnType{}, fmt.Errorf("engine: %s is not a valid reference", r)
}

func (fctx *fromContext) colIndex(col sql.Identifier, what string) (int, sql.ColumnType, error) {
	fc, ok := fctx.colMap[col]
	if !ok {
		return -1, sql.ColumnType{}, fmt.Errorf("engine: %s %s not found", what, col)
	}
	if fc.ambiguous {
		return -1, sql.ColumnType{}, fmt.Errorf("engine: %s %s is ambiguous", what, col)
	}
	return fc.colNum, fc.ct, nil
}

func (fctx *fromContext) tblColIndex(tbl, col sql.Identifier, what string) (int, sql.ColumnType,
	error) {

	if tbl == 0 {
		return fctx.colIndex(col, what)
	}
	cr := colRef{table: tbl, column: col}
	fc, ok := fctx.colRefMap[cr]
	if !ok {
		return -1, sql.ColumnType{}, fmt.Errorf("engine: %s %s not found", what, cr.String())
	}
	if fc.ambiguous {
		return -1, sql.ColumnType{}, fmt.Errorf("engine: %s %s is ambiguous", what, cr.String())
	}
	return fc.colNum, fc.ct, nil
}

func (fctx *fromContext) tableColumns(tbl sql.Identifier) []sql.Identifier {
	var cols []sql.Identifier
	for _, cr := range fctx.cols {
		if cr.table == tbl {
			cols = append(cols, cr.column)
		}
	}
	return cols
}

func (fctx *fromContext) columns() []sql.Identifier {
	var cols []sql.Identifier
	for _, cr := range fctx.cols {
		cols = append(cols, cr.column)
	}
	return cols
}

func (fctx *fromContext) columnTypes() []sql.ColumnType {
	return fctx.colTypes
}
