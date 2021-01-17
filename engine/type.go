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
	onDelete sql.RefAction
	onUpdate sql.RefAction
}

type foreignRef struct {
	name sql.Identifier
	tn   sql.TableName
}

type TableType struct {
	ver         int64
	cols        []sql.Identifier
	colTypes    []sql.ColumnType
	colDefaults []sql.ColumnDefault
	primary     []sql.ColumnKey
	indexes     []sql.IndexType
	constraints []constraint
	checks      []checkConstraint
	foreignKeys []foreignKey
	foreignRefs []foreignRef
	triggers    []trigger
	events      int64
}

func MakeTableType(cols []sql.Identifier, colTypes []sql.ColumnType,
	colDefaults []sql.ColumnDefault, primary []sql.ColumnKey) *TableType {

	return &TableType{
		ver:         1,
		cols:        cols,
		colTypes:    colTypes,
		colDefaults: colDefaults,
		primary:     primary,
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

func (tt *TableType) ColumnDefaults() []sql.ColumnDefault {
	return tt.colDefaults
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

func (tt *TableType) dropTrigger(dfn func(trig sql.Trigger) bool) int {
	var triggers []trigger
	dropped := 0
	for _, trig := range tt.triggers {
		if dfn(trig.trig) {
			dropped += 1
		} else {
			triggers = append(triggers, trig)
		}
	}
	tt.triggers = triggers
	return dropped
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
	unique, hidden bool) sql.IndexType {

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
		Hidden:  hidden,
	}
}

func (tt *TableType) AddIndex(idxname sql.Identifier, unique bool,
	key []sql.ColumnKey) (*TableType, sql.IndexType, int, bool) {

	var it sql.IndexType

	for idx, it := range tt.indexes {
		if it.Name == idxname {
			return nil, it, idx, true
		}
	}

	tt.ver += 1
	it = tt.makeIndexType(idxname, key, unique, true)
	tt.indexes = append(tt.indexes, it)
	return tt, it, len(tt.indexes) - 1, false
}

func (tt *TableType) ShowIndex(idxname sql.Identifier) *TableType {
	for idx, it := range tt.indexes {
		if it.Name == idxname && it.Hidden {
			tt.indexes[idx].Hidden = false
			tt.ver += 1
			return tt
		}
	}

	return nil
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

func (tt *TableType) primaryColumn(colNum int) bool {
	for _, ck := range tt.primary {
		if ck.Column() == colNum {
			return true
		}
	}
	return false
}

func (tt *TableType) dropColumnConstraint(tn sql.TableName, cn sql.Identifier) error {
	var constraints []constraint
	for _, con := range tt.constraints {
		if con.name == cn {
			switch con.typ {
			case sql.DefaultConstraint:
				if tt.colDefaults[con.colNum].Default == nil ||
					tt.colDefaults[con.colNum].DefaultExpr == "" {
					panic(fmt.Sprintf(
						"table %s: mismatch between constraint and column default", tn))
				}
				tt.colDefaults[con.colNum].Default = nil
				tt.colDefaults[con.colNum].DefaultExpr = ""
			case sql.NotNullConstraint:
				if !tt.colTypes[con.colNum].NotNull {
					panic(fmt.Sprintf(
						"table %s: mismatch between constraint and column not null", tn))
				}
				if tt.primaryColumn(con.colNum) {
					return fmt.Errorf("engine: table %s: primary column %s must be not null", tn,
						tt.cols[con.colNum])
				}
				tt.colTypes[con.colNum].NotNull = false
			default:
				panic(fmt.Sprintf("table %s: constraint %s: unexpected type: %v", tn, cn, con.typ))
			}
		} else {
			constraints = append(constraints, con)
		}
	}

	tt.constraints = constraints
	tt.ver += 1
	return nil
}

func (tt *TableType) dropCheckConstraint(tn sql.TableName, cn sql.Identifier) {
	var checks []checkConstraint
	for _, chk := range tt.checks {
		if chk.name != cn {
			checks = append(checks, chk)
		}
	}

	tt.checks = checks
	tt.ver += 1
}

func (tt *TableType) dropConstraint(tn sql.TableName, cn, col sql.Identifier,
	ct sql.ConstraintType) (bool, error) {

	if cn != 0 {
		for _, con := range tt.constraints {
			if con.name == cn {
				return true, tt.dropColumnConstraint(tn, cn)
			}
		}

		for _, chk := range tt.checks {
			if chk.name == cn {
				tt.dropCheckConstraint(tn, cn)
				return true, nil
			}
		}

		return false, fmt.Errorf("engine: table %s: constraint not found: %s", tn, cn)
	} else if col == 0 {
		panic(fmt.Sprintf("table %s: one of constraint or column required", tn))
	}

	colNum := -1
	for cdx, nam := range tt.cols {
		if col == nam {
			colNum = cdx
			break
		}
	}
	if colNum < 0 {
		return false, fmt.Errorf("engine: table %s: column not found: %s", tn, col)
	}

	switch ct {
	case sql.DefaultConstraint:
		if tt.colDefaults[colNum].Default == nil ||
			tt.colDefaults[colNum].DefaultExpr == "" {
			return true,
				fmt.Errorf("engine: table %s: column %s: missing default constraint", tn, col)
		}
		tt.colDefaults[colNum].Default = nil
		tt.colDefaults[colNum].DefaultExpr = ""
	case sql.NotNullConstraint:
		if !tt.colTypes[colNum].NotNull {
			return true,
				fmt.Errorf("engine: table %s: column %s: missing not null constraint", tn, col)
		}
		if tt.primaryColumn(colNum) {
			return true,
				fmt.Errorf("engine: table %s: primary column %s must be not null", tn, col)
		}
		tt.colTypes[colNum].NotNull = false
	default:
		panic(fmt.Sprintf("table %s: column %s: unexpected type: %v", tn, col, ct))
	}

	var constraints []constraint
	for _, con := range tt.constraints {
		if con.colNum != colNum || con.typ != ct {
			constraints = append(constraints, con)
		}
	}

	tt.constraints = constraints
	tt.ver += 1
	return true, nil
}

func (tt *TableType) duplicateConstraint(cn sql.Identifier) bool {
	if cn == sql.PRIMARY_QUOTED {
		return true
	}

	for _, con := range tt.constraints {
		if con.name == cn {
			return true
		}
	}

	for _, ck := range tt.checks {
		if ck.name == cn {
			return true
		}
	}

	for _, fk := range tt.foreignKeys {
		if fk.name == cn {
			return true
		}
	}

	return false
}

func (tt *TableType) constraintName(tn sql.TableName, con sql.Identifier,
	base string) (sql.Identifier, error) {

	if con == 0 {
		cnt := 1
		for {
			con := sql.ID(fmt.Sprintf("%s%d", base, cnt))
			if !tt.duplicateConstraint(con) {
				return con, nil
			}
			cnt += 1
		}
	} else if tt.duplicateConstraint(con) {
		return 0, fmt.Errorf("engine: table: %s: duplicate constraint name: %s", tn, con)
	}

	return con, nil
}

func (tt *TableType) lookupForeignKey(con sql.Identifier) (foreignKey, bool) {
	for _, fk := range tt.foreignKeys {
		if fk.name == con {
			return fk, true
		}
	}
	return foreignKey{}, false
}

func generateCheckSQL(fktn sql.TableName, fkCols []int, fktt *TableType, rtn sql.TableName,
	ridx sql.Identifier, rtt *TableType) string {

	rkey := rtt.lookupIndex(rtn, ridx)
	s := fmt.Sprintf("SELECT COUNT(*) FROM %s LEFT JOIN %s ON", generateTableName(fktn),
		generateTableName(rtn))
	for cdx, col := range fkCols {
		if cdx > 0 {
			s += " AND"
		}
		s += fmt.Sprintf(" (%s.%s = %s.%s)", fktn.Table, fktt.cols[col], rtn.Table,
			rtt.cols[rkey[cdx].Column()])
	}
	s += " WHERE"
	for cdx, ck := range rkey {
		if cdx > 0 {
			s += " OR"
		}
		s += fmt.Sprintf(" (%s.%s IS NULL)", rtn.Table, rtt.cols[ck.Column()])
	}
	return s
}

func generateTableName(tn sql.TableName) string {
	return fmt.Sprintf(`"%s"."%s"."%s"`, tn.Database, tn.Schema, tn.Table)
}

func generateMatchSQL(rtt *TableType, rtn sql.TableName, ridx sql.Identifier, rkey []sql.ColumnKey,
	fkCols []int) string {

	s := "SELECT COUNT(*) FROM " + generateTableName(rtn)
	if ridx != sql.PRIMARY_QUOTED {
		s += fmt.Sprintf(`@"%s"`, ridx)
	}
	s += " WHERE"
	for cdx, ck := range rkey {
		if cdx > 0 {
			s += " AND"
		}
		s += fmt.Sprintf(` "%s" = $%d`, rtt.cols[ck.Column()], cdx+1)
	}

	return s
}

func generateRestrictSQL(fktn sql.TableName, fkCols []int, fktt *TableType) string {
	s := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE", generateTableName(fktn))
	for cdx, col := range fkCols {
		if cdx > 0 {
			s += " AND"
		}
		s += fmt.Sprintf(" %s = $%d", fktt.cols[col], cdx+1)
	}
	return s
}

func generateDeleteSQL(fktn sql.TableName, fkCols []int, fktt *TableType) string {
	s := fmt.Sprintf("DELETE FROM %s WHERE", generateTableName(fktn))
	for cdx, col := range fkCols {
		if cdx > 0 {
			s += " AND"
		}
		s += fmt.Sprintf(" %s = $%d", fktt.cols[col], cdx+1)
	}
	return s
}

func generateUpdateSQL(fktn sql.TableName, fkCols []int, fktt *TableType) string {
	s := fmt.Sprintf("UPDATE %s SET", generateTableName(fktn))
	for cdx, col := range fkCols {
		if cdx > 0 {
			s += " ,"
		}
		s += fmt.Sprintf(" %s = $%d", fktt.cols[col], cdx+len(fkCols)+1)
	}
	s += " WHERE"
	for cdx, col := range fkCols {
		if cdx > 0 {
			s += " AND"
		}
		s += fmt.Sprintf(" %s = $%d", fktt.cols[col], cdx+1)
	}
	return s
}

func generateSetSQL(fktn sql.TableName, fkCols []int, fktt *TableType, setNull bool) string {
	s := fmt.Sprintf("UPDATE %s SET", generateTableName(fktn))
	for cdx, col := range fkCols {
		if cdx > 0 {
			s += " ,"
		}
		s += fmt.Sprintf(" %s = ", fktt.cols[col])
		if setNull {
			s += "NULL"
		} else {
			s += "DEFAULT"
		}
	}
	s += " WHERE"
	for cdx, col := range fkCols {
		if cdx > 0 {
			s += " AND"
		}
		s += fmt.Sprintf(" %s = $%d", fktt.cols[col], cdx+1)
	}
	return s
}

func (tt *TableType) lookupIndex(tn sql.TableName, idx sql.Identifier) []sql.ColumnKey {
	if idx == sql.PRIMARY_QUOTED {
		return tt.primary
	}

	for _, it := range tt.indexes {
		if it.Name == idx {
			return it.Key
		}
	}

	panic(fmt.Sprintf("table %s: can't find index %d", tn, idx))

}
func addForeignKey(con sql.Identifier, fktn sql.TableName, fkCols []int, fktt *TableType,
	rtn sql.TableName, ridx sql.Identifier, rtt *TableType, onDel, onUpd sql.RefAction) error {

	con, err := fktt.constraintName(fktn, con, "foreign_")
	if err != nil {
		return err
	}

	fktt.foreignKeys = append(fktt.foreignKeys,
		foreignKey{
			name:     con,
			keyCols:  fkCols,
			refTable: rtn,
			refIndex: ridx,
			onDelete: onDel,
			onUpdate: onUpd,
		})

	rtt.foreignRefs = append(rtt.foreignRefs,
		foreignRef{
			name: con,
			tn:   fktn,
		})

	rkey := rtt.lookupIndex(rtn, ridx)

	fktt.addTrigger(sql.InsertEvent|sql.UpdateEvent,
		&fkTrigger{
			typ:     fkMatchTrigger,
			con:     con,
			fktn:    fktn,
			rtn:     rtn,
			keyCols: fkCols,
			sqlStmt: generateMatchSQL(rtt, rtn, ridx, rkey, fkCols),
		})

	frCols := make([]int, 0, len(rkey))
	for _, ck := range rkey {
		frCols = append(frCols, ck.Column())
	}

	switch onDel {
	case sql.NoAction, sql.Restrict:
		// Since constraints can't be deferred (yet), NoAction is the same as Restrict.
		rtt.addTrigger(sql.DeleteEvent,
			&fkTrigger{
				typ:     fkRestrictTrigger,
				con:     con,
				fktn:    fktn,
				rtn:     rtn,
				keyCols: frCols,
				sqlStmt: generateRestrictSQL(fktn, fkCols, fktt),
			})
	case sql.Cascade:
		rtt.addTrigger(sql.DeleteEvent,
			&fkTrigger{
				typ:     fkDeleteTrigger,
				con:     con,
				fktn:    fktn,
				rtn:     rtn,
				keyCols: frCols,
				sqlStmt: generateDeleteSQL(fktn, fkCols, fktt),
			})
	case sql.SetNull, sql.SetDefault:
		rtt.addTrigger(sql.DeleteEvent,
			&fkTrigger{
				typ:     fkSetTrigger,
				con:     con,
				fktn:    fktn,
				rtn:     rtn,
				keyCols: frCols,
				sqlStmt: generateSetSQL(fktn, fkCols, fktt, onDel == sql.SetNull),
			})
	default:
		panic(fmt.Sprintf("unexpected delete ref action: %v", onDel))
	}

	switch onUpd {
	case sql.NoAction, sql.Restrict:
		// Since constraints can't be deferred (yet), NoAction is the same as Restrict.
		rtt.addTrigger(sql.UpdateEvent,
			&fkTrigger{
				typ:     fkRestrictTrigger,
				con:     con,
				fktn:    fktn,
				rtn:     rtn,
				keyCols: frCols,
				sqlStmt: generateRestrictSQL(fktn, fkCols, fktt),
			})
	case sql.Cascade:
		rtt.addTrigger(sql.UpdateEvent,
			&fkTrigger{
				typ:     fkUpdateTrigger,
				con:     con,
				fktn:    fktn,
				rtn:     rtn,
				keyCols: frCols,
				sqlStmt: generateUpdateSQL(fktn, fkCols, fktt),
			})
	case sql.SetNull, sql.SetDefault:
		rtt.addTrigger(sql.UpdateEvent,
			&fkTrigger{
				typ:     fkSetTrigger,
				con:     con,
				fktn:    fktn,
				rtn:     rtn,
				keyCols: frCols,
				sqlStmt: generateSetSQL(fktn, fkCols, fktt, onUpd == sql.SetNull),
			})
	default:
		panic(fmt.Sprintf("unexpected update ref action: %v", onUpd))
	}

	fktt.ver += 1
	if rtt != fktt {
		rtt.ver += 1
	}

	return nil
}

func (tt *TableType) dropForeignRef(con sql.Identifier, fktn sql.TableName) *TableType {
	var foreignRefs []foreignRef
	for _, fr := range tt.foreignRefs {
		if fr.tn == fktn && fr.name == con {
			dropped := tt.dropTrigger(
				func(trig sql.Trigger) bool {
					if fkt, ok := trig.(*fkTrigger); ok && fkt.con == con && fkt.fktn == fktn {
						return true
					}
					return false
				})
			if dropped == 0 {
				panic(fmt.Sprintf("engine: no foreign refs to table %s dropped", fktn))
			}
		} else {
			foreignRefs = append(foreignRefs, fr)
		}
	}
	tt.foreignRefs = foreignRefs

	tt.ver += 1
	return tt
}

func (tt *TableType) dropForeignKey(con sql.Identifier, rtn sql.TableName) *TableType {
	var foreignKeys []foreignKey
	for _, fk := range tt.foreignKeys {
		if fk.refTable == rtn && fk.name == con {
			dropped := tt.dropTrigger(
				func(trig sql.Trigger) bool {
					if fkt, ok := trig.(*fkTrigger); ok && fkt.con == con && fkt.rtn == rtn {
						return true
					}
					return false
				})
			if dropped == 0 {
				panic(fmt.Sprintf("engine: no foreign keys to table %s dropped", rtn))
			}
		} else {
			foreignKeys = append(foreignKeys, fk)
		}
	}
	tt.foreignKeys = foreignKeys

	tt.ver += 1
	return tt
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

func encodeIntSlice(is []int) []int32 {
	i32s := make([]int32, 0, len(is))
	for _, i := range is {
		i32s = append(i32s, int32(i))
	}
	return i32s
}

func (tt *TableType) Encode() ([]byte, error) {
	cols := tt.Columns()
	colTypes := tt.ColumnTypes()
	colDefaults := tt.ColumnDefaults()

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
				Default:     expr.Encode(colDefaults[cdx].Default),
				DefaultExpr: colDefaults[cdx].DefaultExpr,
			})
	}

	md.Primary = encodeColumnKey(tt.primary)

	md.Indexes = make([]*IndexMetadata, 0, len(tt.indexes))
	for _, it := range tt.indexes {
		md.Indexes = append(md.Indexes,
			&IndexMetadata{
				Name:    it.Name.String(),
				Key:     encodeColumnKey(it.Key),
				Columns: encodeIntSlice(it.Columns),
				Unique:  it.Unique,
				Hidden:  it.Hidden,
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
		md.ForeignKeys = append(md.ForeignKeys,
			&ForeignKey{
				Name:       fk.name.String(),
				KeyColumns: encodeIntSlice(fk.keyCols),
				ReferenceTable: &TableName{
					Database: fk.refTable.Database.String(),
					Schema:   fk.refTable.Schema.String(),
					Table:    fk.refTable.Table.String(),
				},
				ReferenceIndex: fk.refIndex.String(),
				OnDelete:       int32(fk.onDelete),
				OnUpdate:       int32(fk.onUpdate),
			})
	}

	md.ForeignRefs = make([]*ForeignRef, 0, len(tt.foreignRefs))
	for _, fr := range tt.foreignRefs {
		md.ForeignRefs = append(md.ForeignRefs,
			&ForeignRef{
				Name: fr.name.String(),
				Table: &TableName{
					Database: fr.tn.Database.String(),
					Schema:   fr.tn.Schema.String(),
					Table:    fr.tn.Table.String(),
				},
			})
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

func decodeIntSlice(i32s []int32) []int {
	is := make([]int, 0, len(i32s))
	for _, i32 := range i32s {
		is = append(is, int(i32))
	}
	return is
}

func DecodeTableType(tn sql.TableName, buf []byte) (*TableType, error) {
	var md TableTypeMetadata
	err := proto.Unmarshal(buf, &md)
	if err != nil {
		return nil, fmt.Errorf("engine: table %s: %s", tn, err)
	}

	cols := make([]sql.Identifier, 0, len(md.Columns))
	colTypes := make([]sql.ColumnType, 0, len(md.Columns))
	colDefaults := make([]sql.ColumnDefault, 0, len(md.Columns))
	for cdx := range md.Columns {
		cols = append(cols, sql.QuotedID(md.Columns[cdx].Name))
		colTypes = append(colTypes,
			sql.ColumnType{
				Type:    sql.DataType(md.Columns[cdx].Type),
				Size:    md.Columns[cdx].Size,
				Fixed:   md.Columns[cdx].Fixed,
				NotNull: md.Columns[cdx].NotNull,
			})
		dflt, err := expr.Decode(md.Columns[cdx].Default)
		if err != nil {
			return nil, fmt.Errorf("engine: table %s: %s", tn, err)
		}
		colDefaults = append(colDefaults,
			sql.ColumnDefault{
				Default:     dflt,
				DefaultExpr: md.Columns[cdx].DefaultExpr,
			})
	}

	primary := decodeColumnKey(md.Primary)

	indexes := make([]sql.IndexType, 0, len(md.Indexes))
	for _, it := range md.Indexes {
		indexes = append(indexes,
			sql.IndexType{
				Name:    sql.QuotedID(it.Name),
				Key:     decodeColumnKey(it.Key),
				Columns: decodeIntSlice(it.Columns),
				Unique:  it.Unique,
				Hidden:  it.Hidden,
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
		foreignKeys = append(foreignKeys,
			foreignKey{
				name:    sql.QuotedID(fk.Name),
				keyCols: decodeIntSlice(fk.KeyColumns),
				refTable: sql.TableName{
					Database: sql.QuotedID(fk.ReferenceTable.Database),
					Schema:   sql.QuotedID(fk.ReferenceTable.Schema),
					Table:    sql.QuotedID(fk.ReferenceTable.Table),
				},
				refIndex: sql.QuotedID(fk.ReferenceIndex),
				onDelete: sql.RefAction(fk.OnDelete),
				onUpdate: sql.RefAction(fk.OnUpdate),
			})
	}

	foreignRefs := make([]foreignRef, 0, len(md.ForeignRefs))
	for _, fr := range md.ForeignRefs {
		foreignRefs = append(foreignRefs,
			foreignRef{
				name: sql.QuotedID(fr.Name),
				tn: sql.TableName{
					Database: sql.QuotedID(fr.Table.Database),
					Schema:   sql.QuotedID(fr.Table.Schema),
					Table:    sql.QuotedID(fr.Table.Table),
				},
			})
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
		colDefaults: colDefaults,
		primary:     primary,
		indexes:     indexes,
		constraints: constraints,
		checks:      checks,
		foreignKeys: foreignKeys,
		foreignRefs: foreignRefs,
		triggers:    triggers,
		events:      events,
	}, nil
}
