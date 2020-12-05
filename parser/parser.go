package parser

import (
	"fmt"
	"io"
	"runtime"
	"strconv"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/datadef"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/evaluate/misc"
	"github.com/leftmike/maho/evaluate/query"
	"github.com/leftmike/maho/parser/scanner"
	"github.com/leftmike/maho/parser/token"
	"github.com/leftmike/maho/sql"
)

type Parser interface {
	Parse() (evaluate.Stmt, error)
	ParseExpr() (expr.Expr, error)
}

const lookBackAmount = 3

type parser struct {
	scanner   scanner.Scanner
	lookBack  [lookBackAmount]scanner.ScanCtx
	sctx      *scanner.ScanCtx // = &lookBack[current]
	current   uint
	unscanned uint
	scanned   rune
	failed    bool
}

func NewParser(rr io.RuneReader, fn string) Parser {
	return newParser(rr, fn)
}

func newParser(rr io.RuneReader, fn string) *parser {
	var p parser
	p.scanner.Init(rr, fn)
	return &p
}

func (p *parser) Parse() (stmt evaluate.Stmt, err error) {
	if p.scan() == token.EOF {
		return nil, io.EOF
	}
	p.unscan()

	if p.failed {
		for {
			t := p.scan()
			if t == token.EOF {
				return nil, io.EOF
			}
			if t == token.EndOfStatement {
				break
			}
		}
		p.failed = false
	}

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			stmt = nil
			p.failed = (p.sctx.Token != token.EndOfStatement)
		}
	}()

	stmt = p.parseStmt()
	p.expectEndOfStatement()
	return
}

func (p *parser) error(msg string) {
	panic(fmt.Errorf("parser: %s: %s", p.sctx.Position, msg))
}

func (p *parser) scan() rune {
	p.current = (p.current + 1) % lookBackAmount
	p.sctx = &p.lookBack[p.current]

	if p.unscanned > 0 {
		p.unscanned -= 1
	} else {
		p.scanner.Scan(p.sctx)
		if p.sctx.Token == token.Error {
			p.error(p.sctx.Error.Error())
		}
	}
	return p.sctx.Token
}

func (p *parser) unscan() {
	p.unscanned += 1
	if p.unscanned > lookBackAmount {
		panic("parser: too much lookback")
	}
	if p.current == 0 {
		p.current = lookBackAmount - 1
	} else {
		p.current -= 1
	}
	p.sctx = &p.lookBack[p.current]
}

func (p *parser) got() string {
	switch p.sctx.Token {
	case token.EOF:
		return fmt.Sprintf("end of file")
	case token.EndOfStatement:
		return fmt.Sprintf("end of statement (;)")
	case token.Error:
		return fmt.Sprintf("error %s", p.sctx.Error.Error())
	case token.Identifier:
		return fmt.Sprintf("identifier %s", p.sctx.Identifier)
	case token.Reserved:
		return fmt.Sprintf("reserved identifier %s", p.sctx.Identifier)
	case token.String:
		return fmt.Sprintf("string %q", p.sctx.String)
	case token.Bytes:
		return fmt.Sprintf("bytes %v", p.sctx.String)
	case token.Integer:
		return fmt.Sprintf("integer %d", p.sctx.Integer)
	case token.Float:
		return fmt.Sprintf("float %f", p.sctx.Float)
	}

	return token.Format(p.sctx.Token)
}

func (p *parser) expectReserved(ids ...sql.Identifier) sql.Identifier {
	t := p.scan()
	if t == token.Reserved {
		for _, kw := range ids {
			if kw == p.sctx.Identifier {
				return kw
			}
		}
	}

	var msg string
	if len(ids) == 1 {
		msg = ids[0].String()
	} else {
		for i, kw := range ids {
			if i == len(ids)-1 {
				msg += ", or "
			} else if i > 0 {
				msg += ", "
			}
			msg += kw.String()
		}
	}

	p.error(fmt.Sprintf("expected keyword %s; got %s", msg, p.got()))
	return 0
}

func (p *parser) optionalReserved(ids ...sql.Identifier) bool {
	t := p.scan()
	if t == token.Reserved {
		for _, kw := range ids {
			if kw == p.sctx.Identifier {
				return true
			}
		}
	}

	p.unscan()
	return false
}

func (p *parser) expectIdentifier(msg string) sql.Identifier {
	t := p.scan()
	if t != token.Identifier {
		p.error(fmt.Sprintf("%s, got %s", msg, p.got()))
	}
	return p.sctx.Identifier
}

func (p *parser) maybeIdentifier(id sql.Identifier) bool {
	if p.scan() == token.Identifier && p.sctx.Identifier == id {
		return true
	}

	p.unscan()
	return false
}

func (p *parser) expectTokens(tokens ...rune) rune {
	t := p.scan()
	for _, r := range tokens {
		if t == r {
			return r
		}
	}

	var msg string
	if len(tokens) == 1 {
		msg = fmt.Sprintf("%s", token.Format(tokens[0]))
	} else {
		for i, r := range tokens {
			if i == len(tokens)-1 {
				msg += ", or "
			} else if i > 0 {
				msg += ", "
			}
			msg += fmt.Sprintf("%s", token.Format(r))
		}
	}

	p.error(fmt.Sprintf("expected %s, got %s", msg, p.got()))
	return 0
}

func (p *parser) maybeToken(mr rune) bool {
	if p.scan() == mr {
		return true
	}
	p.unscan()
	return false
}

func (p *parser) expectInteger(min, max int64) int64 {
	if p.scan() != token.Integer || p.sctx.Integer < min || p.sctx.Integer > max {
		p.error(fmt.Sprintf("expected a number between %d and %d inclusive, got %s", min, max,
			p.got()))
	}

	return p.sctx.Integer
}

func (p *parser) expectEndOfStatement() {
	r := p.scan()
	if r != token.EOF && r != token.EndOfStatement {
		p.error(fmt.Sprintf("expected the end of the statement, got %s", p.got()))
	}
}

func (p *parser) parseStmt() evaluate.Stmt {
	if p.maybeToken(token.EndOfStatement) {
		return nil
	}

	switch p.expectReserved(
		sql.BEGIN,
		sql.COMMIT,
		sql.COPY,
		sql.CREATE,
		sql.DELETE,
		sql.DETACH,
		sql.DROP,
		sql.EXECUTE,
		sql.EXPLAIN,
		sql.INSERT,
		sql.PREPARE,
		sql.ROLLBACK,
		sql.SELECT,
		sql.SET,
		sql.SHOW,
		sql.START,
		sql.UPDATE,
		sql.USE,
		sql.VALUES,
	) {
	case sql.BEGIN:
		// BEGIN
		return &evaluate.Begin{}
	case sql.COMMIT:
		// COMMIT
		return &misc.Commit{}
	case sql.COPY:
		// COPY
		return p.parseCopy()
	case sql.CREATE:
		switch p.expectReserved(sql.DATABASE, sql.INDEX, sql.SCHEMA, sql.TABLE, sql.UNIQUE) {
		case sql.DATABASE:
			// CREATE DATABASE ...
			return p.parseCreateDatabase()
		case sql.INDEX:
			// CREATE INDEX ...
			return p.parseCreateIndex(false)
		case sql.SCHEMA:
			// CREATE SCHEMA ...
			return p.parseCreateSchema()
		case sql.TABLE:
			// CREATE TABLE ...
			return p.parseCreateTable()
		case sql.UNIQUE:
			// CREATE UNIQUE INDEX ...
			p.expectReserved(sql.INDEX)
			return p.parseCreateIndex(true)
		}
	case sql.DELETE:
		// DELETE FROM ...
		p.expectReserved(sql.FROM)
		return p.parseDelete()
	case sql.DROP:
		switch p.expectReserved(sql.DATABASE, sql.INDEX, sql.SCHEMA, sql.TABLE) {
		case sql.DATABASE:
			// DROP DATABASE ...
			return p.parseDropDatabase()
		case sql.INDEX:
			// DROP INDEX ...
			return p.parseDropIndex()
		case sql.SCHEMA:
			// DROP SCHEMA ...
			return p.parseDropSchema()
		case sql.TABLE:
			// DROP TABLE ...
			return p.parseDropTable()
		}
	case sql.EXECUTE:
		return p.parseExecute()
	case sql.EXPLAIN:
		return p.parseExplain()
	case sql.INSERT:
		// INSERT INTO ...
		p.expectReserved(sql.INTO)
		return p.parseInsert()
	case sql.PREPARE:
		return p.parsePrepare()
	case sql.ROLLBACK:
		// ROLLBACK
		return &misc.Rollback{}
	case sql.SELECT:
		// SELECT ...
		return p.parseSelect()
	case sql.SET:
		// SET ...
		return p.parseSet()
	case sql.SHOW:
		// SHOW ...
		return p.parseShow()
	case sql.START:
		// START TRANSACTION
		p.expectReserved(sql.TRANSACTION)
		return &evaluate.Begin{}
	case sql.UPDATE:
		// UPDATE ...
		return p.parseUpdate()
	case sql.USE:
		// USE ...
		return p.parseUse()
	case sql.VALUES:
		// VALUES ...
		return p.parseValues()
	}

	return nil
}

func (p *parser) parseSchemaName() sql.SchemaName {
	var sn sql.SchemaName
	id := p.expectIdentifier("expected a database or a schema")
	if p.maybeToken(token.Dot) {
		sn.Database = id
		sn.Schema = p.expectIdentifier("expected a schema")
	} else {
		sn.Schema = id
	}
	return sn
}

func (p *parser) parseTableName() sql.TableName {
	var tn sql.TableName
	tn.Table = p.expectIdentifier("expected a database, schema, or table")
	if p.maybeToken(token.Dot) {
		tn.Schema = tn.Table
		tn.Table = p.expectIdentifier("expected a schema or table")
		if p.maybeToken(token.Dot) {
			tn.Database = tn.Schema
			tn.Schema = tn.Table
			tn.Table = p.expectIdentifier("expected a table")
		}
	}
	return tn
}

func (p *parser) parseAlias(required bool) sql.Identifier {
	if p.optionalReserved(sql.AS) {
		return p.expectIdentifier("expected an alias")
	}
	r := p.scan()
	if r == token.Identifier {
		return p.sctx.Identifier
	} else if required {
		p.error("an alias is required")
	}
	p.unscan()
	return 0
}

func (p *parser) parseTableAlias() query.FromItem {
	tn := p.parseTableName()
	if p.maybeToken(token.AtSign) {
		return &query.FromIndexAlias{
			TableName: tn,
			Index:     p.expectIdentifier("expected an index"),
			Alias:     p.parseAlias(false),
		}
	}
	return &query.FromTableAlias{TableName: tn, Alias: p.parseAlias(false)}
}

func (p *parser) parseColumnAliases() []sql.Identifier {
	if !p.maybeToken(token.LParen) {
		return nil
	}

	var cols []sql.Identifier
	for {
		cols = append(cols, p.expectIdentifier("expected a column alias"))
		if p.maybeToken(token.RParen) {
			break
		}
		p.expectTokens(token.Comma)
	}
	return cols
}

func (p *parser) parseCreateTable() evaluate.Stmt {
	// CREATE TABLE [IF NOT EXISTS] [[database '.'] schema '.'] table ...
	var s datadef.CreateTable

	if p.optionalReserved(sql.IF) {
		p.expectReserved(sql.NOT)
		p.expectReserved(sql.EXISTS)
		s.IfNotExists = true
	}

	s.Table = p.parseTableName()
	p.expectTokens(token.LParen)
	p.parseCreateDetails(&s)
	return &s
}

func (p *parser) parseKey(unique bool) datadef.IndexKey {
	key := datadef.IndexKey{
		Unique: unique,
	}

	p.expectTokens('(')
	for {
		nam := p.expectIdentifier("expected a column name")
		for _, col := range key.Columns {
			if col == nam {
				p.error(fmt.Sprintf("duplicate column name: %s", nam))
			}
		}
		key.Columns = append(key.Columns, nam)

		if p.optionalReserved(sql.ASC) {
			key.Reverse = append(key.Reverse, false)
		} else if p.optionalReserved(sql.DESC) {
			key.Reverse = append(key.Reverse, true)
		} else {
			key.Reverse = append(key.Reverse, false)
		}

		r := p.expectTokens(token.Comma, token.RParen)
		if r == token.RParen {
			break
		}
	}

	return key
}

func (p *parser) parseRefAction() sql.RefAction {
	switch p.expectReserved(sql.NO, sql.RESTRICT, sql.CASCADE, sql.SET) {
	case sql.NO:
		p.expectReserved(sql.ACTION)
		return sql.NoAction
	case sql.RESTRICT:
		return sql.Restrict
	case sql.CASCADE:
		return sql.Cascade
	case sql.SET:
		switch p.expectReserved(sql.NULL, sql.DEFAULT) {
		case sql.NULL:
			return sql.SetNull
		case sql.DEFAULT:
			return sql.SetDefault
		}
	}
	panic("never reached")
}

func (p *parser) parseOnActions(fk *datadef.ForeignKey) *datadef.ForeignKey {
	var onDelete, onUpdate bool
	for p.optionalReserved(sql.ON) {
		if p.expectReserved(sql.DELETE, sql.UPDATE) == sql.DELETE {
			if onDelete {
				p.error("ON DELETE may be specified once per foreign key")
			}
			fk.OnDelete = p.parseRefAction()
			onDelete = true
		} else {
			if onUpdate {
				p.error("ON UPDATE may be specified once per foreign key")
			}
			fk.OnUpdate = p.parseRefAction()
			onUpdate = true
		}
	}

	return fk
}

func (p *parser) parseCreateDetails(s *datadef.CreateTable) {
	/*
		CREATE TABLE [[database '.'] schema '.'] table
			'('	(column data_type [column_constraint] ...
				| [CONSTRAINT constraint] table_constraint) [',' ...] ')'
		table_constraint =
			  PRIMARY KEY key_columns
			| UNIQUE key_columns
			| CHECK '(' expr ')'
			| FOREIGN KEY columns REFERENCES [[database '.'] schema '.'] table [columns]
			  [ON DELETE referential_action] [ON UPDATE referential_action]
		key_columns = '(' column [ASC | DESC] [',' ...] ')'
		columns = '(' column [',' ...] ')'
		referential_action = NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
	*/

	for {
		var cn sql.Identifier
		if p.optionalReserved(sql.CONSTRAINT) {
			cn = p.expectIdentifier("expected a constraint name")
		}

		if p.optionalReserved(sql.PRIMARY) {
			p.expectReserved(sql.KEY)
			key := p.parseKey(true)
			p.addKeyConstraint(s, sql.PrimaryConstraint, makeKeyConstraintName(cn, key, "primary"),
				key)
		} else if p.optionalReserved(sql.UNIQUE) {
			key := p.parseKey(true)
			p.addKeyConstraint(s, sql.UniqueConstraint, makeKeyConstraintName(cn, key, "unique"),
				key)
		} else if p.optionalReserved(sql.CHECK) {
			p.expectTokens(token.LParen)
			s.Constraints = append(s.Constraints,
				datadef.Constraint{
					Type:   sql.CheckConstraint,
					Name:   p.makeConstraintName(cn, s, "check_"),
					ColNum: -1,
					Check:  p.parseExpr(),
				})
			p.expectTokens(token.RParen)
		} else if p.optionalReserved(sql.FOREIGN) {
			p.expectReserved(sql.KEY)

			var cols []sql.Identifier
			p.expectTokens(token.LParen)
			for {
				cols = append(cols, p.expectIdentifier("expected a column name"))
				if p.maybeToken(token.RParen) {
					break
				}
				p.expectTokens(token.Comma)
			}

			p.expectReserved(sql.REFERENCES)

			rtn := p.parseTableName()
			var refCols []sql.Identifier
			if p.maybeToken(token.LParen) {
				for {
					refCols = append(refCols, p.expectIdentifier("expected a column name"))
					if p.maybeToken(token.RParen) {
						break
					}
					p.expectTokens(token.Comma)
				}
			}

			s.ForeignKeys = append(s.ForeignKeys,
				p.parseOnActions(
					&datadef.ForeignKey{
						Name:     p.makeConstraintName(cn, s, "foreign_"),
						FKCols:   cols,
						RefTable: rtn,
						RefCols:  refCols,
					}))
		} else if cn != 0 {
			p.error("CONSTRAINT name specified without a constraint")
		} else {
			p.parseColumn(s)
		}

		r := p.expectTokens(token.Comma, token.RParen)
		if r == token.RParen {
			break
		}
	}
}

var types = map[sql.Identifier]sql.ColumnType{
	sql.BINARY:    {Type: sql.BytesType, Fixed: true, Size: 1},
	sql.VARBINARY: {Type: sql.BytesType, Fixed: false},
	sql.BLOB:      {Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize},
	sql.BYTEA:     {Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize},
	sql.BYTES:     {Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize},
	sql.CHAR:      {Type: sql.StringType, Fixed: true, Size: 1},
	sql.CHARACTER: {Type: sql.StringType, Fixed: true, Size: 1},
	sql.VARCHAR:   {Type: sql.StringType, Fixed: false},
	sql.TEXT:      {Type: sql.StringType, Fixed: false, Size: sql.MaxColumnSize},
	sql.BOOL:      {Type: sql.BooleanType, Size: 1},
	sql.BOOLEAN:   {Type: sql.BooleanType, Size: 1},
	sql.DOUBLE:    {Type: sql.FloatType, Size: 8},
	sql.REAL:      {Type: sql.FloatType, Size: 4},
	sql.SMALLINT:  {Type: sql.IntegerType, Size: 2},
	sql.INT2:      {Type: sql.IntegerType, Size: 2},
	sql.INT:       {Type: sql.IntegerType, Size: 4},
	sql.INTEGER:   {Type: sql.IntegerType, Size: 4},
	sql.INT4:      {Type: sql.IntegerType, Size: 4},
	sql.INT8:      {Type: sql.IntegerType, Size: 8},
	sql.BIGINT:    {Type: sql.IntegerType, Size: 8},
}

func (p *parser) parseColumnType() sql.ColumnType {
	/*
		data_type =
			  BINARY ['(' length ')']
			| VARBINARY ['(' length ')']
			| BLOB ['(' length ')']
			| BYTEA ['(' length ')']
			| BYTES ['(' length ')']
			| CHAR ['(' length ')']
			| CHARACTER ['(' length ')']
			| VARCHAR ['(' length ')']
			| TEXT ['(' length ')']
			| BOOL
			| BOOLEAN
			| DOUBLE [PRECISION]
			| REAL
			| SMALLINT
			| INT2
			| INT
			| INTEGER
			| INT4
			| INTEGER
			| BIGINT
			| INT8
	*/

	typ := p.expectIdentifier("expected a data type")
	def, found := types[typ]
	if !found {
		p.error(fmt.Sprintf("expected a data type, got %s", typ))
	}

	ct := def

	if typ == sql.DOUBLE {
		p.maybeIdentifier(sql.PRECISION)
	}

	if ct.Type == sql.StringType || ct.Type == sql.BytesType {
		if p.maybeToken(token.LParen) {
			ct.Size = uint32(p.expectInteger(0, sql.MaxColumnSize))
			p.expectTokens(token.RParen)
		}
	}

	return ct
}

func makeKeyConstraintName(cn sql.Identifier, key datadef.IndexKey, suffix string) sql.Identifier {
	if cn != 0 {
		return cn
	}

	var nam string
	for _, col := range key.Columns {
		nam += col.String() + "_"
	}

	return sql.ID(nam + suffix)
}

func (p *parser) addKeyConstraint(s *datadef.CreateTable, ct sql.ConstraintType,
	cn sql.Identifier, nkey datadef.IndexKey) {

	for _, c := range s.Constraints {
		if c.Name == cn {
			p.error(fmt.Sprintf("duplicate constraint name: %s", cn))
		}
		if c.Type == sql.PrimaryConstraint && ct == sql.PrimaryConstraint {
			p.error("only one primary key allowed")
		}
	}

	for _, c := range s.Constraints {
		if nkey.Equal(c.Key) {
			p.error("duplicate keys not allowed")
		}
	}

	s.Constraints = append(s.Constraints,
		datadef.Constraint{
			Type:   ct,
			Name:   cn,
			ColNum: -1,
			Key:    nkey,
		})
}

func (p *parser) addColumnConstraint(s *datadef.CreateTable, ct sql.ConstraintType,
	cn sql.Identifier, colNum int) {

	for _, c := range s.Constraints {
		if c.Name == cn {
			p.error(fmt.Sprintf("duplicate constraint name: %s", cn))
		} else if colNum == c.ColNum && ct == c.Type {
			p.error(fmt.Sprintf("duplicate %s constraint on %s", ct, s.Columns[colNum]))
		}
	}

	s.Constraints = append(s.Constraints,
		datadef.Constraint{
			Type:   ct,
			Name:   cn,
			ColNum: colNum,
		})
}

func duplicateName(cn sql.Identifier, s *datadef.CreateTable) bool {
	for _, c := range s.Constraints {
		if c.Name == cn {
			return true
		}
	}
	for _, fk := range s.ForeignKeys {
		if fk.Name == cn {
			return true
		}
	}
	return false
}

func (p *parser) makeConstraintName(cn sql.Identifier, s *datadef.CreateTable,
	base string) sql.Identifier {

	if cn == 0 {
		cnt := 1
		for {
			cn = sql.ID(fmt.Sprintf("%s%d", base, cnt))
			if !duplicateName(cn, s) {
				break
			}
			cnt += 1
		}
	} else if duplicateName(cn, s) {
		p.error(fmt.Sprintf("duplicate constraint name: %s", cn))
	}

	return cn
}

func (p *parser) parseColumn(s *datadef.CreateTable) {
	/*
		column data_type [[CONSTRAINT constraint] column_constraint]
		column_constraint =
			  DEFAULT expr
			| NOT NULL
			| PRIMARY KEY
			| UNIQUE
			| CHECK '(' expr ')'
			| REFERENCES [[database '.'] schema '.'] table ['(' column ')']
			  [ON DELETE referential_action] [ON UPDATE referential_action]
		referential_action = NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
	*/

	nam := p.expectIdentifier("expected a column name")
	for _, col := range s.Columns {
		if col == nam {
			p.error(fmt.Sprintf("duplicate column name: %s", nam))
		}
	}
	s.Columns = append(s.Columns, nam)

	ct := p.parseColumnType()

	var dflt expr.Expr
	for {
		var cn sql.Identifier
		if p.optionalReserved(sql.CONSTRAINT) {
			cn = p.expectIdentifier("expected a constraint name")
		}

		if p.optionalReserved(sql.DEFAULT) {
			if dflt != nil {
				p.error("DEFAULT specified more than once per column")
			}
			if cn != 0 {
				p.addColumnConstraint(s, sql.DefaultConstraint, cn, len(s.Columns)-1)
			}
			dflt = p.parseExpr()
		} else if p.optionalReserved(sql.NOT) {
			p.expectReserved(sql.NULL)

			if ct.NotNull {
				p.error("NOT NULL specified more than once per column")
			}
			if cn != 0 {
				p.addColumnConstraint(s, sql.NotNullConstraint, cn, len(s.Columns)-1)
			}
			ct.NotNull = true
		} else if p.optionalReserved(sql.PRIMARY) {
			p.expectReserved(sql.KEY)

			if cn == 0 {
				cn = sql.ID(nam.String() + "_primary")
			}
			p.addKeyConstraint(s, sql.PrimaryConstraint, cn,
				datadef.IndexKey{
					Unique:  true,
					Columns: []sql.Identifier{nam},
					Reverse: []bool{false},
				})
		} else if p.optionalReserved(sql.UNIQUE) {
			if cn == 0 {
				cn = sql.ID(nam.String() + "_unique")
			}
			p.addKeyConstraint(s, sql.UniqueConstraint, cn,
				datadef.IndexKey{
					Unique:  true,
					Columns: []sql.Identifier{nam},
					Reverse: []bool{false},
				})
		} else if p.optionalReserved(sql.CHECK) {
			p.expectTokens(token.LParen)
			s.Constraints = append(s.Constraints,
				datadef.Constraint{
					Type:   sql.CheckConstraint,
					Name:   p.makeConstraintName(cn, s, "check_"),
					ColNum: len(s.Columns) - 1,
					Check:  p.parseExpr(),
				})
			p.expectTokens(token.RParen)
		} else if p.optionalReserved(sql.REFERENCES) {
			rtn := p.parseTableName()
			var refCols []sql.Identifier
			if p.maybeToken(token.LParen) {
				refCols = []sql.Identifier{p.expectIdentifier("expected a column name")}
				p.expectTokens(token.RParen)
			}

			s.ForeignKeys = append(s.ForeignKeys,
				p.parseOnActions(
					&datadef.ForeignKey{
						Name:     p.makeConstraintName(cn, s, "foreign_"),
						FKCols:   []sql.Identifier{nam},
						RefTable: rtn,
						RefCols:  refCols,
					}))
		} else if cn != 0 {
			p.error("CONSTRAINT name specified without a constraint")
		} else {
			break
		}
	}

	s.ColumnTypes = append(s.ColumnTypes, ct)
	s.ColumnDefaults = append(s.ColumnDefaults, dflt)
}

func (p *parser) parseCreateIndex(unique bool) evaluate.Stmt {
	// CREATE [UNIQUE] INDEX [IF NOT EXISTS] index ON table
	//    [USING btree]
	//     '(' column [ASC | DESC] [, ...] ')'
	var s datadef.CreateIndex

	if p.optionalReserved(sql.IF) {
		p.expectReserved(sql.NOT)
		p.expectReserved(sql.EXISTS)
		s.IfNotExists = true
	}
	s.Index = p.expectIdentifier("expected an index")
	p.expectReserved(sql.ON)
	s.Table = p.parseTableName()

	if p.optionalReserved(sql.USING) {
		if p.expectIdentifier("expected btree") != sql.BTREE {
			p.error(fmt.Sprintf("expected btree, got %s", p.got()))
		}
	}

	s.Key = p.parseKey(unique)
	return &s
}

func (p *parser) parseDelete() evaluate.Stmt {
	// DELETE FROM [database '.'] table [WHERE expr]
	var s query.Delete
	s.Table = p.parseTableName()
	if p.optionalReserved(sql.WHERE) {
		s.Where = p.parseExpr()
	}

	return &s
}

func (p *parser) parseDropTable() evaluate.Stmt {
	// DROP TABLE [IF EXISTS] [database '.' ] table [',' ...]
	var s datadef.DropTable
	if p.optionalReserved(sql.IF) {
		p.expectReserved(sql.EXISTS)
		s.IfExists = true
	}

	s.Tables = []sql.TableName{p.parseTableName()}
	for p.maybeToken(token.Comma) {
		s.Tables = append(s.Tables, p.parseTableName())
	}
	return &s
}

func (p *parser) parseDropIndex() evaluate.Stmt {
	// DROP INDEX [IF EXISTS] index ON table
	var s datadef.DropIndex
	if p.optionalReserved(sql.IF) {
		p.expectReserved(sql.EXISTS)
		s.IfExists = true
	}
	s.Index = p.expectIdentifier("expected an index")
	p.expectReserved(sql.ON)
	s.Table = p.parseTableName()
	return &s
}

func (p *parser) optionalSubquery() (evaluate.Stmt, bool) {
	if p.optionalReserved(sql.SELECT) {
		// ( select )
		return p.parseSelect(), true
	} else if p.optionalReserved(sql.VALUES) {
		// ( values )
		return p.parseValues(), true
	} else if p.optionalReserved(sql.SHOW) {
		// ( show )
		return p.parseShow(), true
	} else if p.optionalReserved(sql.TABLE) {
		// ( TABLE [[database .] schema .] table )
		return &query.Select{
			From: &query.FromTableAlias{TableName: p.parseTableName()},
		}, true
	}
	return nil, false
}

func (p *parser) ParseExpr() (e expr.Expr, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			e = nil
		}
	}()

	e = p.parseExpr()
	return
}

func adjustPrecedence(e expr.Expr) expr.Expr {
	switch e := e.(type) {
	case *expr.Unary:
		e.Expr = adjustPrecedence(e.Expr)
		if e.Op == expr.NoOp {
			return e
		}

		// - {2 * 3}  --> {- 2} * 3
		if b, ok := e.Expr.(*expr.Binary); ok && b.Op.Precedence() < e.Op.Precedence() {
			e.Expr = b.Left
			b.Left = e
			return adjustPrecedence(b)
		}
	case *expr.Binary:
		e.Left = adjustPrecedence(e.Left)
		e.Right = adjustPrecedence(e.Right)

		// 1 * {2 + 3} --> {1 * 2} + 3
		if b, ok := e.Right.(*expr.Binary); ok && b.Op.Precedence() <= e.Op.Precedence() {
			e.Right = b.Left
			b.Left = e
			return adjustPrecedence(b)
		}

		// {1 + 2} * 3 --> 1 + {2 * 3}
		if b, ok := e.Left.(*expr.Binary); ok && b.Op.Precedence() < e.Op.Precedence() {
			e.Left = b.Right
			b.Right = e
			return adjustPrecedence(b)
		}
	case *expr.Call:
		for i, a := range e.Args {
			e.Args[i] = adjustPrecedence(a)
		}
	}

	return e
}

func (p *parser) parseExpr() expr.Expr {
	return adjustPrecedence(p.parseSubExpr())
}

/*
expr = literal
    | '-' expr
    | NOT expr
    | '(' expr | subquery ')'
    | expr op expr
    | ref ['.' ref ...]
    | param
    | func '(' [expr [',' ...]] ')'
    | COUNT '(' '*' ')'
    | EXISTS '(' subquery ')'
    | expr IN '(' subquery ')'
    | expr NOT IN '(' subquery ')'
    | expr op ANY '(' subquery ')'
    | expr op SOME '(' subquery ')'
    | expr op ALL '(' subquery ')'
op = '+' '-' '*' '/' '%'
    | '=' '==' '!=' '<>' '<' '<=' '>' '>='
    | '<<' '>>' '&' '|'
    | AND | OR
subquery = select | values | show
*/

var binaryOps = map[rune]struct {
	op     expr.Op
	isBool bool
}{
	token.Ampersand:      {expr.BinaryAndOp, false},
	token.Bar:            {expr.BinaryOrOp, false},
	token.BarBar:         {expr.ConcatOp, false},
	token.Equal:          {expr.EqualOp, true},
	token.EqualEqual:     {expr.EqualOp, true},
	token.BangEqual:      {expr.NotEqualOp, true},
	token.Greater:        {expr.GreaterThanOp, true},
	token.GreaterEqual:   {expr.GreaterEqualOp, true},
	token.GreaterGreater: {expr.RShiftOp, false},
	token.Less:           {expr.LessThanOp, true},
	token.LessEqual:      {expr.LessEqualOp, true},
	token.LessGreater:    {expr.NotEqualOp, true},
	token.LessLess:       {expr.LShiftOp, false},
	token.Minus:          {expr.SubtractOp, false},
	token.Percent:        {expr.ModuloOp, false},
	token.Plus:           {expr.AddOp, false},
	token.Slash:          {expr.DivideOp, false},
	token.Star:           {expr.MultiplyOp, false},
}

func (p *parser) optionalBinaryOp() (expr.Op, bool, bool) {
	r := p.scan()
	if bop, ok := binaryOps[r]; ok {
		return bop.op, true, bop.isBool
	} else if r == token.Reserved {
		switch p.sctx.Identifier {
		case sql.AND:
			return expr.AndOp, true, true
		case sql.OR:
			return expr.OrOp, true, true
		}
	}

	p.unscan()
	return 0, false, false
}

func (p *parser) parseSubExpr() expr.Expr {
	var e expr.Expr
	r := p.scan()
	if r == token.Reserved {
		if p.sctx.Identifier == sql.TRUE {
			e = expr.True()
		} else if p.sctx.Identifier == sql.FALSE {
			e = expr.False()
		} else if p.sctx.Identifier == sql.NULL {
			e = expr.Nil()
		} else if p.sctx.Identifier == sql.NOT {
			e = &expr.Unary{Op: expr.NotOp, Expr: p.parseSubExpr()}
		} else if p.sctx.Identifier == sql.EXISTS {
			// EXISTS ( subquery )
			e = expr.Subquery{Op: expr.Exists, Stmt: p.parseSubquery()}
		} else {
			p.error(fmt.Sprintf("unexpected identifier %s", p.sctx.Identifier))
		}
	} else if r == token.String {
		e = expr.StringLiteral(p.sctx.String)
	} else if r == token.Bytes {
		e = expr.BytesLiteral(p.sctx.Bytes)
	} else if r == token.Integer {
		e = expr.Int64Literal(p.sctx.Integer)
	} else if r == token.Float {
		e = expr.Float64Literal(p.sctx.Float)
	} else if r == token.Parameter {
		e = expr.Param{Num: int(p.sctx.Integer)}
	} else if r == token.Identifier {
		id := p.sctx.Identifier
		if p.maybeToken(token.LParen) {
			// func ( expr [,...] )
			c := &expr.Call{Name: id}
			if !p.maybeToken(token.RParen) {
				if id == sql.COUNT && p.maybeToken(token.Star) {
					p.expectTokens(token.RParen)
					c.Name = sql.COUNT_ALL
				} else {
					for {
						c.Args = append(c.Args, p.parseSubExpr())
						if p.maybeToken(token.RParen) {
							break
						}
						p.expectTokens(token.Comma)
					}
				}
			}
			e = c
		} else {
			// ref [. ref]
			ref := expr.Ref{p.sctx.Identifier}
			for p.maybeToken(token.Dot) {
				ref = append(ref, p.expectIdentifier("expected a reference"))
			}
			e = ref
		}
	} else if r == token.Minus {
		// - expr
		e = &expr.Unary{Op: expr.NegateOp, Expr: p.parseSubExpr()}
	} else if r == token.LParen {
		if s, ok := p.optionalSubquery(); ok {
			// ( subquery )
			e = expr.Subquery{Op: expr.Scalar, Stmt: s}
		} else {
			// ( expr )
			e = &expr.Unary{Op: expr.NoOp, Expr: p.parseSubExpr()}
		}
		if p.scan() != token.RParen {
			p.error(fmt.Sprintf("expected closing parenthesis, got %s", p.got()))
		}
	} else {
		p.error(fmt.Sprintf("expected an expression, got %s", p.got()))
	}

	op, ok, bop := p.optionalBinaryOp()
	if !ok {
		if p.optionalReserved(sql.IN, sql.NOT) {
			switch p.sctx.Identifier {
			case sql.IN:
				return expr.Subquery{Op: expr.Any, ExprOp: expr.EqualOp, Expr: e,
					Stmt: p.parseSubquery()}
			case sql.NOT:
				if p.optionalReserved(sql.IN) {
					return expr.Subquery{Op: expr.All, ExprOp: expr.NotEqualOp, Expr: e,
						Stmt: p.parseSubquery()}
				}
				p.unscan()
			}
		}

		return e
	}

	if p.optionalReserved(sql.ANY, sql.SOME, sql.ALL) {
		if !bop {
			p.error("expected boolean binary operator")
		}
		var subqueryOp expr.SubqueryOp
		if p.sctx.Identifier == sql.ALL {
			subqueryOp = expr.All
		} else {
			subqueryOp = expr.Any
		}
		return expr.Subquery{Op: subqueryOp, ExprOp: op, Expr: e, Stmt: p.parseSubquery()}
	}

	return &expr.Binary{Op: op, Left: e, Right: p.parseSubExpr()}
}

func (p *parser) parseSubquery() evaluate.Stmt {
	p.expectTokens(token.LParen)
	s, ok := p.optionalSubquery()
	if !ok {
		p.error("expected a subquery")
	}
	p.expectTokens(token.RParen)
	return s
}

func (p *parser) parseInsert() evaluate.Stmt {
	/*
		INSERT INTO [database '.'] table ['(' column [',' ...] ')']
			VALUES '(' (expr | DEFAULT) [',' ...] ')' [',' ...]
	*/

	var s query.InsertValues
	s.Table = p.parseTableName()

	if p.maybeToken(token.LParen) {
		for {
			nam := p.expectIdentifier("expected a column name")
			for _, c := range s.Columns {
				if c == nam {
					p.error(fmt.Sprintf("duplicate column name %s", nam))
				}
			}
			s.Columns = append(s.Columns, nam)
			r := p.expectTokens(token.Comma, token.RParen)
			if r == token.RParen {
				break
			}
		}
	}

	p.expectReserved(sql.VALUES)

	for {
		var row []expr.Expr

		p.expectTokens(token.LParen)
		for {
			r := p.scan()
			if r == token.Reserved && p.sctx.Identifier == sql.DEFAULT {
				row = append(row, nil)
			} else {
				p.unscan()
				row = append(row, p.parseExpr())
			}
			r = p.expectTokens(token.Comma, token.RParen)
			if r == token.RParen {
				break
			}
		}

		s.Rows = append(s.Rows, row)

		if !p.maybeToken(token.Comma) {
			break
		}
	}

	return &s
}

func (p *parser) parseCopy() evaluate.Stmt {
	/*
		COPY [[database '.'] schema '.'] table '(' column [',' ...] ')' FROM STDIN
			[DELIMITER delimiter]
	*/

	var s query.Copy
	s.Table = p.parseTableName()

	if p.maybeToken(token.LParen) {
		for {
			nam := p.expectIdentifier("expected a column name")
			for _, c := range s.Columns {
				if c == nam {
					p.error(fmt.Sprintf("duplicate column name %s", nam))
				}
			}
			s.Columns = append(s.Columns, nam)
			r := p.expectTokens(token.Comma, token.RParen)
			if r == token.RParen {
				break
			}
		}
	}

	p.expectReserved(sql.FROM)
	if p.expectIdentifier("expected STDIN") != sql.STDIN {
		p.error("expected STDIN")
	}

	s.Delimiter = '\t'
	if p.optionalReserved(sql.DELIMITER) {
		if p.scan() != token.String || len(p.sctx.String) != 1 {
			p.error("expected a one character string")
		}
		s.Delimiter = rune(p.sctx.String[0])
	}

	// Must be last because the scanner will skip to the end of the line before returning
	// the reader.
	s.From, s.FromLine = p.scanner.RuneReader()

	return &s
}

func (p *parser) parseValues() *query.Values {
	/*
	   values = VALUES '(' expr [',' ...] ')' [',' ...]
	*/

	var s query.Values
	for {
		var row []expr.Expr

		p.expectTokens(token.LParen)
		for {
			row = append(row, p.parseExpr())
			r := p.expectTokens(token.Comma, token.RParen)
			if r == token.RParen {
				break
			}
		}

		if s.Expressions != nil && len(s.Expressions[0]) != len(row) {
			p.error("values: all rows must have same number of columns")
		}
		s.Expressions = append(s.Expressions, row)

		if !p.maybeToken(token.Comma) {
			break
		}
	}

	return &s
}

/*
select =
    SELECT select-list
    [FROM from-item [',' ...]]
    [WHERE expr]
    [GROUP BY expr [',' ...]]
    [HAVING expr]
    [ORDER BY column [ASC | DESC] [',' ...]]
select-list = '*'
    | select-item [',' ...]
select-item = table '.' '*'
    | [table '.' ] column [[AS] column-alias]
    | expr [[AS] column-alias]
*/

func (p *parser) parseSelect() *query.Select {
	var s query.Select
	if !p.maybeToken(token.Star) {
		for {
			t := p.scan()
			if t == token.Identifier {
				tbl := p.sctx.Identifier
				if p.maybeToken(token.Dot) {
					if p.maybeToken(token.Star) {
						// table '.' *
						s.Results = append(s.Results, query.TableResult{Table: tbl})

						if !p.maybeToken(token.Comma) {
							break
						}
						continue
					}
					p.unscan()
				}
			}
			p.unscan()

			// expr [[ AS ] column-alias]
			s.Results = append(s.Results, query.ExprResult{
				Expr:  p.parseExpr(),
				Alias: p.parseAlias(false),
			})

			if !p.maybeToken(token.Comma) {
				break
			}
		}
	}

	if p.optionalReserved(sql.FROM) {
		s.From = p.parseFromList()
	}

	if p.optionalReserved(sql.WHERE) {
		s.Where = p.parseExpr()
	}

	if p.optionalReserved(sql.GROUP) {
		p.expectReserved(sql.BY)

		for {
			s.GroupBy = append(s.GroupBy, p.parseExpr())
			if !p.maybeToken(token.Comma) {
				break
			}
		}
	}

	if p.optionalReserved(sql.HAVING) {
		s.Having = p.parseExpr()
	}

	if p.optionalReserved(sql.ORDER) {
		p.expectReserved(sql.BY)

		for {
			var by query.OrderBy
			by.Expr = expr.Ref{p.expectIdentifier("expected a column")}
			if p.optionalReserved(sql.DESC) {
				by.Reverse = true
			} else {
				p.optionalReserved(sql.ASC)
			}
			s.OrderBy = append(s.OrderBy, by)
			if !p.maybeToken(token.Comma) {
				break
			}
		}
	}

	return &s
}

/*
from-item = [[database '.'] schema '.'] table ['@' index] [[AS] alias]
    | '(' select | values | show ')' [AS] alias ['(' column-alias [',' ...] ')']
    | '(' from-item [',' ...] ')'
    | from-item join-type from-item [ON expr | USING '(' join-column [',' ...] ')']
join-type = [INNER] JOIN
    | LEFT [OUTER] JOIN
    | RIGHT [OUTER] JOIN
    | FULL [OUTER] JOIN
    | CROSS JOIN
*/

func (p *parser) parseFromItem() query.FromItem {
	var fi query.FromItem
	if p.maybeToken(token.LParen) {
		if s, ok := p.optionalSubquery(); ok {
			// ( subquery )
			fi = p.parseFromStmt(s)
		} else {
			fi = p.parseFromList()
			p.expectTokens(token.RParen)
		}
	} else {
		fi = p.parseTableAlias()
	}

	jt := query.NoJoin
	if p.optionalReserved(sql.JOIN) {
		jt = query.Join
	} else if p.optionalReserved(sql.INNER) {
		p.expectReserved(sql.JOIN)
		jt = query.Join
	} else if p.optionalReserved(sql.LEFT) {
		p.optionalReserved(sql.OUTER)
		jt = query.LeftJoin
		p.expectReserved(sql.JOIN)
	} else if p.optionalReserved(sql.RIGHT) {
		p.optionalReserved(sql.OUTER)
		jt = query.RightJoin
		p.expectReserved(sql.JOIN)
	} else if p.optionalReserved(sql.FULL) {
		p.optionalReserved(sql.OUTER)
		jt = query.FullJoin
		p.expectReserved(sql.JOIN)
	} else if p.optionalReserved(sql.CROSS) {
		p.expectReserved(sql.JOIN)
		jt = query.CrossJoin
	}

	if jt == query.NoJoin {
		return fi
	}

	fj := query.FromJoin{Left: fi, Right: p.parseFromItem(), Type: jt}
	if p.optionalReserved(sql.ON) {
		fj.On = p.parseExpr()
	} else if p.optionalReserved(sql.USING) {
		p.expectTokens(token.LParen)
		for {
			nam := p.expectIdentifier("expected a column name")
			for _, c := range fj.Using {
				if c == nam {
					p.error(fmt.Sprintf("duplicate column %s", nam))
				}
			}
			fj.Using = append(fj.Using, nam)
			r := p.expectTokens(token.Comma, token.RParen)
			if r == token.RParen {
				break
			}
		}
	}

	if jt == query.Join || jt == query.LeftJoin || jt == query.RightJoin || jt == query.FullJoin {
		if (fj.On != nil && fj.Using != nil) || (fj.On == nil && fj.Using == nil) {
			p.error(fmt.Sprintf("%s must have one of ON or USING", jt))
		}
	}
	if jt == query.CrossJoin {
		if fj.On != nil || fj.Using != nil {
			p.error("CROSS JOIN may not have ON or USING")
		}
	}

	return fj
}

func (p *parser) parseFromList() query.FromItem {
	fi := p.parseFromItem()
	for p.maybeToken(token.Comma) {
		fi = query.FromJoin{Left: fi, Right: p.parseFromItem(), Type: query.CrossJoin}
	}
	return fi
}

func (p *parser) parseFromStmt(s evaluate.Stmt) query.FromItem {
	p.expectTokens(token.RParen)
	a := p.parseAlias(true)
	return query.FromStmt{Stmt: s, Alias: a, ColumnAliases: p.parseColumnAliases()}
}

func (p *parser) parseUpdate() evaluate.Stmt {
	// UPDATE [database '.'] table SET column '=' (expr | DEFAULT) [',' ...] [WHERE expr]
	var s query.Update
	s.Table = p.parseTableName()
	p.expectReserved(sql.SET)

	for {
		var cu query.ColumnUpdate
		cu.Column = p.expectIdentifier("expected a column name")
		p.expectTokens(token.Equal)
		r := p.scan()
		if r == token.Reserved && p.sctx.Identifier == sql.DEFAULT {
			cu.Expr = nil
		} else {
			p.unscan()
			cu.Expr = p.parseExpr()
		}
		s.ColumnUpdates = append(s.ColumnUpdates, cu)
		if !p.maybeToken(token.Comma) {
			break
		}
	}

	if p.optionalReserved(sql.WHERE) {
		s.Where = p.parseExpr()
	}

	return &s
}

func (p *parser) parseSet() evaluate.Stmt {
	// SET variable ( TO | '=' ) literal
	var s misc.Set

	if p.optionalReserved(sql.DATABASE) {
		s.Variable = sql.DATABASE
	} else if p.optionalReserved(sql.SCHEMA) {
		s.Variable = sql.SCHEMA
	} else {
		s.Variable = p.expectIdentifier("expected a config variable")
	}
	if !p.maybeToken(token.Equal) {
		p.expectReserved(sql.TO)
	}
	e := p.parseExpr()
	l, ok := e.(*expr.Literal)
	if !ok {
		p.error(fmt.Sprintf("expected a literal value, got %s", e.String()))
	}
	if sv, ok := l.Value.(sql.StringValue); ok {
		s.Value = string(sv)
	} else {
		s.Value = l.Value.String()
	}

	return &s
}

func (p *parser) parseShowFromTable() (sql.TableName, *expr.Binary) {
	tn := p.parseTableName()

	var schemaTest *expr.Binary
	if tn.Schema == 0 {
		schemaTest = &expr.Binary{
			Op:    expr.EqualOp,
			Left:  expr.Ref{sql.ID("schema_name")},
			Right: expr.Subquery{Op: expr.Scalar, Stmt: &misc.Show{Variable: sql.SCHEMA}},
		}
	} else {
		schemaTest = &expr.Binary{
			Op:    expr.EqualOp,
			Left:  expr.Ref{sql.ID("schema_name")},
			Right: expr.StringLiteral(tn.Schema.String()),
		}
	}

	return tn, schemaTest
}

func (p *parser) parseShow() evaluate.Stmt {
	// SHOW COLUMNS FROM [[database '.'] schema '.'] table
	// SHOW CONFIG
	// SHOW CONSTRAINTS FROM [[database '.'] schema '.'] table
	// SHOW DATABASE
	// SHOW DATABASES
	// SHOW SCHEMA
	// SHOW SCHEMAS [FROM database]
	// SHOW TABLES [FROM [database '.'] schema]
	// SHOW flag

	t := p.scan()
	if t != token.Reserved && t != token.Identifier {
		p.error("expected COLUMNS, CONSTRAINTS, DATABASE, DATABASES, SCHEMA, SCHEMAS, TABLES, " +
			"or a config variable")
	}

	switch p.sctx.Identifier {
	case sql.COLUMNS:
		p.expectReserved(sql.FROM)
		tn, schemaTest := p.parseShowFromTable()

		return &query.Select{
			From: &query.FromTableAlias{
				TableName: sql.TableName{
					Database: tn.Database,
					Schema:   sql.METADATA,
					Table:    sql.COLUMNS,
				},
			},
			Where: &expr.Binary{
				Op: expr.AndOp,
				Left: &expr.Binary{
					Op:    expr.EqualOp,
					Left:  expr.Ref{sql.ID("table_name")},
					Right: expr.StringLiteral(tn.Table.String()),
				},
				Right: schemaTest,
			},
		}
	case sql.CONFIG:
		return &query.Select{
			Results: []query.SelectResult{
				query.ExprResult{Expr: expr.Ref{sql.ID("name")}},
				query.ExprResult{Expr: expr.Ref{sql.ID("value")}},
				query.ExprResult{Expr: expr.Ref{sql.ID("by")}},
			},
			From: &query.FromTableAlias{
				TableName: sql.TableName{
					Database: sql.SYSTEM,
					Schema:   sql.INFO,
					Table:    sql.CONFIG,
				},
			},
			Where: &expr.Binary{
				Op:    expr.EqualOp,
				Left:  expr.Ref{sql.ID("hidden")},
				Right: expr.False(),
			},
		}
	case sql.CONSTRAINTS:
		p.expectReserved(sql.FROM)
		tn, schemaTest := p.parseShowFromTable()

		return &query.Select{
			From: &query.FromTableAlias{
				TableName: sql.TableName{
					Database: tn.Database,
					Schema:   sql.METADATA,
					Table:    sql.CONSTRAINTS,
				},
			},
			Where: &expr.Binary{
				Op: expr.AndOp,
				Left: &expr.Binary{
					Op:    expr.EqualOp,
					Left:  expr.Ref{sql.ID("table_name")},
					Right: expr.StringLiteral(tn.Table.String()),
				},
				Right: schemaTest,
			},
		}
	case sql.DATABASES:
		return &query.Select{
			From: &query.FromTableAlias{
				TableName: sql.TableName{
					Database: sql.SYSTEM,
					Schema:   sql.INFO,
					Table:    sql.DATABASES,
				},
			},
		}
	case sql.SCHEMAS:
		var db sql.Identifier
		if p.optionalReserved(sql.FROM) {
			db = p.expectIdentifier("expected a database")
		}
		return &query.Select{
			From: &query.FromTableAlias{
				TableName: sql.TableName{
					Database: db,
					Schema:   sql.METADATA,
					Table:    sql.SCHEMAS,
				},
			},
		}
	case sql.TABLES:
		var sn sql.SchemaName
		var where *expr.Binary

		if p.optionalReserved(sql.FROM) {
			sn = p.parseSchemaName()
			where = &expr.Binary{
				Op:    expr.EqualOp,
				Left:  expr.Ref{sql.ID("schema_name")},
				Right: expr.StringLiteral(sn.Schema.String()),
			}
		} else {
			where = &expr.Binary{
				Op:    expr.EqualOp,
				Left:  expr.Ref{sql.ID("schema_name")},
				Right: expr.Subquery{Op: expr.Scalar, Stmt: &misc.Show{Variable: sql.SCHEMA}},
			}
		}
		return &query.Select{
			From: &query.FromTableAlias{
				TableName: sql.TableName{
					Database: sn.Database,
					Schema:   sql.METADATA,
					Table:    sql.TABLES,
				},
			},
			Where: where,
		}
	default:
		return &misc.Show{Variable: p.sctx.Identifier}
	}
}

func (p *parser) parseUse() evaluate.Stmt {
	// USE database
	s := misc.Set{Variable: sql.DATABASE}

	e := p.parseExpr()
	l, ok := e.(*expr.Literal)
	if !ok {
		p.error(fmt.Sprintf("expected a literal value, got %s", e.String()))
	}
	if sv, ok := l.Value.(sql.StringValue); ok {
		s.Value = string(sv)
	} else {
		s.Value = l.Value.String()
	}

	return &s
}

func (p *parser) parseOptions() map[sql.Identifier]string {
	options := map[sql.Identifier]string{}
	for {
		if p.scan() != token.Identifier {
			p.unscan()
			break
		}

		opt := p.sctx.Identifier

		p.maybeToken(token.Equal)

		var val string
		switch p.scan() {
		case token.Identifier:
			val = p.sctx.Identifier.String()
		case token.String:
			val = p.sctx.String
		case token.Bytes:
			val = string(p.sctx.Bytes)
		case token.Integer:
			val = strconv.FormatInt(p.sctx.Integer, 10)
		case token.Float:
			val = strconv.FormatFloat(p.sctx.Float, 'g', -1, 64)
		default:
			p.error("expected a value")
		}

		options[opt] = val
	}
	if len(options) == 0 {
		p.error("expected options")
	}
	return options
}

func (p *parser) parseCreateDatabase() evaluate.Stmt {
	// CREATE DATABASE database
	//     [ WITH [ PATH [ '=' ] path ] ]
	var s datadef.CreateDatabase

	s.Database = p.expectIdentifier("expected a database")
	if p.optionalReserved(sql.WITH) {
		s.Options = p.parseOptions()
	}
	return &s
}

func (p *parser) parseDropDatabase() evaluate.Stmt {
	// DROP DATABASE [IF EXISTS] database
	var s datadef.DropDatabase

	if p.optionalReserved(sql.IF) {
		p.expectReserved(sql.EXISTS)
		s.IfExists = true
	}

	s.Database = p.expectIdentifier("expected a database")
	return &s
}

func (p *parser) parseCreateSchema() evaluate.Stmt {
	// CREATE SCHEMA [database '.'] schema
	var s datadef.CreateSchema

	s.Schema = p.parseSchemaName()
	return &s
}

func (p *parser) parseDropSchema() evaluate.Stmt {
	// DROP SCHEMA [IF EXISTS] [database '.'] schema
	var s datadef.DropSchema

	if p.optionalReserved(sql.IF) {
		p.expectReserved(sql.EXISTS)
		s.IfExists = true
	}

	s.Schema = p.parseSchemaName()
	return &s
}

func (p *parser) parseExplain() evaluate.Stmt {
	// EXPLAIN [VERBOSE] select

	var s misc.Explain
	s.Verbose = p.optionalReserved(sql.VERBOSE)
	switch p.expectReserved(sql.SELECT) {
	case sql.SELECT:
		// SELECT ...
		s.Stmt = p.parseSelect()
	}

	return s
}

func (p *parser) parsePrepare() evaluate.Stmt {
	// PREPARE name AS (delete | insert | select | update | values)

	var s misc.Prepare
	s.Name = p.expectIdentifier("expected a prepared statement")
	p.expectReserved(sql.AS)
	switch p.expectReserved(sql.DELETE, sql.INSERT, sql.SELECT, sql.UPDATE, sql.VALUES) {
	case sql.DELETE:
		// DELETE FROM ...
		p.expectReserved(sql.FROM)
		s.Stmt = p.parseDelete()
	case sql.INSERT:
		// INSERT INTO ...
		p.expectReserved(sql.INTO)
		s.Stmt = p.parseInsert()
	case sql.SELECT:
		// SELECT ...
		s.Stmt = p.parseSelect()
	case sql.UPDATE:
		// UPDATE ...
		s.Stmt = p.parseUpdate()
	case sql.VALUES:
		// VALUES ...
		s.Stmt = p.parseValues()
	}

	return &s
}

func (p *parser) parseExecute() evaluate.Stmt {
	// EXECUTE name ['(' expr [',' ...] ')']

	var s misc.Execute
	s.Name = p.expectIdentifier("expected a prepared statement")
	if p.maybeToken(token.LParen) {
		for {
			s.Params = append(s.Params, p.parseExpr())
			if p.maybeToken(token.RParen) {
				break
			}
			p.expectTokens(token.Comma)
		}
	}

	return &s
}
