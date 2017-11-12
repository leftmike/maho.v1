package parser

import (
	"fmt"
	"io"
	"runtime"

	"maho/db"
	"maho/expr"
	"maho/parser/scanner"
	"maho/parser/token"
	"maho/query"
	"maho/sql"
	"maho/stmt"
)

type Parser interface {
	Parse() (stmt.Stmt, error)
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
}

func NewParser(rr io.RuneReader, fn string) Parser {
	return newParser(rr, fn)
}

func newParser(rr io.RuneReader, fn string) *parser {
	var p parser
	p.scanner.Init(rr, fn)
	return &p
}

func (p *parser) Parse() (stmt stmt.Stmt, err error) {
	if p.scan() == token.EOF {
		return nil, io.EOF
	}
	p.unscan()

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			stmt = nil
		}
	}()

	stmt = p.parseStmt()
	p.expectEndOfStatement()
	return
}

func (p *parser) error(msg string) {
	panic(fmt.Errorf("%s: %s", p.sctx.Position, msg))
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
	case token.Integer:
		return fmt.Sprintf("integer %d", p.sctx.Integer)
	case token.Double:
		return fmt.Sprintf("double %f", p.sctx.Double)
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

	p.error(fmt.Sprintf("expected keyword %s, got %s", msg, p.got()))
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

func (p *parser) parseStmt() stmt.Stmt {
	switch p.expectReserved(sql.CREATE, sql.DELETE, sql.DROP, sql.INSERT, sql.SELECT, sql.UPDATE,
		sql.VALUES) {
	case sql.CREATE:
		/*
			CREATE [UNIQUE] INDEX [IF NOT EXISTS]
			CREATE [TEMP | TEMPORARY] TABLE [IF NOT EXISTS]
		*/
		{
			var (
				tmp bool
				unq bool
				typ sql.Identifier
				not bool
			)

			if p.optionalReserved(sql.TEMP, sql.TEMPORARY) {
				typ = p.expectReserved(sql.TABLE)
				tmp = true
			} else if p.optionalReserved(sql.UNIQUE) {
				typ = p.expectReserved(sql.INDEX)
				unq = true
			} else {
				typ = p.expectReserved(sql.TABLE, sql.INDEX)
			}
			if p.optionalReserved(sql.IF) {
				p.expectReserved(sql.NOT)
				p.expectReserved(sql.EXISTS)
				not = true
			}

			switch typ {
			case sql.INDEX:
				return p.parseCreateIndex(unq, not)
			case sql.TABLE:
				return p.parseCreateTable(tmp, not)
			}
		}
	case sql.DELETE:
		/*
		   DELETE FROM
		*/
		p.expectReserved(sql.FROM)
		return p.parseDelete()
	case sql.DROP:
		/*
			DROP TABLE [database.]table [,...]
		*/
		p.expectReserved(sql.TABLE)
		return p.parseDropTable()
	case sql.INSERT:
		/*
		   INSERT INTO
		*/
		p.expectReserved(sql.INTO)
		return p.parseInsert()
	case sql.VALUES:
		/*
			VALUES
		*/
		return p.parseValues()
	case sql.SELECT:
		/*
			SELECT
		*/
		return p.parseSelect()
	case sql.UPDATE:
		/*
			UPDATE
		*/
		return p.parseUpdate()
	}

	return nil
}

func (p *parser) parseCreateIndex(unq bool, not bool) stmt.Stmt {
	p.error("CREATE INDEX not implemented")
	return nil
}

func (p *parser) parseTableName() stmt.TableName {
	var tbl stmt.TableName
	id := p.expectIdentifier("expected a database or a table")
	if p.maybeToken(token.Dot) {
		tbl.Database = id
		tbl.Table = p.expectIdentifier("expected a table")
	} else {
		tbl.Table = id
	}
	return tbl
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

func (p *parser) parseTableAlias() stmt.TableAlias {
	var ta stmt.TableAlias
	ta.TableName = p.parseTableName()
	ta.Alias = p.parseAlias(false)
	return ta
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

func (p *parser) parseCreateTable(tmp bool, not bool) stmt.Stmt {
	if tmp {
		p.error("temporary tables not implemented")
	}
	if not {
		p.error("IF NOT EXISTS not implemented")
	}

	// CREATE TABLE [database .] table ([<column>,] ...)
	var s stmt.CreateTable
	s.Table = p.parseTableName()

	if p.maybeToken(token.LParen) {
		p.parseCreateColumns(&s)
		return &s
	}

	p.error("CREATE TABLE ... AS ... not implemented")
	return nil
}

var types = map[sql.Identifier]db.ColumnType{
	sql.BINARY:    {Type: sql.CharacterType, Fixed: true, Binary: true, Size: 1},
	sql.VARBINARY: {Type: sql.CharacterType, Fixed: false, Binary: true},
	sql.BLOB:      {Type: sql.CharacterType, Fixed: false, Binary: true, Size: db.MaxColumnSize},
	sql.CHAR:      {Type: sql.CharacterType, Fixed: true, Size: 1},
	sql.VARCHAR:   {Type: sql.CharacterType, Fixed: false},
	sql.TEXT:      {Type: sql.CharacterType, Fixed: false, Size: db.MaxColumnSize},
	sql.BOOL:      {Type: sql.BooleanType, Size: 1},
	sql.BOOLEAN:   {Type: sql.BooleanType, Size: 1},
	sql.DOUBLE:    {Type: sql.DoubleType, Size: 8},
	sql.REAL:      {Type: sql.DoubleType, Size: 4},
	sql.SMALLINT:  {Type: sql.IntegerType, Size: 2},
	sql.INT2:      {Type: sql.IntegerType, Size: 2},
	sql.INT:       {Type: sql.IntegerType, Size: 4},
	sql.INTEGER:   {Type: sql.IntegerType, Size: 4},
	sql.INT4:      {Type: sql.IntegerType, Size: 4},
	sql.INT8:      {Type: sql.IntegerType, Size: 8},
	sql.BIGINT:    {Type: sql.IntegerType, Size: 8},
}

func (p *parser) parseCreateColumns(s *stmt.CreateTable) {
	/*
		CREATE TABLE [database .] table (<column> [, ...])
		<column> = name <data_type> [DEFAULT <expr>] | [NOT NULL]
		<data_type> =
			| BINARY [(length)]
			| VARBINARY [(length)]
			| BLOB [(length)]
			| CHAR [(length)] [BINARY]
			| VARCHAR [(length)] [BINARY]
			| TEXT [(length)] [BINARY]
			| BOOL
			| BOOLEAN
			| DOUBLE [PRECISION]
			| REAL
			| SMALLINT
			| INT2
			| INT
			| INT4
			| INTEGER
			| BIGINT
			| INT8
	*/

	for {
		nam := p.expectIdentifier("expected a column name")
		for _, col := range s.Columns {
			if col == nam {
				p.error(fmt.Sprintf("duplicate column name: %s", nam))
			}
		}
		s.Columns = append(s.Columns, nam)

		typ := p.expectIdentifier("expected a data type")
		def, found := types[typ]
		if !found {
			p.error(fmt.Sprintf("expected a data type, got %s", typ))
		}

		ct := def

		if typ == sql.VARCHAR || typ == sql.VARBINARY {
			p.expectTokens(token.LParen)
			ct.Size = uint32(p.expectInteger(0, db.MaxColumnSize))
			p.expectTokens(token.RParen)
		} else if typ == sql.DOUBLE {
			p.maybeIdentifier(sql.PRECISION)
		}

		if ct.Type == sql.CharacterType {
			if p.maybeToken(token.LParen) {
				ct.Size = uint32(p.expectInteger(0, db.MaxColumnSize))
				p.expectTokens(token.RParen)
			}
		}

		if ct.Type == sql.CharacterType && !ct.Binary && p.maybeIdentifier(sql.BINARY) {
			ct.Binary = true
		}

		for {
			if p.optionalReserved(sql.DEFAULT) {
				if ct.Default != nil {
					p.error("DEFAULT specified more than once per column")
				}
				ct.Default = p.parseExpr()
			} else if p.optionalReserved(sql.NOT) {
				p.expectReserved(sql.NULL)
				if ct.NotNull {
					p.error("NOT NULL specified more than once per column")
				}
				ct.NotNull = true
			} else {
				break
			}
		}

		s.ColumnTypes = append(s.ColumnTypes, ct)

		r := p.expectTokens(token.Comma, token.RParen)
		if r == token.RParen {
			break
		}
	}
}

func (p *parser) parseDelete() stmt.Stmt {
	// DELETE FROM [ database-name '.'] table-name [WHERE <expr>]
	var s stmt.Delete
	s.Table = p.parseTableName()
	if p.optionalReserved(sql.WHERE) {
		s.Where = p.parseExpr()
	}

	return &s
}

func (p *parser) parseDropTable() stmt.Stmt {
	// DROP TABLE [ IF EXISTS ] [ database-name '.' ] table-name [, ...]
	var s stmt.DropTable
	if p.optionalReserved(sql.IF) {
		p.expectReserved(sql.EXISTS)
		s.IfExists = true
	}

	s.Tables = []stmt.TableName{p.parseTableName()}
	for p.maybeToken(token.Comma) {
		s.Tables = append(s.Tables, p.parseTableName())
	}
	return &s
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

/*
<expr>:
      <literal>
    | - <expr>
    | NOT <expr>
    | ( <expr> )
    | <expr> <op> <expr>
    | <ref> [. <ref> ...]
    | <func> ( [<expr> [,...]] )
<op>:
      + - * / %
    | = == != <> < <= > >=
    | << >> & |
    | AND | OR
*/

var binaryOps = map[rune]expr.Op{
	token.Ampersand:      expr.BinaryAndOp,
	token.Bar:            expr.BinaryOrOp,
	token.BarBar:         expr.ConcatOp,
	token.Equal:          expr.EqualOp,
	token.EqualEqual:     expr.EqualOp,
	token.BangEqual:      expr.NotEqualOp,
	token.Greater:        expr.GreaterThanOp,
	token.GreaterEqual:   expr.GreaterEqualOp,
	token.GreaterGreater: expr.RShiftOp,
	token.Less:           expr.LessThanOp,
	token.LessEqual:      expr.LessEqualOp,
	token.LessGreater:    expr.NotEqualOp,
	token.LessLess:       expr.LShiftOp,
	token.Minus:          expr.SubtractOp,
	token.Percent:        expr.ModuloOp,
	token.Plus:           expr.AddOp,
	token.Slash:          expr.DivideOp,
	token.Star:           expr.MultiplyOp,
}

func (p *parser) parseExpr() expr.Expr {
	var e expr.Expr
	r := p.scan()
	if r == token.Reserved {
		if p.sctx.Identifier == sql.TRUE {
			e = &expr.Literal{true}
		} else if p.sctx.Identifier == sql.FALSE {
			e = &expr.Literal{false}
		} else if p.sctx.Identifier == sql.NULL {
			e = &expr.Literal{nil}
		} else if p.sctx.Identifier == sql.NOT {
			e = p.parseUnaryExpr(expr.NotOp)
		} else {
			p.error(fmt.Sprintf("unexpected identifier %s", p.sctx.Identifier))
		}
	} else if r == token.String {
		e = &expr.Literal{p.sctx.String}
	} else if r == token.Integer {
		e = &expr.Literal{p.sctx.Integer}
	} else if r == token.Double {
		e = &expr.Literal{p.sctx.Double}
	} else if r == token.Identifier {
		id := p.sctx.Identifier
		if p.maybeToken(token.LParen) {
			// <func> ( <expr> [,...] )
			c := &expr.Call{Name: id}
			if !p.maybeToken(token.RParen) {
				for {
					c.Args = append(c.Args, p.parseExpr())
					if p.maybeToken(token.RParen) {
						break
					}
					p.expectTokens(token.Comma)
				}
			}
			e = c
		} else {
			// <ref> [. <ref>]
			ref := expr.Ref{p.sctx.Identifier}
			for p.maybeToken(token.Dot) {
				ref = append(ref, p.expectIdentifier("expected a reference"))
			}
			e = ref
		}
	} else if r == token.Minus {
		// - <expr>
		e = p.parseUnaryExpr(expr.NegateOp)
	} else if r == token.LParen {
		// ( <expr> )
		e = &expr.Unary{expr.NoOp, p.parseExpr()}
		if p.scan() != token.RParen {
			p.error(fmt.Sprintf("expected closing parenthesis, got %s", p.got()))
		}
	} else {
		p.error(fmt.Sprintf("expected an expression, got %s", p.got()))
	}

	var op expr.Op
	r = p.scan()
	op, ok := binaryOps[r]
	if !ok {
		if r == token.Reserved && p.sctx.Identifier == sql.AND {
			op = expr.AndOp
		} else if r == token.Reserved && p.sctx.Identifier == sql.OR {
			op = expr.OrOp
		} else {
			p.unscan()
			return e
		}
	}

	e2 := p.parseExpr()
	if b2, ok := e2.(*expr.Binary); ok && b2.Op.Precedence() < op.Precedence() {
		b2.Left = &expr.Binary{op, e, b2.Left}
		e = b2
	} else {
		e = &expr.Binary{op, e, e2}
	}
	return e
}

func (p *parser) parseUnaryExpr(op expr.Op) expr.Expr {
	e := p.parseExpr()
	if b, ok := e.(*expr.Binary); ok && b.Op.Precedence() < op.Precedence() {
		for {
			if bl, ok := b.Left.(*expr.Binary); ok && bl.Op.Precedence() < op.Precedence() {
				b = bl
			} else {
				break
			}
		}

		b.Left = &expr.Unary{op, b.Left}
		return e
	}

	return &expr.Unary{op, e}
}

func (p *parser) parseInsert() stmt.Stmt {
	/*
		INSERT INTO [database .] table [(column, ...)] VALUES (<expr> | DEFAULT, ...), ...
	*/

	var s stmt.InsertValues
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

func (p *parser) parseValues() *stmt.Values {
	/*
	   VALUES '(' <expr> [',' ...] ')' [',' ...]
	*/

	var s stmt.Values
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
<select> = SELECT <select-list>
    [ FROM <from-item> [',' ...]]
    [ WHERE <expr> ]
<select-list> = '*'
    | <select-item> [',' ...]
<select-item> = table '.' '*'
    | [ table '.' ] column [[ AS ] column-alias ]
    | <expr> [[ AS ] column-alias ]
*/

func (p *parser) parseSelect() *stmt.Select {
	var s stmt.Select
	if !p.maybeToken(token.Star) {
		for {
			t := p.scan()
			if t == token.Identifier {
				tbl := p.sctx.Identifier
				if p.maybeToken(token.Dot) {
					if p.maybeToken(token.Star) {
						// table '.' *
						s.Results = append(s.Results, query.TableResult{tbl})

						if !p.maybeToken(token.Comma) {
							break
						}
						continue
					}
					p.unscan()
				}
			}
			p.unscan()

			e := p.parseExpr()
			a := p.parseAlias(false)

			if ref, ok := e.(expr.Ref); ok && (len(ref) == 1 || len(ref) == 2) {
				// [ table '.' ] column [[ AS ] column-alias]
				var tcr query.TableColumnResult
				if len(ref) == 1 {
					tcr.Column = ref[0]
				} else {
					tcr.Table = ref[0]
					tcr.Column = ref[1]
				}
				tcr.Alias = a
				s.Results = append(s.Results, tcr)
			} else {
				// <expr> [[ AS ] column-alias]
				s.Results = append(s.Results, query.ExprResult{e, a})
			}

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

	return &s
}

/*
<from-item> = [ database-name '.' ] table-name [[ AS ] alias ]
    | '(' <select> | <values> ')' [ AS ] alias ['(' column-alias [',' ...] ')']
    | '(' <from-item> [',' ...] ')'
    | <from-item> [ NATURAL ] <join-type> <from-item> [ ON <expr> | USING '(' join-column ',' ...]
<join-type> = [ INNER ] JOIN
    | LEFT [ OUTER ] JOIN
    | RIGHT [ OUTER ] JOIN
    | FULL [ OUTER ] JOIN
    | CROSS JOIN
*/

func (p *parser) parseFromItem() query.FromItem {
	var fi query.FromItem
	if p.maybeToken(token.LParen) {
		if p.optionalReserved(sql.SELECT) {
			ss := p.parseSelect()
			p.expectTokens(token.RParen)
			a := p.parseAlias(true)
			fi = query.FromSelect{Select: query.Select(*ss), Alias: a,
				ColumnAliases: p.parseColumnAliases()}
		} else if p.optionalReserved(sql.VALUES) {
			vs := p.parseValues()
			p.expectTokens(token.RParen)
			a := p.parseAlias(true)
			fi = query.FromValues{Values: query.Values(*vs), Alias: a,
				ColumnAliases: p.parseColumnAliases()}
		} else {
			fi = p.parseFromList()
			p.expectTokens(token.RParen)
		}
	} else {
		ta := p.parseTableAlias()
		fi = query.FromTableAlias{Database: ta.Database, Table: ta.Table, Alias: ta.Alias}
	}

	var nj bool
	if p.optionalReserved(sql.NATURAL) {
		nj = true
	}

	jt := query.NoJoin
	if p.optionalReserved(sql.JOIN) {
		jt = query.Join
	} else if p.optionalReserved(sql.INNER) {
		p.expectReserved(sql.JOIN)
		jt = query.InnerJoin
	} else if p.optionalReserved(sql.LEFT) {
		if p.optionalReserved(sql.OUTER) {
			jt = query.LeftOuterJoin
		} else {
			jt = query.LeftJoin
		}
		p.expectReserved(sql.JOIN)
	} else if p.optionalReserved(sql.RIGHT) {
		if p.optionalReserved(sql.OUTER) {
			jt = query.RightOuterJoin
		} else {
			jt = query.RightJoin
		}
		p.expectReserved(sql.JOIN)
	} else if p.optionalReserved(sql.FULL) {
		if p.optionalReserved(sql.OUTER) {
			jt = query.FullOuterJoin
		} else {
			jt = query.FullJoin
		}
		p.expectReserved(sql.JOIN)
	} else if p.optionalReserved(sql.CROSS) {
		p.expectReserved(sql.JOIN)
		jt = query.CrossJoin
	}

	if jt == query.NoJoin {
		return fi
	}

	fj := query.FromJoin{Left: fi, Right: p.parseFromItem(), Natural: nj, Type: jt}
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

	if jt == query.InnerJoin || jt == query.LeftOuterJoin || jt == query.RightOuterJoin ||
		jt == query.FullOuterJoin {
		if nj {
			if fj.On != nil || fj.Using != nil {
				p.error(fmt.Sprintf("%s must have one of NATURAL, ON, or USING", jt))
			}
		} else if fj.On != nil && fj.Using != nil {
			p.error(fmt.Sprintf("%s must have one of NATURAL, ON, or USING", jt))
		} else if fj.On == nil && fj.Using == nil {
			p.error(fmt.Sprintf("%s must have one of NATURAL, ON, or USING", jt))
		}
	}
	if jt == query.CrossJoin {
		if nj || fj.On != nil || fj.Using != nil {
			p.error("CROSS JOIN may not have NATURAL, ON, or USING")
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

func (p *parser) parseUpdate() stmt.Stmt {
	// UPDATE [ database-name '.'] table-name SET column '=' <expr> [',' ...] [WHERE <expr>]
	var s stmt.Update
	s.Table = p.parseTableName()
	p.expectReserved(sql.SET)

	for {
		var cu stmt.ColumnUpdate
		cu.Column = p.expectIdentifier("expected a column name")
		p.expectTokens(token.Equal)
		cu.Expr = p.parseExpr()
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
