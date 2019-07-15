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
	ParseExpr() (sql.Expr, error)
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

func (p *parser) Parse() (stmt evaluate.Stmt, err error) {
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
	switch p.expectReserved(
		sql.ATTACH,
		sql.BEGIN,
		sql.COMMIT,
		sql.CREATE,
		sql.DELETE,
		sql.DETACH,
		sql.DROP,
		sql.INSERT,
		sql.ROLLBACK,
		sql.SELECT,
		sql.SET,
		sql.START,
		sql.UPDATE,
		sql.USE,
		sql.VALUES,
	) {
	case sql.BEGIN:
		// BEGIN
		return &misc.Begin{}
	case sql.COMMIT:
		// COMMIT
		return &misc.Commit{}
	case sql.CREATE:
		switch p.expectReserved(sql.DATABASE, sql.SCHEMA, sql.TABLE) {
		case sql.DATABASE:
			// CREATE DATABASE ...
			return p.parseCreateDatabase()
		case sql.SCHEMA:
			// CREATE SCHEMA ...
			return p.parseCreateSchema()
		case sql.TABLE:
			// CREATE TABLE ...
			return p.parseCreateTable()
		}
	case sql.DELETE:
		// DELETE FROM ...
		p.expectReserved(sql.FROM)
		return p.parseDelete()
	case sql.DROP:
		switch p.expectReserved(sql.DATABASE, sql.SCHEMA, sql.TABLE) {
		case sql.DATABASE:
			// DROP DATABASE ...
			return p.parseDropDatabase()
		case sql.SCHEMA:
			// DROP SCHEMA ...
			return p.parseDropSchema()
		case sql.TABLE:
			// DROP TABLE ...
			return p.parseDropTable()
		}
	case sql.INSERT:
		// INSERT INTO ...
		p.expectReserved(sql.INTO)
		return p.parseInsert()
	case sql.ROLLBACK:
		// ROLLBACK
		return &misc.Rollback{}
	case sql.SELECT:
		// SELECT ...
		return p.parseSelect()
	case sql.SET:
		// SET ...
		return p.parseSet()
	case sql.START:
		// START TRANSACTION
		p.expectReserved(sql.TRANSACTION)
		return &misc.Begin{}
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

func (p *parser) parseTableAlias() query.FromTableAlias {
	tn := p.parseTableName()
	return query.FromTableAlias{TableName: tn, Alias: p.parseAlias(false)}
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
	// CREATE TABLE ...
	var s datadef.CreateTable
	s.Table = p.parseTableName()

	if p.maybeToken(token.LParen) {
		p.parseCreateColumns(&s)
		return &s
	}

	p.error("CREATE TABLE ... AS ... not implemented")
	return nil
}

var types = map[sql.Identifier]sql.ColumnType{
	sql.BINARY:    {Type: sql.CharacterType, Fixed: true, Binary: true, Size: 1},
	sql.VARBINARY: {Type: sql.CharacterType, Fixed: false, Binary: true},
	sql.BLOB:      {Type: sql.CharacterType, Fixed: false, Binary: true, Size: sql.MaxColumnSize},
	sql.CHAR:      {Type: sql.CharacterType, Fixed: true, Size: 1},
	sql.VARCHAR:   {Type: sql.CharacterType, Fixed: false},
	sql.TEXT:      {Type: sql.CharacterType, Fixed: false, Size: sql.MaxColumnSize},
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

func (p *parser) parseCreateColumns(s *datadef.CreateTable) {
	/*
		CREATE TABLE [database '.'] table '(' column [',' ...] ')'
		column = name data_type [(DEFAULT expr) | (NOT NULL)]
		data_type =
			| BINARY ['(' length ')']
			| VARBINARY ['(' length ')']
			| BLOB ['(' length ')']
			| CHAR ['(' length ')']
			| VARCHAR ['(' length ')']
			| TEXT ['(' length ')']
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

		if typ == sql.DOUBLE {
			p.maybeIdentifier(sql.PRECISION)
		}

		if ct.Type == sql.CharacterType {
			if p.maybeToken(token.LParen) {
				ct.Size = uint32(p.expectInteger(0, sql.MaxColumnSize))
				p.expectTokens(token.RParen)
			}
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

func (p *parser) ParseExpr() (e sql.Expr, err error) {
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

func adjustPrecedence(e sql.Expr) sql.Expr {
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

func (p *parser) parseExpr() sql.Expr {
	return adjustPrecedence(p.parseSubExpr())
}

/*
expr = literal
    | '-' expr
    | NOT expr
    | '(' expr ')'
    | expr op expr
    | ref ['.' ref ...]
    | func '(' [expr [',' ...]] ')'
    | 'count' '(' '*' ')'
op = '+' '-' '*' '/' '%'
    | '=' '==' '!=' '<>' '<' '<=' '>' '>='
    | '<<' '>>' '&' '|'
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

func (p *parser) parseSubExpr() sql.Expr {
	var e sql.Expr
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
		} else {
			p.error(fmt.Sprintf("unexpected identifier %s", p.sctx.Identifier))
		}
	} else if r == token.String {
		e = expr.StringLiteral(p.sctx.String)
	} else if r == token.Integer {
		e = expr.Int64Literal(p.sctx.Integer)
	} else if r == token.Float {
		e = expr.Float64Literal(p.sctx.Float)
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
		// ( expr )
		e = &expr.Unary{Op: expr.NoOp, Expr: p.parseSubExpr()}
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

	return &expr.Binary{Op: op, Left: e, Right: p.parseSubExpr()}
}

func (p *parser) parseInsert() evaluate.Stmt {
	/*
		INSERT INTO [database '.'] table ['(' column [',' ...] ')']
			VALUES '(' expr | DEFAULT [',' ...] ')' [',' ...]
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
		var row []sql.Expr

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

func (p *parser) parseValues() *query.Values {
	/*
	   values = VALUES '(' expr [',' ...] ')' [',' ...]
	*/

	var s query.Values
	for {
		var row []sql.Expr

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
from-item = [database '.'] table [[AS] alias]
    | '(' select | values ')' [AS] alias ['(' column-alias [',' ...] ')']
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

func (p *parser) parseUpdate() evaluate.Stmt {
	// UPDATE [database '.'] table SET column '=' expr [',' ...] [WHERE expr]
	var s query.Update
	s.Table = p.parseTableName()
	p.expectReserved(sql.SET)

	for {
		var cu query.ColumnUpdate
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
