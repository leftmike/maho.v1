package parser

import (
	"fmt"
	"io"
	"maho/sql"
	"maho/sql/stmt"
	"maho/sql/token"
	"math"
	"runtime"
)

type Parser struct {
	initialized bool
	scanner     scanner
	unscanned   bool
	scanned     rune
}

func (p *Parser) Init(rr io.RuneReader, fn string) {
	if p.initialized {
		panic("parser already initialized")
	}
	p.initialized = true

	p.scanner.Init(rr, fn)
}

func (p *Parser) Parse() (stmt stmt.Stmt, err error) {
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
	p.expectEOF()
	return
}

func (p *Parser) error(msg string) {
	panic(fmt.Errorf("%s: %s", p.scanner.pos, msg))
}

func (p *Parser) scan() rune {
	if p.unscanned {
		p.unscanned = false
		return p.scanned
	}

	p.scanned = p.scanner.Scan()
	if p.scanned == token.Error {
		p.error(p.scanner.error.Error())
	}
	return p.scanned
}

func (p *Parser) unscan() {
	p.unscanned = true
}

func (p *Parser) got() string {
	switch p.scanned {
	case token.EOF:
		return fmt.Sprintf("end of file")
	case token.Error:
		return fmt.Sprintf("error %s", p.scanner.error.Error())
	case token.Identifier:
		return fmt.Sprintf("identifier %s", p.scanner.identifier)
	case token.Reserved:
		return fmt.Sprintf("reserved identifier %s", p.scanner.identifier)
	case token.String:
		return fmt.Sprintf("string %q", p.scanner.string)
	case token.Integer:
		return fmt.Sprintf("integer %d", p.scanner.integer)
	case token.Double:
		return fmt.Sprintf("double %f", p.scanner.double)
	}

	return fmt.Sprintf("rune %c", p.scanned)
}

func (p *Parser) expectReserved(ids ...sql.Identifier) sql.Identifier {
	t := p.scan()
	if t == token.Reserved {
		for _, kw := range ids {
			if kw == p.scanner.identifier {
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

	p.error(fmt.Sprintf("expected keyword %s got %s", msg, p.got()))
	return 0
}

func (p *Parser) optionalReserved(ids ...sql.Identifier) bool {
	t := p.scan()
	if t == token.Reserved {
		for _, kw := range ids {
			if kw == p.scanner.identifier {
				return true
			}
		}
	}

	p.unscan()
	return false
}

func (p *Parser) expectIdentifier(msg string) sql.Identifier {
	t := p.scan()
	if t != token.Identifier {
		p.error(fmt.Sprintf("%s got %s", msg, p.got()))
	}
	return p.scanner.identifier
}

func (p *Parser) maybeIdentifier(id sql.Identifier) bool {
	if p.scan() == token.Identifier && p.scanner.identifier == id {
		return true
	}

	p.unscan()
	return false
}

func (p *Parser) expectTokens(tokens ...rune) rune {
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

	p.error(fmt.Sprintf("expected %s got %s", msg, p.got()))
	return 0
}

func (p *Parser) maybeToken(mr rune) bool {
	if p.scan() == mr {
		return true
	}
	p.unscan()
	return false
}

func (p *Parser) expectInteger(min, max int64) int64 {
	if p.scan() != token.Integer || p.scanner.integer < min || p.scanner.integer > max {
		p.error(fmt.Sprintf("expected a number between %d and %d inclusive got %s", min, max,
			p.got()))
	}

	return p.scanner.integer
}

func (p *Parser) expectEOF() {
	if p.scan() != token.EOF {
		p.error(fmt.Sprintf("expected the end of the statement got %s", p.got()))
	}
}

func (p *Parser) parseStmt() stmt.Stmt {
	switch p.expectReserved(sql.CREATE, sql.DELETE, sql.DROP, sql.INSERT, sql.SELECT, sql.UPDATE) {
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

func (p *Parser) parseCreateIndex(unq bool, not bool) stmt.Stmt {
	p.error("CREATE INDEX not implemented")
	return nil
}

func (p *Parser) parseTableName(tbl *stmt.TableName) {
	id := p.expectIdentifier("expected a database or a table")
	if p.maybeToken(token.Dot) {
		tbl.Database = id
		tbl.Table = p.expectIdentifier("expected a table")
	} else {
		tbl.Table = id
	}
}

func (p *Parser) parseAliasTableName(atbl *stmt.AliasTableName) {
	p.parseTableName(&atbl.TableName)
	if p.optionalReserved(sql.AS) {
		atbl.Alias = p.expectIdentifier("expected an alias")
	} else {
		r := p.scan()
		if r == token.Identifier {
			atbl.Alias = p.scanner.identifier
		} else {
			p.unscan()
			atbl.Alias = atbl.Table
		}
	}
}

func (p *Parser) parseCreateTable(tmp bool, not bool) stmt.Stmt {
	if tmp {
		p.error("temporary tables not implemented")
	}
	if not {
		p.error("IF NOT EXISTS not implemented")
	}

	// CREATE TABLE [database .] table ([<column>,] ...)
	var s stmt.CreateTable
	p.parseTableName(&s.Table)

	if p.maybeToken(token.LParen) {
		p.parseCreateColumns(&s)
		return &s
	}

	p.error("CREATE TABLE ... AS ... not implemented")
	return nil
}

var types = map[sql.Identifier]sql.Column{
	sql.BINARY:    {Type: sql.CharacterType, Fixed: true, Binary: true, Size: 1},
	sql.VARBINARY: {Type: sql.CharacterType, Fixed: false, Binary: true},
	sql.BLOB:      {Type: sql.CharacterType, Fixed: false, Binary: true, Size: math.MaxUint32 - 1},
	sql.CHAR:      {Type: sql.CharacterType, Fixed: true, Size: 1},
	sql.VARCHAR:   {Type: sql.CharacterType, Fixed: false},
	sql.TEXT:      {Type: sql.CharacterType, Fixed: false, Size: math.MaxUint32 - 1},
	sql.BOOL:      {Type: sql.BooleanType, Size: 1},
	sql.BOOLEAN:   {Type: sql.BooleanType, Size: 1},
	sql.DOUBLE:    {Type: sql.DoubleType, Size: 8, Width: 255, Fraction: 30},
	sql.TINYINT:   {Type: sql.IntegerType, Size: 1, Width: 255},
	sql.SMALLINT:  {Type: sql.IntegerType, Size: 2, Width: 255},
	sql.MEDIUMINT: {Type: sql.IntegerType, Size: 3, Width: 255},
	sql.INT:       {Type: sql.IntegerType, Size: 4, Width: 255},
	sql.INTEGER:   {Type: sql.IntegerType, Size: 4, Width: 255},
	sql.BIGINT:    {Type: sql.IntegerType, Size: 8, Width: 255},
}

func (p *Parser) parseCreateColumns(s *stmt.CreateTable) {
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
			| DOUBLE [(length, decimals)]
			| TINYINT [(length)]
			| SMALLINT [(length)]
			| MEDIUMINT [(length)]
			| INT [(length)]
			| INTEGER [(length)]
			| BIGINT [(length)]
	*/

	for {
		nam := p.expectIdentifier("expected a column name")
		for _, c := range s.Columns {
			if c.Name == nam {
				p.error(fmt.Sprintf("duplicate column name: %s", nam))
			}
		}

		typ := p.expectIdentifier("expected a data type")
		def, found := types[typ]
		if !found {
			p.error(fmt.Sprintf("expected a data type got %s", typ))
		}

		col := def
		col.Name = nam

		if typ == sql.VARCHAR || typ == sql.VARBINARY {
			p.expectTokens(token.LParen)
			col.Size = uint32(p.expectInteger(0, math.MaxUint32-1))
			p.expectTokens(token.RParen)
		} else {
			switch col.Type {
			case sql.CharacterType:
				if !p.maybeToken(token.LParen) {
					break
				}
				col.Size = uint32(p.expectInteger(0, math.MaxUint32-1))
				p.expectTokens(token.RParen)
			case sql.DoubleType:
				if !p.maybeToken(token.LParen) {
					break
				}
				col.Width = uint8(p.expectInteger(1, 255))
				p.expectTokens(token.Comma)
				col.Fraction = uint8(p.expectInteger(0, 30))
				p.expectTokens(token.RParen)
			case sql.IntegerType:
				if !p.maybeToken(token.LParen) {
					break
				}
				col.Width = uint8(p.expectInteger(1, 255))
				p.expectTokens(token.RParen)
			}
		}

		if col.Type == sql.CharacterType && !col.Binary && p.maybeIdentifier(sql.BINARY) {
			col.Binary = true
		}

		for {
			if p.optionalReserved(sql.DEFAULT) {
				if col.Default != nil {
					p.error("DEFAULT specified more than once per column")
				}
				col.Default = p.parseExpr()
			} else if p.optionalReserved(sql.NOT) {
				p.expectReserved(sql.NULL)
				if col.NotNull {
					p.error("NOT NULL specified more than once per column")
				}
				col.NotNull = true
			} else {
				break
			}
		}

		s.Columns = append(s.Columns, col)

		r := p.expectTokens(token.Comma, token.RParen)
		if r == token.RParen {
			break
		}
	}
}

func (p *Parser) parseDelete() stmt.Stmt {
	p.error("DELETE not implemented")
	return nil
}

func (p *Parser) parseDropTable() stmt.Stmt {
	p.error("DROP not implemented")
	return nil
}

func (p *Parser) ParseExpr() (e sql.Expr, err error) {
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

var binaryOps = map[rune]sql.Op{
	token.Ampersand:      sql.BinaryAndOp,
	token.Bar:            sql.BinaryOrOp,
	token.BarBar:         sql.ConcatOp,
	token.Equal:          sql.EqualOp,
	token.EqualEqual:     sql.EqualOp,
	token.BangEqual:      sql.NotEqualOp,
	token.Greater:        sql.GreaterThanOp,
	token.GreaterEqual:   sql.GreaterEqualOp,
	token.GreaterGreater: sql.RShiftOp,
	token.Less:           sql.LessThanOp,
	token.LessEqual:      sql.LessEqualOp,
	token.LessGreater:    sql.NotEqualOp,
	token.LessLess:       sql.LShiftOp,
	token.Minus:          sql.SubtractOp,
	token.Percent:        sql.ModuloOp,
	token.Plus:           sql.AddOp,
	token.Slash:          sql.DivideOp,
	token.Star:           sql.MultiplyOp,
}

func (p *Parser) parseExpr() sql.Expr {
	var e sql.Expr
	r := p.scan()
	if r == token.Reserved {
		if p.scanner.identifier == sql.TRUE {
			e = &sql.Literal{true}
		} else if p.scanner.identifier == sql.FALSE {
			e = &sql.Literal{false}
		} else if p.scanner.identifier == sql.NULL {
			e = &sql.Literal{nil}
		} else if p.scanner.identifier == sql.NOT {
			e = p.parseUnaryExpr(sql.NotOp)
		} else {
			p.error(fmt.Sprintf("unexpected identifier %s", p.scanner.identifier))
		}
	} else if r == token.String {
		e = &sql.Literal{p.scanner.string}
	} else if r == token.Integer {
		e = &sql.Literal{p.scanner.integer}
	} else if r == token.Double {
		e = &sql.Literal{p.scanner.double}
	} else if r == token.Identifier {
		if p.maybeToken(token.LParen) {
			// <func> ( <expr> [,...] )
			c := &sql.Call{Name: p.scanner.identifier}
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
			// <ref> [. <ref> ...]
			ref := sql.Ref{p.scanner.identifier}
			for p.maybeToken(token.Dot) {
				ref = append(ref, p.expectIdentifier("expected a reference"))
			}

			e = ref
		}
	} else if r == token.Minus {
		// - <expr>
		e = p.parseUnaryExpr(sql.NegateOp)
	} else if r == token.LParen {
		// ( <expr> )
		e = &sql.Unary{sql.NoOp, p.parseExpr()}
		if p.scan() != token.RParen {
			p.error(fmt.Sprintf("expected closing parenthesis got %s", p.got()))
		}
	} else {
		// XXX: need a better error message
		p.error(fmt.Sprintf(
			"expected a string, a number, TRUE, FALSE or NULL for each value got %s", p.got()))
	}

	var op sql.Op
	r = p.scan()
	op, ok := binaryOps[r]
	if !ok {
		if r == token.Reserved && p.scanner.identifier == sql.AND {
			op = sql.AndOp
		} else if r == token.Reserved && p.scanner.identifier == sql.OR {
			op = sql.OrOp
		} else {
			p.unscan()
			return e
		}
	}

	e2 := p.parseExpr()
	if b2, ok := e2.(*sql.Binary); ok && b2.Op.Precedence() < op.Precedence() {
		b2.Left = &sql.Binary{op, e, b2.Left}
		e = b2
	} else {
		e = &sql.Binary{op, e, e2}
	}
	return e
}

func (p *Parser) parseUnaryExpr(op sql.Op) sql.Expr {
	e := p.parseExpr()
	if b, ok := e.(*sql.Binary); ok && b.Op.Precedence() < op.Precedence() {
		for {
			if bl, ok := b.Left.(*sql.Binary); ok && bl.Op.Precedence() < op.Precedence() {
				b = bl
			} else {
				break
			}
		}

		b.Left = &sql.Unary{op, b.Left}
		return e
	}

	return &sql.Unary{op, e}
}

func (p *Parser) parseInsert() stmt.Stmt {
	/*
		INSERT INTO [database .] table [(column, ...)] VALUES (<expr> | DEFAULT, ...), ...
	*/

	var s stmt.InsertValues
	p.parseTableName(&s.Table)

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
			if r == token.Reserved && p.scanner.identifier == sql.DEFAULT {
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

func (p *Parser) parseSelect() stmt.Stmt {
	/*
		SELECT <select-expr> [, ...] FROM [database.]table [[ AS ] name] [WHERE <expr>]
		<select-expr> = * | [table.]name [[ AS ] name] [, ...]
	*/

	var s stmt.Select
	if p.maybeToken(token.Star) {
		p.expectReserved(sql.FROM)
	} else {
		for done := false; !done; {
			var sr stmt.SelectResult
			sr.Column = p.expectIdentifier("expected a table or a column")
			if p.maybeToken(token.Dot) {
				sr.Table = sr.Column
				sr.Column = p.expectIdentifier("expected a column")
			}

			sr.Alias = sr.Column
			if p.optionalReserved(sql.AS) {
				sr.Alias = p.expectIdentifier("expected an alias")
			}

			if p.optionalReserved(sql.FROM) {
				done = true
			} else if !p.maybeToken(token.Comma) {
				sr.Alias = p.expectIdentifier("expected an alias")
				p.expectTokens(token.Comma)
			}

			s.Results = append(s.Results, sr)
		}

	}

	p.parseAliasTableName(&s.Table)

	if p.optionalReserved(sql.WHERE) {
		s.Where = p.parseExpr()
	}

	return &s
}

func (p *Parser) parseUpdate() stmt.Stmt {
	p.error("UPDATE not implemented")
	return nil
}
