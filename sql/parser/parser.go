package parser

import (
	"fmt"
	"io"
	"maho/sql"
	"maho/sql/scanner"
	"maho/sql/stmt"
	"math"
	"runtime"
)

type Parser struct {
	initialized bool
	scanner     scanner.Scanner
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
	if p.scan() == scanner.EOF {
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
	panic(fmt.Errorf("%s: %s", p.scanner.Position, msg))
}

func (p *Parser) scan() rune {
	if p.unscanned {
		p.unscanned = false
		return p.scanned
	}

	p.scanned = p.scanner.Scan()
	if p.scanned == scanner.Error {
		p.error(p.scanner.Error.Error())
	}
	return p.scanned
}

func (p *Parser) unscan() {
	p.unscanned = true
}

func (p *Parser) expectReserved(ids ...sql.Identifier) sql.Identifier {
	t := p.scan()
	if t == scanner.Reserved {
		for _, kw := range ids {
			if kw == p.scanner.Identifier {
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

	p.error(fmt.Sprintf("expected keyword: %s", msg))
	return 0
}

func (p *Parser) optionalReserved(ids ...sql.Identifier) bool {
	t := p.scan()
	if t == scanner.Reserved {
		for _, kw := range ids {
			if kw == p.scanner.Identifier {
				return true
			}
		}
	}

	p.unscan()
	return false
}

func (p *Parser) expectIdentifier(msg string) sql.Identifier {
	t := p.scan()
	if t != scanner.Identifier {
		p.error(msg)
	}
	return p.scanner.Identifier
}

func (p *Parser) maybeIdentifier(id sql.Identifier) bool {
	if p.scan() == scanner.Identifier && p.scanner.Identifier == id {
		return true
	}

	p.unscan()
	return false
}

func (p *Parser) expectRunes(runes ...rune) rune {
	t := p.scan()
	for _, r := range runes {
		if t == r {
			return r
		}
	}

	var msg string
	if len(runes) == 1 {
		msg = fmt.Sprintf("'%c'", runes[0])
	} else {
		for i, r := range runes {
			if i == len(runes)-1 {
				msg += ", or "
			} else if i > 0 {
				msg += ", "
			}
			msg += fmt.Sprintf("'%c'", r)
		}
	}

	p.error(fmt.Sprintf("expected: %s", msg))
	return 0
}

func (p *Parser) maybeRune(mr rune) bool {
	if p.scan() == mr {
		return true
	}
	p.unscan()
	return false
}

func (p *Parser) expectInteger(min, max int64) int64 {
	if p.scan() != scanner.Integer || p.scanner.Integer < min || p.scanner.Integer > max {
		p.error(fmt.Sprintf("expected a number between %d and %d inclusive", min, max))
	}

	return p.scanner.Integer
}

func (p *Parser) expectEOF() {
	if p.scan() != scanner.EOF {
		p.error(fmt.Sprintf("expected the end of the statement"))
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

func (p *Parser) parseCreateTable(tmp bool, not bool) stmt.Stmt {
	if tmp {
		p.error("temporary tables not implemented")
	}
	if not {
		p.error("IF NOT EXISTS not implemented")
	}

	// CREATE TABLE [database .] table ([<column>,] ...)
	var s stmt.CreateTable
	id := p.expectIdentifier("expected a database or a table")
	if p.maybeRune('.') {
		s.Table.Database = id
		s.Table.Table = p.expectIdentifier("expected a table")
	} else {
		s.Table.Table = id
	}

	if p.maybeRune('(') {
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
			p.error("expected a data type")
		}

		col := def
		col.Name = nam

		if typ == sql.VARCHAR || typ == sql.VARBINARY {
			p.expectRunes('(')
			col.Size = uint32(p.expectInteger(0, math.MaxUint32-1))
			p.expectRunes(')')
		} else {
			switch col.Type {
			case sql.CharacterType:
				if !p.maybeRune('(') {
					break
				}
				col.Size = uint32(p.expectInteger(0, math.MaxUint32-1))
				p.expectRunes(')')
			case sql.DoubleType:
				if !p.maybeRune('(') {
					break
				}
				col.Width = uint8(p.expectInteger(1, 255))
				p.expectRunes(',')
				col.Fraction = uint8(p.expectInteger(0, 30))
				p.expectRunes(')')
			case sql.IntegerType:
				if !p.maybeRune('(') {
					break
				}
				col.Width = uint8(p.expectInteger(1, 255))
				p.expectRunes(')')
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
				col.Default = p.parseExpression(false)
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

		r := p.expectRunes(',', ')')
		if r == ')' {
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

func (p *Parser) parseExpression(df bool) sql.Value {
	r := p.scan()
	if r == scanner.Reserved {
		if df && p.scanner.Identifier == sql.DEFAULT {
			return sql.Default{}
		} else if p.scanner.Identifier == sql.TRUE {
			return true
		} else if p.scanner.Identifier == sql.FALSE {
			return false
		} else if p.scanner.Identifier == sql.NULL {
			return nil
		} else {
			p.error(fmt.Sprintf("unexpected identifier: %s", p.scanner.Identifier))
		}
	} else if r == scanner.String {
		return p.scanner.String
	} else if r == scanner.Integer {
		return p.scanner.Integer
	} else if r == scanner.Double {
		return p.scanner.Double
	}

	if df {
		p.error("expected a string, a number, TRUE, FALSE, NULL or DEFAULT for each value")
	}
	p.error("expected a string, a number, TRUE, FALSE or NULL for each value")
	return nil
}

func (p *Parser) parseInsert() stmt.Stmt {
	/*
		INSERT INTO [database .] table [(column, ...)] VALUES (<expr> | DEFAULT, ...), ...
	*/

	var s stmt.InsertValues
	id := p.expectIdentifier("expected a database or a table")
	if p.maybeRune('.') {
		s.Table.Database = id
		s.Table.Table = p.expectIdentifier("expected a table")
	} else {
		s.Table.Table = id
	}

	if p.maybeRune('(') {
		for {
			nam := p.expectIdentifier("expected a column name")
			for _, c := range s.Columns {
				if c == nam {
					p.error(fmt.Sprintf("duplicate column name: %s", nam))
				}
			}
			s.Columns = append(s.Columns, nam)
			r := p.expectRunes(',', ')')
			if r == ')' {
				break
			}
		}
	}

	p.expectReserved(sql.VALUES)

	for {
		var row []sql.Value

		p.expectRunes('(')
		for {
			row = append(row, p.parseExpression(true))
			r := p.expectRunes(',', ')')
			if r == ')' {
				break
			}
		}

		s.Rows = append(s.Rows, row)

		if !p.maybeRune(',') {
			break
		}
	}

	return &s
}

func (p *Parser) parseSelect() stmt.Stmt {
	/*
		SELECT <select-expr> [, ...] FROM [database.]table [[ AS ]] name] [WHERE <expr>]
		<select-expr> = * | [table.]name [[ AS ] name] [, ...]
	*/

	var s stmt.Select
	if p.maybeRune('*') {
		p.expectReserved(sql.FROM)
	} else {
		for done := false; !done; {
			var sr stmt.SelectResult
			sr.Column = p.expectIdentifier("expected a table or a column")
			if p.maybeRune('.') {
				sr.Table = sr.Column
				sr.Column = p.expectIdentifier("expected a column")
			}
			sr.Alias = sr.Column
			if p.optionalReserved(sql.AS) {
				sr.Alias = p.expectIdentifier("expected an alias")
			}
			if p.optionalReserved(sql.FROM) {
				done = true
			} else if !p.maybeRune(',') {
				sr.Alias = p.expectIdentifier("expected an alias")
				p.expectRunes(',')
			}

			s.Results = append(s.Results, sr)
		}

	}

	s.Table.Table = p.expectIdentifier("expected a database or a table")
	if p.maybeRune('.') {
		s.Table.Database = s.Table.Table
		s.Table.Table = p.expectIdentifier("expected a table")
	}

	// [[ AS ]] name
	// maybe have a list of tables
	// [WHERE <expr>]

	return &s
}

func (p *Parser) parseUpdate() stmt.Stmt {
	p.error("UPDATE not implemented")
	return nil
}
