package sql

import (
	"strings"
)

type Identifier int

const MaxIdentifier = 128

const (
	BASIC Identifier = iota + 1
	BIGINT
	BINARY
	BLOB
	BOOL
	BOOLEAN
	CHAR
	COLUMNS
	DATABASES
	DOUBLE
	ENGINE
	IDENTIFIERS
	INT
	INT2
	INT4
	INT8
	INTEGER
	SMALLINT
	STORES
	TABLES
	TEXT
	VARBINARY
	VARCHAR
)

const (
	AND Identifier = -(iota + 1)
	AS
	CREATE
	CROSS
	DEFAULT
	DELETE
	DROP
	EXISTS
	FALSE
	FROM
	FULL
	IF
	INDEX
	INNER
	INSERT
	INTO
	JOIN
	LEFT
	NATURAL
	NOT
	NULL
	ON
	OR
	OUTER
	RIGHT
	SELECT
	SET
	TABLE
	TEMP
	TEMPORARY
	TRUE
	UNIQUE
	UPDATE
	USING
	VALUES
	WHERE
)

var knownIdentifiers = map[string]Identifier{
	"basic":       BASIC,
	"columns":     COLUMNS,
	"databases":   DATABASES,
	"engine":      ENGINE,
	"identifiers": IDENTIFIERS,
	"stores":      STORES,
	"tables":      TABLES,
}

var knownKeywords = map[string]struct {
	id       Identifier
	reserved bool
}{
	"AND":       {AND, true},
	"AS":        {AS, true},
	"BIGINT":    {BIGINT, false},
	"BINARY":    {BINARY, false},
	"BLOB":      {BLOB, false},
	"BOOL":      {BOOL, false},
	"BOOLEAN":   {BOOLEAN, false},
	"CHAR":      {CHAR, false},
	"CREATE":    {CREATE, true},
	"CROSS":     {CROSS, true},
	"DEFAULT":   {DEFAULT, true},
	"DELETE":    {DELETE, true},
	"DOUBLE":    {DOUBLE, false},
	"DROP":      {DROP, true},
	"EXISTS":    {EXISTS, true},
	"FALSE":     {FALSE, true},
	"FROM":      {FROM, true},
	"FULL":      {FULL, true},
	"IF":        {IF, true},
	"INDEX":     {INDEX, true},
	"INNER":     {INNER, true},
	"INSERT":    {INSERT, true},
	"INT":       {INT, false},
	"INT2":      {INT2, false},
	"INT4":      {INT4, false},
	"INT8":      {INT8, false},
	"INTEGER":   {INTEGER, false},
	"INTO":      {INTO, true},
	"JOIN":      {JOIN, true},
	"LEFT":      {LEFT, true},
	"NATURAL":   {NATURAL, true},
	"NOT":       {NOT, true},
	"NULL":      {NULL, true},
	"ON":        {ON, true},
	"OR":        {OR, true},
	"OUTER":     {OUTER, true},
	"RIGHT":     {RIGHT, true},
	"SELECT":    {SELECT, true},
	"SET":       {SET, true},
	"SMALLINT":  {SMALLINT, false},
	"TABLE":     {TABLE, true},
	"TEMP":      {TEMP, true},
	"TEMPORARY": {TEMPORARY, true},
	"TEXT":      {TEXT, false},
	"TRUE":      {TRUE, true},
	"UNIQUE":    {UNIQUE, true},
	"UPDATE":    {UPDATE, true},
	"USING":     {USING, true},
	"VALUES":    {VALUES, true},
	"VARBINARY": {VARBINARY, false},
	"VARCHAR":   {VARCHAR, false},
	"WHERE":     {WHERE, true},
}

var (
	lastIdentifier = Identifier(0)
	identifiers    = make(map[string]Identifier)
	keywords       = make(map[string]Identifier)
	Names          = make(map[Identifier]string)
)

func ID(s string) Identifier {
	if len(s) > MaxIdentifier {
		s = s[:MaxIdentifier]
	}

	if id, found := keywords[strings.ToUpper(s)]; found {
		return id
	}

	s = strings.ToLower(s)
	if id, found := identifiers[s]; found {
		return id
	}
	lastIdentifier += 1
	identifiers[s] = lastIdentifier
	Names[lastIdentifier] = s
	return lastIdentifier
}

func QuotedID(s string) Identifier {
	if len(s) > MaxIdentifier {
		s = s[:MaxIdentifier]
	}

	if id, found := identifiers[s]; found {
		return id
	}
	lastIdentifier += 1
	identifiers[s] = lastIdentifier
	Names[lastIdentifier] = s
	return lastIdentifier
}

func (id Identifier) String() string {
	return Names[id]
}

func (id Identifier) IsReserved() bool {
	if id < 0 {
		return true
	}
	return false
}

func init() {
	for s, id := range knownIdentifiers {
		identifiers[strings.ToLower(s)] = id
		Names[id] = s
		if id > lastIdentifier {
			lastIdentifier = id
		}
	}
	for s, n := range knownKeywords {
		keywords[s] = n.id
		Names[n.id] = s
		if n.id > lastIdentifier {
			lastIdentifier = n.id
		}
	}
}
