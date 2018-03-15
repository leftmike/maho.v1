package sql

import (
	"strings"
)

type Identifier int

const MaxIdentifier = 128

const (
	BIGINT Identifier = iota + 1
	BINARY
	BLOB
	BOOL
	BOOLEAN
	CHAR
	COUNT
	COUNT_ALL
	DOUBLE
	INT
	INT2
	INT4
	INT8
	INTEGER
	PRECISION
	REAL
	SMALLINT
	TEXT
	VARBINARY
	VARCHAR
)

const (
	AND Identifier = -(iota + 1)
	AS
	ASC
	BY
	CREATE
	CROSS
	DEFAULT
	DELETE
	DESC
	DROP
	EXISTS
	FALSE
	FROM
	FULL
	GROUP
	HAVING
	IF
	INDEX
	INNER
	INSERT
	INTO
	JOIN
	LEFT
	NOT
	NULL
	ON
	OR
	ORDER
	OUTER
	RIGHT
	SELECT
	SET
	TABLE
	TRUE
	UNIQUE
	UPDATE
	USING
	VALUES
	WHERE
)

var knownIdentifiers = map[string]Identifier{
	"count":     COUNT,
	"count_all": COUNT_ALL,
}

var knownKeywords = map[string]struct {
	id       Identifier
	reserved bool
}{
	"AND":       {AND, true},
	"AS":        {AS, true},
	"ASC":       {ASC, true},
	"BY":        {BY, true},
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
	"DESC":      {DESC, true},
	"DOUBLE":    {DOUBLE, false},
	"DROP":      {DROP, true},
	"EXISTS":    {EXISTS, true},
	"FALSE":     {FALSE, true},
	"FROM":      {FROM, true},
	"FULL":      {FULL, true},
	"GROUP":     {GROUP, true},
	"HAVING":    {HAVING, true},
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
	"NOT":       {NOT, true},
	"NULL":      {NULL, true},
	"ON":        {ON, true},
	"OR":        {OR, true},
	"ORDER":     {ORDER, true},
	"OUTER":     {OUTER, true},
	"PRECISION": {PRECISION, false},
	"REAL":      {REAL, false},
	"RIGHT":     {RIGHT, true},
	"SELECT":    {SELECT, true},
	"SET":       {SET, true},
	"SMALLINT":  {SMALLINT, false},
	"TABLE":     {TABLE, true},
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

	s = strings.ToLower(s)
	if id, found := identifiers[s]; found {
		return id
	}
	lastIdentifier += 1
	identifiers[s] = lastIdentifier
	Names[lastIdentifier] = s
	return lastIdentifier
}

func UnquotedID(s string) Identifier {
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
