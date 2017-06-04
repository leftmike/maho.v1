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
	INT
	INTEGER
	MEDIUMINT
	SMALLINT
	TABLES
	TEXT
	TINYINT
	VARBINARY
	VARCHAR
)

const (
	CREATE = -(iota + 1)
	DELETE
	EXISTS
	FROM
	IF
	INDEX
	INSERT
	INTO
	NOT
	SELECT
	TABLE
	TEMP
	TEMPORARY
	UNIQUE
	UPDATE
	WHERE
)

var knownIdentifiers = map[string]Identifier{
	"basic":     BASIC,
	"columns":   COLUMNS,
	"databases": DATABASES,
	"engine":    ENGINE,
	"tables":    TABLES,
}

var knownKeywords = map[string]struct {
	id       Identifier
	reserved bool
}{
	"BIGINT":    {BIGINT, false},
	"BINARY":    {BINARY, false},
	"BLOB":      {BLOB, false},
	"BOOL":      {BOOL, false},
	"BOOLEAN":   {BOOLEAN, false},
	"CHAR":      {CHAR, false},
	"CREATE":    {CREATE, true},
	"DELETE":    {DELETE, true},
	"DOUBLE":    {DOUBLE, false},
	"EXISTS":    {EXISTS, true},
	"FROM":      {FROM, true},
	"IF":        {IF, true},
	"INDEX":     {INDEX, true},
	"INSERT":    {INSERT, true},
	"INT":       {INT, false},
	"INTEGER":   {INTEGER, false},
	"INTO":      {INTO, true},
	"MEDIUMINT": {MEDIUMINT, false},
	"NOT":       {NOT, true},
	"SELECT":    {SELECT, true},
	"SMALLINT":  {SMALLINT, false},
	"TABLE":     {TABLE, true},
	"TEMP":      {TEMP, true},
	"TEMPORARY": {TEMPORARY, true},
	"TEXT":      {TEXT, false},
	"TINYINT":   {TINYINT, false},
	"UNIQUE":    {UNIQUE, true},
	"UPDATE":    {UPDATE, true},
	"VARBINARY": {VARBINARY, false},
	"VARCHAR":   {VARCHAR, false},
	"WHERE":     {WHERE, true},
}

var (
	lastIdentifier = Identifier(9999)
	identifiers    = make(map[string]Identifier)
	keywords       = make(map[string]Identifier)
	names          = make(map[Identifier]string)
)

func Id(s string) Identifier {
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
	names[lastIdentifier] = s
	return lastIdentifier
}

func QuotedId(s string) Identifier {
	if len(s) > MaxIdentifier {
		s = s[:MaxIdentifier]
	}

	if id, found := identifiers[s]; found {
		return id
	}
	lastIdentifier += 1
	identifiers[s] = lastIdentifier
	names[lastIdentifier] = s
	return lastIdentifier
}

func (id Identifier) String() string {
	return names[id]
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
		names[id] = s
	}
	for s, n := range knownKeywords {
		keywords[s] = n.id
		names[n.id] = s
	}
}
