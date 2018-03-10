# Maho
Maho is a partial implementation of a basic SQL server.

# Goals (Motivations)
* Learn [Go](https://golang.org/) and SQL.
* Mostly [PostgreSQL](https://www.postgresql.org/) compatible; see [sqltest](https://github.com/leftmike/sqltest) for SQL compatibility tests.

# Features
* Parse and execute SQL statements.

# Missing
* Most SQL statements.
* An actual server.
* Persistence.
* Indexes.
* Transactions.
* Etc.

# Supported SQL
```
CREATE TABLE [database '.'] table '(' <column> [',' ...] ')'
<column> = name <data_type> [(DEFAULT <expr>) | (NOT NULL)]
<data_type> =
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
```

```
DELETE FROM [database '.'] table [WHERE <expr>]
```

```
DROP TABLE [IF EXISTS] [database '.'] table [',' ...]
```

```
INSERT INTO [database '.'] table ['(' column [',' ...] ')']
	VALUES '(' <expr> | DEFAULT [',' ...] ')' [',' ...]
```

```
VALUES '(' <expr> [',' ...] ')' [',' ...]
```

```
SELECT <select-list>
    [FROM <from-item> [',' ...]]
    [WHERE <expr>]
    [GROUP BY <expr> [',' ...]]
    [HAVING <expr>]
    [ORDER BY column [ASC | DESC] [',' ...]]
<select-list> = '*'
    | <select-item> [',' ...]
<select-item> = table '.' '*'
    | [table '.' ] column [[AS] column-alias]
    | <expr> [[AS] column-alias]
<from-item> = [database '.'] table [[AS] alias]
    | '(' <select> | <values> ')' [AS] alias ['(' column-alias [',' ...] ')']
    | '(' <from-item> [',' ...] ')'
    | <from-item> <join-type> <from-item> [ON <expr> | USING '(' join-column [',' ...] ')']
<join-type> = [INNER] JOIN
    | LEFT [OUTER] JOIN
    | RIGHT [OUTER] JOIN
    | FULL [OUTER] JOIN
    | CROSS JOIN
```

```
UPDATE [database '.'] table SET column '=' <expr> [',' ...] [WHERE <expr>]
```

```
<expr> = <literal>
    | '-' <expr>
    | NOT <expr>
    | '(' <expr> ')'
    | <expr> <op> <expr>
    | <ref> ['.' <ref> ...]
    | <func> '(' [<expr> [',' ...]] ')'
<op> = '+' '-' '*' '/' '%'
    | '=' '==' '!=' '<>' '<' '<=' '>' '>='
    | '<<' '>>' '&' '|'
    | AND | OR
```

Scalar Functions:
* `abs(<number>)`
* `concat(<arg1>, <arg2>, ...)`

Aggregate Functions:
* `avg(<number>)`
* `count(<arg>)` or `count(*)`
* `max(<number>)`
* `min(<number>)`
* `sum(<number>)`
