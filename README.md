# Maho
Maho is a partial implementation of a basic SQL server.

## Goals (Motivations)
* Learn [Go](https://golang.org/) and SQL.
* Mostly [PostgreSQL](https://www.postgresql.org/) compatible; see [sqltest](https://github.com/leftmike/sqltest) for SQL compatibility tests.

## Features
* Parse and execute SQL statements.
* Authentication for remote access using SSH.

## Missing
* Most SQL statements.
* Persistence.
* Indexes.
* Etc.

## Remote Access

To run an ssh server, maho needs a ssh host key; by default it uses `id_rsa` in the current
directory. Generate it if necessary.

```
ssh-keygen -t rsa -f id_rsa
```

Authorization of remote clients is done using an `authorized_keys` file and / or a list of
usernames and passwords. The list of usernames and passwords are specified in the config file;
the default is `maho.hcl` in the current directory.

```
// maho config
database = maho
engine = memrows
accounts = [
    {user: "michael", password: "password"}
    {user: "test", password: "secret"}
    {
        user: setup
        password: default
    }
]
```

Run maho: `maho -ssh=1`. And then in another terminal, connect using ssh:
`ssh -p 8241 test@localhost`; using the config above, the password will be `secret`.

## Supported SQL
```
ATTACH DATABASE database
    [WITH
        [PATH ['='] path]
    ]
```

```
BEGIN
```

```
COMMIT
```

```
CREATE DATABASE database
    [WITH
        [PATH ['='] path]
    ]
```

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
DETACH DATABASE database
```

```
DROP DATABASE [IF EXISTS] database
```

```
DROP TABLE [IF EXISTS] [database '.'] table [',' ...]
```

```
INSERT INTO [database '.'] table ['(' column [',' ...] ')']
	VALUES '(' <expr> | DEFAULT [',' ...] ')' [',' ...]
```

```
ROLLBACK
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
SET DATABASE (TO | '=') <database>
```

```
START TRANSACTION
```

```
UPDATE [database '.'] table SET column '=' <expr> [',' ...] [WHERE <expr>]
```

```
VALUES '(' <expr> [',' ...] ')' [',' ...]
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
