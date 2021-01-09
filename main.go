package main

/*
To Do:
- fuzzing: parser.Parse

- add test for not seeing modified rows within a single SQL statement

- add type sql.ColumnValue interface{} and type BoolColumn []bool, type Int64Column []int64, etc
- Rows.NextColumns(ctx context.Context, destCols []sql.ColumnValue) error

- specify a subset of columns to return: Table.Rows(cols []int, ...)

- maho sql; use builtin client

- use etcd-io/etcd/raft

- make sure all Rows get properly closed

- storage/service might no longer be necessary?

- add protobuf column type

- select: ORDER BY: column(s) can have [table '.']

- tests with 1000s to 100000s of rows
-- generate rows
-- use sample databases
-- usda.sql: foreign keys

- kvrows
-- cleanup proposals
-- consider making Rows() incremental, maybe as blocks of rows

- rowcols
-- snapshot store and truncate WAL

- proto3 (postgres protocol)
-- use binary format for oid.T_bool, T_bytea, T_float4, T_float8, T_int2, T_int4, T_int8

- conditional expressions: CASE, COALESCE, NULLIF, GREATEST, LEAST

- EXPLAIN
-- group by fields: need to get name of compiled aggregator
-- include full column names
-- SELECT: track where columns come from, maybe as part of Plan
-- DELETE, INSERT, UPDATE, VALUES

- ALTER TABLE [IF EXISTS] [ONLY] table action [, ....]
- action = ADD [CONSTRAINT constraint] FOREIGN KEY ...
- action = DROP CONSTRAINT [IF EXISTS] constraint
- action = ALTER [COLUMN] column DROP DEFAULT
- action = ALTER [COLUMN] column DROP NOT NULL

- foreign key references
-- need read lock on referenced keys

- references from foreign keys (ForeignRef?)
-- use index on foreign key table if available

- constraints
-- DROP TABLE: CASCADE: to remove foreign key constraint of another table
-- drop CHECK constraint: use ALTER TABLE table DROP CONSTRAINT constraint
-- drop DEFAULT: use ALTER TABLE table ALTER COLUMN column DROP DEFAULT (or DROP CONSTRAINT)
-- drop FOREIGN KEY: use ALTER TABLE table DROP CONSTRAINT constraint
-- drop NOT NULL: use ALTER TABLE table ALTER COLUMN column DROP NOT NULL (or DROP CONSTRAINT)
*/

import (
	"os"

	"github.com/leftmike/maho/cmd"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
