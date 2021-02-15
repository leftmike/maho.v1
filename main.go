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

- kvrows(badger): fails on sqltest/testdata/sql/drop.sql

- foreign key references
-- need read lock on referenced keys
-- use SELECT ... [FOR SHARE] ???
-- engine.transaction: add guardCount; func EnterGuard(), func LeaveGuard()
-- engine.Table interface: Rows, IndexRows: add guard flag to get current snapshot _and_
   guard against concurrent updates (read lock referenced rows)
-- storage/test: test Rows, IndexRows: guard = true
-- write lock happens as proposal; can read lock be separate for kvrows?
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
