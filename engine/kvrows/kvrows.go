package kvrows

/*
database/name
database/version
database/opens

table/<id>/<primary-key>:<row>
table 1 is table of tables
table 2 is table of columns
*/

import (
	"errors"
	"fmt"
	"os"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/engine/kv"
	"github.com/leftmike/maho/sql"
)

var (
	databaseName    = "database/name"
	databaseVersion = "database/version"
	databaseOpens   = "database/opens"
	kvrowsVersion   = uint32(1)

	errNotImplemented = errors.New("kvrows: attach database not implemented")
)

type Engine struct {
	Engine kv.Engine
}

type database struct {
	name sql.Identifier
	db   kv.DB
}

type table struct {
}

type rows struct {
}

func (e Engine) AttachDatabase(svcs engine.Services, name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	_, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("kvrows: database not found or unavailable at %s", path)
	}

	db, err := e.Engine.Open(path)
	if err != nil {
		return nil, err
	}

	_ = db
	return nil, errNotImplemented
}

func (e Engine) CreateDatabase(svcs engine.Services, name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	_, err := os.Stat(path)
	if err == nil {
		return nil, fmt.Errorf("kvrows: existing file or directory at %s", path)
	}

	db, err := e.Engine.Open(path)
	if err != nil {
		return nil, err
	}

	wtx, err := db.WriteTx()
	if err != nil {
		return nil, err
	}
	defer wtx.Discard()

	err = setString(wtx, databaseName, name.String())
	if err != nil {
		return nil, err
	}
	err = setUInt32(wtx, databaseVersion, kvrowsVersion)
	if err != nil {
		return nil, err
	}
	err = setUInt32(wtx, databaseOpens, 1)
	if err != nil {
		return nil, err
	}

	err = wtx.Commit()
	if err != nil {
		return nil, err
	}
	return nil, errNotImplemented
}

func (kvdb *database) Message() string {
	return ""
}

func (kvdb *database) LookupTable(ses engine.Session, tctx interface{},
	tblname sql.Identifier) (engine.Table, error) {

	return nil, errNotImplemented
}

func (kvdb *database) CreateTable(ses engine.Session, tctx interface{}, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	return errNotImplemented
}

func (kvdb *database) DropTable(ses engine.Session, tctx interface{}, tblname sql.Identifier,
	exists bool) error {

	return errNotImplemented
}

func (kvdb *database) ListTables(ses engine.Session, tctx interface{}) ([]engine.TableEntry, error) {
	return nil, errNotImplemented
}

func (kvdb *database) Begin(lkr fatlock.Locker) interface{} {
	return errNotImplemented
}

func (kvdb *database) Commit(ses engine.Session, tctx interface{}) error {
	return errNotImplemented
}

func (kvdb *database) Rollback(tctx interface{}) error {
	return errNotImplemented
}

func (kvdb *database) NextStmt(tctx interface{}) {}

func (kvdb *database) Close() error {
	return errNotImplemented
}

func (kvt *table) Columns(ses engine.Session) []sql.Identifier {
	return nil
}

func (kvt *table) ColumnTypes(ses engine.Session) []sql.ColumnType {
	return nil
}

func (kvt *table) Rows(ses engine.Session) (engine.Rows, error) {
	return nil, errNotImplemented
}

func (kvt *table) Insert(ses engine.Session, row []sql.Value) error {
	return errNotImplemented
}

func (kvr *rows) Columns() []sql.Identifier {
	return nil
}

func (kvr *rows) Close() error {
	return errNotImplemented
}

func (kvr *rows) Next(ses engine.Session, dest []sql.Value) error {
	return errNotImplemented
}

func (kvr *rows) Delete(ses engine.Session) error {
	return errNotImplemented
}

func (kvr *rows) Update(ses engine.Session, updates []sql.ColumnUpdate) error {
	return errNotImplemented
}
