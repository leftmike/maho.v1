package mvcc

/*
To Do:
- rename mvcc/layout.go --> page.go
- probably move mvcc back to engine/ or move basic to /
- mvcc: pageSize is a config option

Notes:
- some shared infrastructure (that is also pluggable): (fat) lock manager; page cache (manager)
- each database has directory of tables: name and physical location
- table metadata is stored with the table and fixed at creation time
- each table has an id and a location; id is fixed at create time; location is physical location
of the table and it can change
- write a tool to dump a database, maybe a page at a time

Database.LookupTable(tblname sql.Identifier) (TableID, PageNum, err)

package page

type Page struct {
    PinCount int
    PageNum PageNum
    Bytes []byte
    Mutex sync.RWMutex
}

type Cache interface {
    NewPage(pn engine.PageNum) (Page, error)
    PinPage(ctx context.Context, pn engine.PageNum) (Page, error)
    UnpinPage(p Page) error
    DirtyPage(p Page) error
}

- keep track of how many times an engine has been started: engine_starts
- live transactions are identified by (engine_starts, transaction_slot)
- version: updated for every committed transaction; 63bits
- the other 63bits are live transactions
- pending changes are written to the database; distinguish abandoned changes based on engine_starts
- table has metadata and an array of pointers to data pages
- each data page has one or more rows
- each row is locked for write
- use linux pread system call (combines seek and read)

- as a transaction proceeds: update pages with pending changes, contruct wal records
- count number of pending changes on a page and store in the page header
- when a page is read, if it had pending changes, then cleanup any abandoned ones
- at commit:
    (1) get next version number
    (2) update wal records with the version number (or add to wal records)
    (3) flush wal records to disk
    (4) change pending updates to versioned updates on pages
    (5) mark version number as committed -- may increase highest snapshot version

- a row is a linked list of records: complete, delta, deleted(?)
- each record has a version, a pointer to a previous version, a type, and zero or more columns
- rows don't move

type Record struct {
    Version uint64
    Previous uint64 // record pointer to previous version of record
    Type byte // COMPLETE, DELTA, DELETED, FRAGMENT
    Next uint64 // record pointer to next fragment
    Values []byte
}

- values is a list of columns which are not null, in order

- inserted record: version = ver_inserted, previous = 0, offset, length = columns
- updated record:
    (1) fat-lock row
    (2) create delta record w/ old versions of updated columns: version = ver_current,
        previous = prev_current, offset, length = changed columns
    (3) lock page for write
    (4) record: version = trans_id, previous = delta record, offset, length = columns which
        are updated in place
    (5) unlock page
    (6) add changes as pending wal records
     :
    (7) commit
        (a) get ver_new
        (b) lock page for write
        (c) change version = trans_id to ver_new
        (d) unlock page
        (e) write and flush wal records
        (f) update latest snapshot to maybe ver_new
        (g) release all fat-locks
    --or--
    (7) rollback
        (a) lock page for write
        (b) undo column updates
        (c) change version = trans_id to ver_current
        (d) unlock page
        (e) free delta record

- all changes are logged before page is written
- all logged changes are idempotent: they can be applied multiple times and always result in
  the same resulting page
- rows are not updated in place

- a slot is a pointer to another slot or a local reference to data
- slots which are rows *always* contain a pointer; all other slots are local references
- slot pointer: 1 bit reserved for pointer vs local reference, 16 bits for slot index,
  47 bits for page number
- local reference: 1 bit reserved for pointer vs local reference, 20 bits for offset,
  20 bits for length, 3 bits for format, 2 bits for type, 2 bits for flags, 16 bits column count

- record types: complete, delta, tombstone
- record flags: has_previous, fragment
- record has column count
- record fields: version, optional previous (for older versions), optional next (for fragments),
  zero or more columns
*/
