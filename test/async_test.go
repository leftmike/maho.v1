package test_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/repl"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

func fln() testutil.FileLineNumber {
	return testutil.MakeFileLineNumber()
}

type step struct {
	fln    testutil.FileLineNumber
	thrd   int
	cmd    string
	stmts  string
	result string
	fail   bool
}

var (
	test1 = []step{
		{
			fln: fln(),
			stmts: `
CREATE TABLE tbl1 (
    c1 int PRIMARY KEY,
    c2 int
);
CREATE TABLE tbl2 (
    c3 int PRIMARY KEY,
    c4 int REFERENCES tbl1
);
INSERT INTO tbl1 VALUES
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40),
    (5, 50);
INSERT INTO tbl2 VALUES
    (-1, 1),
    (-2, 2),
    (-3, 3);
`,
		},
		{fln: fln(), thrd: 0, cmd: "sync"},
		{
			fln: fln(),
			stmts: `
INSERT INTO tbl2 VALUES
    (-4, 100);
`,
			fail: true,
		},
		{
			fln:  fln(),
			thrd: 1,
			stmts: `
DELETE FROM tbl1 WHERE c1 = 2;
`,
			fail: true,
		},
		{fln: fln(), thrd: 0, cmd: "sync"},
		{fln: fln(), thrd: 1, cmd: "sync"},
		{
			fln: fln(),
			stmts: `
BEGIN;
INSERT INTO tbl2 VALUES
    (-4, 4);
`,
		},
		{fln: fln(), thrd: 0, cmd: "sync"},
		{
			fln:  fln(),
			thrd: 1,
			stmts: `
DELETE FROM tbl1 WHERE c1 = 4;
`,
			fail: true,
		},
		{
			fln: fln(),
			stmts: `
COMMIT;
`,
		},
	}
)

func asyncThread(t *testing.T, e sql.Engine, dbname sql.Identifier, steps <-chan step,
	out chan<- struct{}) {

	ses := evaluate.NewSession(e, dbname, sql.PUBLIC)

	for stp := range steps {
		if stp.stmts != "" {
			var b bytes.Buffer
			p := parser.NewParser(strings.NewReader(stp.stmts), stp.stmts)

			for {
				stmt, err := p.Parse()
				if err != nil {
					if err == io.EOF {
						break
					}

					t.Fatalf("%sParse(%s) failed with %s", stp.fln, stp.stmts, err)
				}

				err = ses.Run(stmt,
					func(ctx context.Context, ses *evaluate.Session, e sql.Engine,
						tx sql.Transaction) error {

						return repl.Evaluate(ctx, ses, e, tx, stmt, &b)
					})

				if stp.fail {
					if err == nil {
						t.Errorf("%sEvaluate(%s) did not fail", stp.fln, stp.stmts)
					}
				} else if err != nil {
					t.Errorf("%sEvaluate(%s) failed with %s", stp.fln, stp.stmts, err)
				}
			}

			if stp.result != "" && b.String() != stp.result {
				t.Errorf("%sEvaluate(%s): got %s; want %s", stp.fln, stp.stmts, b.String(),
					stp.result)
			}
		} else {
			switch stp.cmd {
			case "sync":
				out <- struct{}{}
			default:
				t.Fatalf("unexpected command: %s", stp.cmd)
			}
		}
	}
}

func testAsync(t *testing.T, e sql.Engine, dbname sql.Identifier, steps []step) {
	var thrds [4]chan step
	var syncs [4]chan struct{}
	var wg sync.WaitGroup

	for _, stp := range steps {
		if stp.thrd >= len(thrds) {
			t.Fatalf("stp.thrd = %d; len(thrds) = %d", stp.thrd, len(thrds))
		}
		if thrds[stp.thrd] == nil {
			thrds[stp.thrd] = make(chan step, 10)
			syncs[stp.thrd] = make(chan struct{})
			wg.Add(1)

			go func(thrd int) {
				defer wg.Done()

				asyncThread(t, e, dbname, thrds[thrd], syncs[thrd])
			}(stp.thrd)
		}

		thrds[stp.thrd] <- stp
		if stp.cmd == "sync" {
			<-syncs[stp.thrd]
		}
	}

	for _, thrd := range thrds {
		if thrd != nil {
			close(thrd)
		}
	}

	wg.Wait()
}

func TestAsync(t *testing.T) {
	cleanDir(t)

	for _, cfg := range configs {
		if cfg.name != "basic" {
			continue // XXX: need read locks for foreign keys
		}

		t.Run(cfg.name,
			func(t *testing.T) {
				dataDir := filepath.Join("testdata", "async", cfg.name)
				os.MkdirAll(dataDir, 0755)

				st, err := cfg.newStore(dataDir)
				if err != nil {
					t.Fatal(err)
				}

				e := engine.NewEngine(st, flags.Default())
				e.CreateDatabase(sql.ID("test"), nil)
				// Ignore errors: the database might already exist.

				testAsync(t, e, sql.ID("test"), test1)
			})
	}
}
