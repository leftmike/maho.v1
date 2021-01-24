package util_test

import (
	"sync"
	"testing"
	"time"

	"github.com/leftmike/maho/storage/util"
)

type step struct {
	thrd int
	cmd  string
	key  string
	fail bool
}

func lockerThread(t *testing.T, rl *util.RowLocks, steps <-chan step) {
	var lkr util.Locker

	for stp := range steps {
		switch stp.cmd {
		case "write":
			ret := rl.WLock(&lkr, []byte(stp.key))
			_ = ret
		case "read":
			rl.RLock(&lkr, []byte(stp.key))
		case "unlock":
			lkr.Unlock()
		default:
			t.Fatalf("unexpected command: %s", stp.cmd)
		}
	}
}

func TestRowLocks(t *testing.T) {
	var rl util.RowLocks
	var wg sync.WaitGroup

	thrds := [4]chan step{
		make(chan step),
		make(chan step),
		make(chan step),
		make(chan step),
	}
	for _, thrd := range thrds {
		wg.Add(1)
		go func(thrd <-chan step) {
			defer wg.Done()

			lockerThread(t, &rl, thrd)
		}(thrd)
	}

	steps := []step{
		{thrd: 0, cmd: "read", key: "abc"},
		{thrd: 1, cmd: "write", key: "def"},
		{thrd: 1, cmd: "write", key: "abc"},
		{thrd: 0, cmd: "unlock"},
		{thrd: 1, cmd: "unlock"},

		{thrd: 0, cmd: "read", key: "abcd"},
		{thrd: 1, cmd: "read", key: "abcd"},
		{thrd: 0, cmd: "read", key: "abcd"},
		{thrd: 1, cmd: "read", key: "abcd"},
		{thrd: 0, cmd: "write", key: "abcd", fail: true},
		{thrd: 1, cmd: "write", key: "abcd", fail: true},
		{thrd: 1, cmd: "unlock"},
		{thrd: 0, cmd: "write", key: "abcd"},
		{thrd: 0, cmd: "write", key: "abcd"},
		{thrd: 0, cmd: "unlock"},

		{thrd: 0, cmd: "read", key: "abcdf"},
		{thrd: 1, cmd: "read", key: "abcdf"},
		{thrd: 1, cmd: "unlock"},
		{thrd: 0, cmd: "write", key: "abcdf"},
		{thrd: 0, cmd: "unlock"},

		{thrd: 0, cmd: "write", key: "efgh"},
		{thrd: 1, cmd: "read", key: "efgh"},
		{thrd: 2, cmd: "read", key: "efgh"},
		{thrd: 3, cmd: "write", key: "efgh"},
		{thrd: 0, cmd: "unlock"},
		{thrd: 2, cmd: "write", key: "efgh", fail: true},
		{thrd: 2, cmd: "unlock"},
		{thrd: 1, cmd: "unlock"},
		{thrd: 3, cmd: "write", key: "efgh"},
		{thrd: 3, cmd: "unlock"},
	}

	for _, stp := range steps {
		thrds[stp.thrd] <- stp
		time.Sleep(time.Millisecond)
	}

	for _, thrd := range thrds {
		close(thrd)
	}

	wg.Wait()
}
