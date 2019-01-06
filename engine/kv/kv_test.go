package kv_test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine/kv"
)

func TestFixPath(t *testing.T) {
	cases := []struct {
		path   string
		result string
		fail   bool
	}{
		{path: "", fail: true},
		{path: "filename", result: "filename.kv_test"},
		{path: filepath.Join("path", "filename"), result: filepath.Join("path", "filename.kv_test")},
		{path: filepath.Join("path", "filename.test"),
			result: filepath.Join("path", "filename.test")},
	}

	for _, c := range cases {
		r, err := kv.FixPath(c.path, ".kv_test", "test")
		if err != nil {
			if !c.fail {
				t.Errorf("FixPath(%q) failed with %s", c.path, err)
			}
		} else if c.fail {
			t.Errorf("FixPath(%q) did not fail", c.path)
		} else if c.result != r {
			t.Errorf("FixPath(%q) got %q want %q", c.path, r, c.result)
		}
	}
}
