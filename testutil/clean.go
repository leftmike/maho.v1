package testutil

import (
	"os"
	"path/filepath"
)

// CleanDir removes everything in the directory named by dirname except for
// any directory entries specified by keeps.
func CleanDir(dirname string, keeps []string) error {
	d, err := os.Open(dirname)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	fis, err := d.Readdir(-1)
	d.Close()
	if err != nil {
		return err
	}

	m := map[string]struct{}{}
	for _, k := range keeps {
		m[k] = struct{}{}
	}

	for _, fi := range fis {
		n := fi.Name()
		if _, found := m[n]; found {
			continue
		}
		err = os.RemoveAll(filepath.Join(dirname, n))
		if err != nil {
			return err
		}
	}
	return nil
}
