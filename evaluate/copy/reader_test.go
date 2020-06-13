package copy_test

import (
	"io"
	"strings"
	"testing"

	"github.com/leftmike/maho/evaluate/copy"
)

func TestReader(t *testing.T) {
	test := `a
bb
ccc\\d
\.
eeeee
`
	results := []struct {
		r   rune
		p   string
		eof bool
	}{
		{r: 'a', p: "test:1:1"},
		{r: '\n', p: "test:2:0"},
		{r: 'b', p: "test:2:1"},
		{r: 'b', p: "test:2:2"},
		{r: '\n', p: "test:3:0"},
		{r: 'c', p: "test:3:1"},
		{r: 'c', p: "test:3:2"},
		{r: 'c', p: "test:3:3"},
		{r: '\\', p: "test:3:4"},
		{r: '\\', p: "test:3:5"},
		{r: 'd', p: "test:3:6"},
		{r: '\n', p: "test:4:0"},
		{eof: true},
		{eof: true},
	}

	rdr := copy.NewReader("test", strings.NewReader(test), 1)
	for _, ret := range results {
		r, err := rdr.ReadRune()
		if err == io.EOF {
			if !ret.eof {
				t.Errorf("ReadRune(%s) failed with %s", rdr.Position(), err)
			}
		} else if err != nil {
			t.Errorf("ReadRune(%s) failed with %s", rdr.Position(), err)
		} else if ret.eof {
			t.Errorf("ReadRune(%s) did not return EOF", rdr.Position())
		} else if ret.r != r {
			t.Errorf("ReadRune(%s) got %c want %c", rdr.Position(), r, ret.r)
		} else if p := rdr.Position(); ret.p != p {
			t.Errorf("Position() got %s want %s", p, ret.p)
		}
	}
}
