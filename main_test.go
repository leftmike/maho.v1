package main

import (
	"testing"
)

func TestMain(t *testing.T) {
	err := start()
	if err != nil {
		t.Errorf("start() failed with %s", err)
		return
	}
	/*
	   	cases := []struct {
	   		s string
	   		r string
	   	}{
	   		{"select * from engine.tables", `   database         table num_columns
	      --------         ----- -----------
	    1 'engine'      'stores'           1
	    2 'engine'   'databases'           2
	    3 'engine'      'tables'           3
	    4 'engine'     'columns'           9
	    5 'engine' 'identifiers'           3
	   `},
	   	}

	   	for i, c := range cases {
	   		var b bytes.Buffer
	   		replSQL(parser.NewParser(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i)), &b)
	   		if b.String() != c.r {
	   			t.Errorf("parse(%q) got\n%s\nwant\n%s", c.s, b.String(), c.r)
	   		}
	   	}
	*/
}
