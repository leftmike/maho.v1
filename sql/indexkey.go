package sql

import (
	"fmt"
)

type IndexKey struct {
	Unique  bool
	Columns []Identifier
	Reverse []bool // ASC = false, DESC = true
}

func (ik IndexKey) String() string {
	s := "("
	for i := range ik.Columns {
		if i > 0 {
			s += ", "
		}
		if ik.Reverse[i] {
			s += fmt.Sprintf("%s DESC", ik.Columns[i])
		} else {
			s += fmt.Sprintf("%s ASC", ik.Columns[i])
		}
	}
	s += ")"
	return s
}

func (ik IndexKey) Equal(oik IndexKey) bool {
	if len(ik.Columns) != len(oik.Columns) {
		return false
	}

	for cdx := range ik.Columns {
		if ik.Columns[cdx] != oik.Columns[cdx] || ik.Reverse[cdx] != oik.Reverse[cdx] {
			return false
		}
	}
	return true
}
