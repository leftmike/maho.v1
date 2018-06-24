package testutil

import (
	"sort"

	"github.com/leftmike/maho/sql"
)

type sortValues [][]sql.Value

func (sv sortValues) Len() int {
	return len(sv)
}

func (sv sortValues) Swap(i, j int) {
	sv[i], sv[j] = sv[j], sv[i]
}

func (sv sortValues) Less(i, j int) bool {
	for cdx, val := range sv[i] {
		cmp := sql.Compare(val, sv[j][cdx])
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

func SortValues(values [][]sql.Value) {
	sort.Sort((sortValues)(values))
}
