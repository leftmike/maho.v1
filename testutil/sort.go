package testutil

import (
	"sort"

	"github.com/leftmike/maho/sql"
)

type sortValues struct {
	values [][]sql.Value
	key    []sql.ColumnKey
}

func (sv sortValues) Len() int {
	return len(sv.values)
}

func (sv sortValues) Swap(i, j int) {
	sv.values[i], sv.values[j] = sv.values[j], sv.values[i]
}

func (sv sortValues) Less(i, j int) bool {
	for _, ck := range sv.key {
		vi := sv.values[i][ck.Column()]
		vj := sv.values[j][ck.Column()]
		cmp := sql.Compare(vi, vj)
		if cmp < 0 {
			return !ck.Reverse()
		} else if cmp > 0 {
			return ck.Reverse()
		}
	}
	return false
}

func SortValues(key []sql.ColumnKey, values [][]sql.Value) {
	sort.Sort(sortValues{values: values, key: key})
}
