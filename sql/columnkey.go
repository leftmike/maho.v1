package sql

type ColumnKey int

func MakeColumnKey(num int, reverse bool) ColumnKey {
	if num < 0 {
		panic("column numbers must be non-negative")
	}
	num += 1
	if reverse {
		return ColumnKey(-num)
	}
	return ColumnKey(num)
}

func (ck ColumnKey) Reverse() bool {
	return ck < 0
}

func (ck ColumnKey) Column() int {
	if ck < 0 {
		ck = -ck
	}
	return int(ck - 1)
}

func ColumnInKey(key []ColumnKey, pk ColumnKey) bool {
	for _, ck := range key {
		if ck.Column() == pk.Column() {
			return true
		}
	}

	return false
}
