package sql

import (
	"fmt"
	"math"
)

type ColumnUpdate struct {
	Index int
	Value Value
}

const (
	MaxColumnSize = math.MaxUint32 - 1
)

type ColumnType struct {
	Type DataType

	// Size of the column in bytes for integers and in characters for character columns
	Size  uint32
	Fixed bool // fixed sized character column

	NotNull bool // not allowed to be NULL
	Default CExpr
}

var (
	IdColType         = ColumnType{Type: StringType, Size: MaxIdentifier, NotNull: true}
	Int32ColType      = ColumnType{Type: IntegerType, Size: 4, NotNull: true}
	Int64ColType      = ColumnType{Type: IntegerType, Size: 8, NotNull: true}
	NullInt64ColType  = ColumnType{Type: IntegerType, Size: 8}
	BoolColType       = ColumnType{Type: BooleanType, NotNull: true}
	StringColType     = ColumnType{Type: StringType, Size: 4096, NotNull: true}
	NullStringColType = ColumnType{Type: StringType, Size: 4096}
)

func ColumnDataType(dt DataType, size uint32, fixed bool) string {
	switch dt {
	case BooleanType:
		return "BOOL"
	case StringType:
		if fixed {
			return fmt.Sprintf("CHAR(%d)", size)
		} else if size == MaxColumnSize {
			return "TEXT"
		} else {
			return fmt.Sprintf("VARCHAR(%d)", size)
		}
	case BytesType:
		if fixed {
			return fmt.Sprintf("BINARY(%d)", size)
		} else if size == MaxColumnSize {
			return "BYTES"
		} else {
			return fmt.Sprintf("VARBINARY(%d)", size)
		}
	case FloatType:
		return "DOUBLE"
	case IntegerType:
		switch size {
		case 2:
			return "SMALLINT"
		case 4:
			return "INT"
		case 8:
			return "BIGINT"
		}
	}
	return ""
}
