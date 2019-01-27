package encoding

import (
	fmt "fmt"

	"github.com/leftmike/maho/sql"
)

//go:generate protoc --go_out=. metadata.proto

func FromDataType(dt sql.DataType) DataType {
	switch dt {
	case sql.BooleanType:
		return DataType_Boolean
	case sql.CharacterType:
		return DataType_Character
	case sql.FloatType:
		return DataType_Float
	case sql.IntegerType:
		return DataType_Integer
	}
	panic(fmt.Sprintf("FromDataType: unknown type: %d", dt))
}

func ToDataType(dt DataType) (sql.DataType, bool) {
	switch dt {
	case DataType_Boolean:
		return sql.BooleanType, true
	case DataType_Character:
		return sql.CharacterType, true
	case DataType_Float:
		return sql.FloatType, true
	case DataType_Integer:
		return sql.IntegerType, true
	}
	return 0, false
}
