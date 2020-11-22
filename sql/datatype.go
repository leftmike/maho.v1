package sql

import "fmt"

type DataType int

const (
	UnknownType DataType = iota
	BooleanType
	StringType
	BytesType
	FloatType
	IntegerType
)

func (dt DataType) String() string {
	switch dt {
	case UnknownType:
		return "UNKNOWN"
	case BooleanType:
		return "BOOL"
	case StringType:
		return "STRING"
	case BytesType:
		return "BYTES"
	case FloatType:
		return "DOUBLE"
	case IntegerType:
		return "INT"
	default:
		panic(fmt.Sprintf("unexpected datatype; got %#v", dt))
	}
}
