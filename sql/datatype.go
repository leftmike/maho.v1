package sql

type DataType int

const (
	BooleanType DataType = iota + 1
	StringType
	BytesType
	FloatType
	IntegerType
)

func (dt DataType) String() string {
	switch dt {
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
	}

	return ""
}
