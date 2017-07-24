package sql

type DataType int

const (
	BooleanType DataType = iota + 1
	CharacterType
	DoubleType
	IntegerType
)

func (dt DataType) String() string {
	switch dt {
	case BooleanType:
		return "BOOL"
	case CharacterType:
		return "CHAR"
	case DoubleType:
		return "DOUBLE"
	case IntegerType:
		return "INT"
	}

	return ""
}
