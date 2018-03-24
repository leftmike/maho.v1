package sql

type DataType int

const (
	BooleanType DataType = iota + 1
	CharacterType
	FloatType
	IntegerType
)

func (dt DataType) String() string {
	switch dt {
	case BooleanType:
		return "BOOL"
	case CharacterType:
		return "CHAR"
	case FloatType:
		return "DOUBLE"
	case IntegerType:
		return "INT"
	}

	return ""
}
