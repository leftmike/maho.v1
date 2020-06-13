package sql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
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
	Default Expr
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

func (ct ColumnType) DataType() string {
	switch ct.Type {
	case BooleanType:
		return "BOOL"
	case StringType:
		if ct.Fixed {
			return fmt.Sprintf("CHAR(%d)", ct.Size)
		} else if ct.Size == MaxColumnSize {
			return "TEXT"
		} else {
			return fmt.Sprintf("VARCHAR(%d)", ct.Size)
		}
	case BytesType:
		if ct.Fixed {
			return fmt.Sprintf("BINARY(%d)", ct.Size)
		} else if ct.Size == MaxColumnSize {
			return "BYTES"
		} else {
			return fmt.Sprintf("VARBINARY(%d)", ct.Size)
		}
	case FloatType:
		return "DOUBLE"
	case IntegerType:
		switch ct.Size {
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

func (ct ColumnType) ConvertValue(n Identifier, v Value) (Value, error) {
	if v == nil {
		if ct.NotNull {
			return nil, fmt.Errorf("column \"%s\" may not be NULL", n)
		}
		return nil, nil
	}

	switch ct.Type {
	case BooleanType:
		if sv, ok := v.(StringValue); ok {
			s := strings.Trim(string(sv), " \t\n")
			if s == "t" || s == "true" || s == "y" || s == "yes" || s == "on" || s == "1" {
				return BoolValue(true), nil
			} else if s == "f" || s == "false" || s == "n" || s == "no" || s == "off" || s == "0" {
				return BoolValue(false), nil
			} else {
				return nil, fmt.Errorf("column \"%s\": expected a boolean value: %v", n, v)
			}
		} else if _, ok := v.(BoolValue); !ok {
			return nil, fmt.Errorf("column \"%s\": expected a boolean value: %v", n, v)
		}
	case StringType:
		if i, ok := v.(Int64Value); ok {
			return StringValue(strconv.FormatInt(int64(i), 10)), nil
		} else if f, ok := v.(Float64Value); ok {
			return StringValue(strconv.FormatFloat(float64(f), 'g', -1, 64)), nil
		} else if b, ok := v.(BytesValue); ok {
			if !utf8.Valid([]byte(b)) {
				return nil, fmt.Errorf(`column "%s": expected a valid utf8 string: %v`, n, v)
			}
			return StringValue(b), nil
		} else if _, ok := v.(StringValue); !ok {
			return nil, fmt.Errorf(`column "%s": expected a string value: %v`, n, v)
		}
	case BytesType:
		if s, ok := v.(StringValue); ok {
			return BytesValue(s), nil
		} else if _, ok := v.(BytesValue); !ok {
			return nil, fmt.Errorf(`column "%s": expected a bytes value: %v`, n, v)
		}
	case FloatType:
		if i, ok := v.(Int64Value); ok {
			return Float64Value(i), nil
		} else if s, ok := v.(StringValue); ok {
			d, err := strconv.ParseFloat(strings.Trim(string(s), " \t\n"), 64)
			if err != nil {
				return nil, fmt.Errorf("column \"%s\": expected a float: %v: %s", n, v, err)
			}
			return Float64Value(d), nil
		} else if _, ok := v.(Float64Value); !ok {
			return nil, fmt.Errorf("column \"%s\": expected a float value: %v", n, v)
		}
	case IntegerType:
		if f, ok := v.(Float64Value); ok {
			return Int64Value(f), nil
		} else if s, ok := v.(StringValue); ok {
			i, err := strconv.ParseInt(strings.Trim(string(s), " \t\n"), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("column \"%s\": expected an integer: %v: %s", n, v, err)
			}
			return Int64Value(i), nil
		} else if _, ok := v.(Int64Value); !ok {
			return nil, fmt.Errorf("column \"%s\": expected an integer value: %v", n, v)
		}
	}

	return v, nil
}
