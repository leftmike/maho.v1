package sql

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	NullString  = "NULL"
	TrueString  = "true"
	FalseString = "false"
)

type Value interface {
	fmt.Stringer

	// return -1 if v1 < v2
	// return 0 if v1 == v2
	// return 1 if v1 > v2
	Compare(v2 Value) (int, error)
}

type BoolValue bool

func (b BoolValue) String() string {
	if b {
		return TrueString
	}
	return FalseString
}

func (b1 BoolValue) Compare(v2 Value) (int, error) {
	if b2, ok := v2.(BoolValue); ok {
		if b1 {
			if b2 {
				return 0, nil
			}
			return 1, nil
		} else {
			if b2 {
				return -1, nil
			}
			return 0, nil
		}
	}
	return 0, fmt.Errorf("engine: want boolean got %v", v2)
}

type Int64Value int64

func (i Int64Value) String() string {
	return fmt.Sprintf("%v", int64(i))
}

func (i1 Int64Value) Compare(v2 Value) (int, error) {
	switch v2 := v2.(type) {
	case Int64Value:
		if i1 < v2 {
			return -1, nil
		} else if i1 > v2 {
			return 1, nil
		}
		return 0, nil
	case Float64Value:
		if Float64Value(i1) < v2 {
			return -1, nil
		} else if Float64Value(i1) > v2 {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("engine: want number got %v", v2)
}

type Float64Value float64

func (d Float64Value) String() string {
	return fmt.Sprintf("%v", float64(d))
}

func (d1 Float64Value) Compare(v2 Value) (int, error) {
	switch v2 := v2.(type) {
	case Int64Value:
		if d1 < Float64Value(v2) {
			return -1, nil
		} else if d1 > Float64Value(v2) {
			return 1, nil
		}
		return 0, nil
	case Float64Value:
		if d1 < Float64Value(v2) {
			return -1, nil
		} else if d1 > Float64Value(v2) {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("engine: want number got %v", v2)
}

type StringValue string

func (s StringValue) String() string {
	return fmt.Sprintf("'%s'", string(s))
}

func (s1 StringValue) Compare(v2 Value) (int, error) {
	if s2, ok := v2.(StringValue); ok {
		return strings.Compare(string(s1), string(s2)), nil
	}
	return 0, fmt.Errorf("engine: want string got %v", v2)
}

type BytesValue []byte

var (
	hexDigits = [16]rune{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
		'e', 'f'}
)

func (b BytesValue) String() string {
	var buf bytes.Buffer
	buf.WriteString("'\\x")
	for _, v := range b {
		buf.WriteRune(hexDigits[v>>4])
		buf.WriteRune(hexDigits[v&0xF])
	}

	buf.WriteRune('\'')
	return buf.String()
}

func (b1 BytesValue) Compare(v2 Value) (int, error) {
	if b2, ok := v2.(BytesValue); ok {
		return bytes.Compare([]byte(b1), []byte(b2)), nil
	}
	return 0, fmt.Errorf("engine: want bytes got %v", v2)
}

func Compare(v1, v2 Value) int {
	if v1 == nil {
		if v2 == nil {
			return 0
		}
		return -1
	}
	if v2 == nil {
		return 1
	}
	switch v1 := v1.(type) {
	case BoolValue:
		switch v2 := v2.(type) {
		case BoolValue:
			cmp, _ := v1.Compare(v2)
			return cmp
		case Float64Value, Int64Value, StringValue, BytesValue:
			return -1
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", v2, v2))
		}
	case Float64Value, Int64Value:
		switch v2 := v2.(type) {
		case BoolValue:
			return 1
		case Float64Value, Int64Value:
			cmp, _ := v1.Compare(v2)
			return cmp
		case StringValue, BytesValue:
			return -1
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", v2, v2))
		}
	case StringValue:
		switch v2 := v2.(type) {
		case BoolValue, Float64Value, Int64Value:
			return 1
		case StringValue:
			cmp, _ := v1.Compare(v2)
			return cmp
		case BytesValue:
			return -1
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", v2, v2))
		}
	case BytesValue:
		switch v2 := v2.(type) {
		case BoolValue, Float64Value, Int64Value, StringValue:
			return 1
		case BytesValue:
			cmp, _ := v1.Compare(v2)
			return cmp
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", v2, v2))
		}
	default:
		panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", v1, v1))
	}
}

func Format(v Value) string {
	if v == nil {
		return NullString
	}

	return v.String()
}

func ConvertValue(dt DataType, v Value) (Value, error) {
	switch dt {
	case BooleanType:
		if sv, ok := v.(StringValue); ok {
			s := strings.Trim(string(sv), " \t\n")
			if s == "t" || s == "true" || s == "y" || s == "yes" || s == "on" || s == "1" {
				return BoolValue(true), nil
			} else if s == "f" || s == "false" || s == "n" || s == "no" || s == "off" || s == "0" {
				return BoolValue(false), nil
			} else {
				return nil, fmt.Errorf("expected a boolean value: %v", v)
			}
		} else if _, ok := v.(BoolValue); !ok {
			return nil, fmt.Errorf("expected a boolean value: %v", v)
		}
	case StringType:
		if i, ok := v.(Int64Value); ok {
			return StringValue(strconv.FormatInt(int64(i), 10)), nil
		} else if f, ok := v.(Float64Value); ok {
			return StringValue(strconv.FormatFloat(float64(f), 'g', -1, 64)), nil
		} else if b, ok := v.(BytesValue); ok {
			if !utf8.Valid([]byte(b)) {
				return nil, fmt.Errorf("expected a valid utf8 string: %v", v)
			}
			return StringValue(b), nil
		} else if _, ok := v.(StringValue); !ok {
			return nil, fmt.Errorf("expected a string value: %v", v)
		}
	case BytesType:
		if s, ok := v.(StringValue); ok {
			return BytesValue(s), nil
		} else if _, ok := v.(BytesValue); !ok {
			return nil, fmt.Errorf("expected a bytes value: %v", v)
		}
	case FloatType:
		if i, ok := v.(Int64Value); ok {
			return Float64Value(i), nil
		} else if s, ok := v.(StringValue); ok {
			d, err := strconv.ParseFloat(strings.Trim(string(s), " \t\n"), 64)
			if err != nil {
				return nil, fmt.Errorf("expected a float: %v: %s", v, err)
			}
			return Float64Value(d), nil
		} else if _, ok := v.(Float64Value); !ok {
			return nil, fmt.Errorf("expected a float value: %v", v)
		}
	case IntegerType:
		if f, ok := v.(Float64Value); ok {
			return Int64Value(f), nil
		} else if s, ok := v.(StringValue); ok {
			i, err := strconv.ParseInt(strings.Trim(string(s), " \t\n"), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("expected an integer: %v: %s", v, err)
			}
			return Int64Value(i), nil
		} else if _, ok := v.(Int64Value); !ok {
			return nil, fmt.Errorf("expected an integer value: %v", v)
		}
	default:
		panic(fmt.Sprintf("expected a valid data type; got %v", dt))
	}

	return v, nil
}

/*
database/sql package ==>
Scan converts from columns to Go types:
*string
*[]byte
*int, *int8, *int16, *int32, *int64
*uint, *uint8, *uint16, *uint32, *uint64
*bool
*float32, *float64
*interface{}
*RawBytes
any type implementing Scanner (see Scanner docs)

database/sql/driver package ==>
nil
int64
float64
bool
[]byte
string
time.Time
*/
