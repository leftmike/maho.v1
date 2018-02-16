package sql

import (
	"fmt"
	"strings"
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
		case Float64Value, Int64Value, StringValue:
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
		case StringValue:
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
