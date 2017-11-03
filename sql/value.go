package sql

import (
	"fmt"
)

const (
	NullString  = "NULL"
	TrueString  = "true"
	FalseString = "false"
)

type Value interface{}

func Format(v Value) string {
	if v == nil {
		return NullString
	}

	if b, ok := v.(bool); ok {
		if b {
			return TrueString
		}
		return FalseString
	} else if s, ok := v.(string); ok {
		return fmt.Sprintf("'%s'", s)
	}

	return fmt.Sprintf("%v", v)
}

func Less(v1, v2 Value) bool {
	if v1 == nil {
		return v2 != nil
	}
	if v2 == nil {
		return false
	}
	switch v1 := v1.(type) {
	case bool:
		switch v2 := v2.(type) {
		case bool:
			return !v1 && v2
		case float64:
			return true
		case int64:
			return true
		case string:
			return true
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", v2, v2))
		}
	case float64:
		switch v2 := v2.(type) {
		case bool:
			return false
		case float64:
			return v1 < v2
		case int64:
			return true
		case string:
			return true
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", v2, v2))
		}
	case int64:
		switch v2 := v2.(type) {
		case bool:
			return false
		case float64:
			return false
		case int64:
			return v1 < v2
		case string:
			return true
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", v2, v2))
		}
	case string:
		switch v2 := v2.(type) {
		case bool:
			return false
		case float64:
			return false
		case int64:
			return false
		case string:
			return v1 < v2
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", v2, v2))
		}
	default:
		panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", v1, v1))
	}
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
