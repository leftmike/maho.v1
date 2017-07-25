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

	if _, ok := v.(Default); ok {
		return "DEFAULT"
	}

	return fmt.Sprintf("%v", v)
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
