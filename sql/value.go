package sql

import (
	"fmt"
	"reflect"
)

type Value interface{}

type Formatter interface {
	Format() string
}

func FormatValue(v Value) string {
	if v == nil {
		return "NULL"
	}

	switch reflect.TypeOf(v).Kind() {
	case reflect.Bool:
		if reflect.ValueOf(v).Bool() {
			return "t"
		}
		return "f"
	case reflect.String:
		return fmt.Sprintf("'%v'", v)
	}

	if f, ok := v.(Formatter); ok {
		return f.Format()
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