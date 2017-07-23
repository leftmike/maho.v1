package sql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

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

func (dt DataType) ConvertTo(v Value) (Value, error) {
	k := reflect.TypeOf(v).Kind()
	switch dt {
	case BooleanType:
		if k == reflect.String {
			s := strings.Trim(reflect.ValueOf(v).String(), " \t\n")
			if s == "t" || s == "true" || s == "y" || s == "yes" || s == "on" || s == "1" {
				return true, nil
			} else if s == "f" || s == "false" || s == "n" || s == "no" || s == "off" || s == "0" {
				return false, nil
			} else {
				return nil, fmt.Errorf("expected a boolean value: %v", v)
			}
		} else if k != reflect.Bool {
			return nil, fmt.Errorf("expected a boolean value: %v", v)
		}
	case CharacterType:
		if k == reflect.Int64 {
			return strconv.FormatInt(reflect.ValueOf(v).Int(), 10), nil
		} else if k == reflect.Float64 {
			return strconv.FormatFloat(reflect.ValueOf(v).Float(), 'g', -1, 64), nil
		} else if k != reflect.String {
			return nil, fmt.Errorf("expected a string value: %v", v)
		}
	case DoubleType:
		if k == reflect.Int64 {
			return float64(reflect.ValueOf(v).Int()), nil
		} else if k == reflect.String {
			d, err := strconv.ParseFloat(strings.Trim(reflect.ValueOf(v).String(), " \t\n"), 64)
			if err != nil {
				return nil, fmt.Errorf("expected a float: %v: %s", v, err.Error())
			}
			return d, nil
		} else if k != reflect.Float64 {
			return nil, fmt.Errorf("expected a float value: %v", v)
		}
	case IntegerType:
		if k == reflect.Float64 {
			return int64(reflect.ValueOf(v).Float()), nil
		} else if k == reflect.String {
			i, err := strconv.ParseInt(strings.Trim(reflect.ValueOf(v).String(), " \t\n"), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("expected an integer: %v: %s", v, err.Error())
			}
			return i, nil
		} else if k != reflect.Int64 {
			return nil, fmt.Errorf("expected an integer value: %v", v)
		}
	}

	return v, nil
}
