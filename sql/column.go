package sql

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

type Column struct {
	Name Identifier
	Type DataType

	// Size of the column in bytes for integers and in characters for character columns
	Size uint32

	Width    uint8 // display width of integers and doubles
	Fraction uint8 // display width of fractional part of doubles
	Fixed    bool  // fixed sized character column
	Binary   bool  // binary character column

	NotNull bool
	Default Value
}

func (c Column) DataType() string {
	switch c.Type {
	case BooleanType:
		return "BOOL"
	case CharacterType:
		if c.Binary {
			if c.Fixed {
				return fmt.Sprintf("BINARY(%d)", c.Size)
			} else if c.Size == math.MaxUint32-1 {
				return "BLOB"
			} else {
				return fmt.Sprintf("VARBINARY(%d)", c.Size)
			}
		} else {
			if c.Fixed {
				return fmt.Sprintf("CHAR(%d)", c.Size)
			} else if c.Size == math.MaxUint32-1 {
				return "TEXT"
			} else {
				return fmt.Sprintf("VARCHAR(%d)", c.Size)
			}
		}
	case DoubleType:
		return "DOUBLE"
	case IntegerType:
		switch c.Size {
		case 1:
			return "TINYINT"
		case 2:
			return "SMALLINT"
		case 3:
			return "MEDIUMINT"
		case 4:
			return "INT"
		case 8:
			return "BIGINT"
		}
	}
	return ""
}

func (c Column) ConvertValue(v Value) (Value, error) {
	if v == nil {
		if c.NotNull {
			return nil, fmt.Errorf("column may not be NULL: %s", c.Name)
		}
		return nil, nil
	} else if _, ok := v.(Default); ok {
		if c.NotNull && c.Default == nil {
			return nil, fmt.Errorf("value must be specified for column: %s", c.Name)
		} else if c.Default == nil {
			return nil, nil
		} else {
			v = c.Default
		}
	}

	k := reflect.TypeOf(v).Kind()
	switch c.Type {
	case BooleanType:
		if k == reflect.String {
			s := strings.Trim(reflect.ValueOf(v).String(), " \t\n")
			if s == "t" || s == "true" || s == "y" || s == "yes" || s == "on" || s == "1" {
				return true, nil
			} else if s == "f" || s == "false" || s == "n" || s == "no" || s == "off" || s == "0" {
				return false, nil
			} else {
				return nil, fmt.Errorf("column: %s: expected a boolean value: %v", c.Name, v)
			}
		} else if k != reflect.Bool {
			return nil, fmt.Errorf("column: %s: expected a boolean value: %v", c.Name, v)
		}
	case CharacterType:
		if k == reflect.Int64 {
			return strconv.FormatInt(reflect.ValueOf(v).Int(), 10), nil
		} else if k == reflect.Float64 {
			return strconv.FormatFloat(reflect.ValueOf(v).Float(), 'g', -1, 64), nil
		} else if k != reflect.String {
			return nil, fmt.Errorf("column: %s: expected a string value: %v", c.Name, v)
		}
	case DoubleType:
		if k == reflect.Int64 {
			return float64(reflect.ValueOf(v).Int()), nil
		} else if k == reflect.String {
			d, err := strconv.ParseFloat(strings.Trim(reflect.ValueOf(v).String(), " \t\n"), 64)
			if err != nil {
				return nil, fmt.Errorf("column: %s: expected a float: %v: %s", c.Name, v,
					err.Error())
			}
			return d, nil
		} else if k != reflect.Float64 {
			return nil, fmt.Errorf("column: %s: expected a float value: %v", c.Name, v)
		}
	case IntegerType:
		if k == reflect.Float64 {
			return int64(reflect.ValueOf(v).Float()), nil
		} else if k == reflect.String {
			i, err := strconv.ParseInt(strings.Trim(reflect.ValueOf(v).String(), " \t\n"), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("column: %s: expected an integer: %v: %s", c.Name, v,
					err.Error())
			}
			return i, nil
		} else if k != reflect.Int64 {
			return nil, fmt.Errorf("column: %s: expected an integer value: %v", c.Name, v)
		}
	}

	return v, nil
}
