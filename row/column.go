package row

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"maho/expr"
	"maho/sql"
)

type Column struct {
	Name sql.Identifier
	Type sql.DataType

	// Size of the column in bytes for integers and in characters for character columns
	Size uint32

	Width    uint8 // display width of integers and doubles
	Fraction uint8 // display width of fractional part of doubles
	Fixed    bool  // fixed sized character column
	Binary   bool  // binary character column

	NotNull bool
	Default expr.Expr
}

func (c Column) DataType() string {
	switch c.Type {
	case sql.BooleanType:
		return "BOOL"
	case sql.CharacterType:
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
	case sql.DoubleType:
		return "DOUBLE"
	case sql.IntegerType:
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

func (c Column) ConvertValue(v sql.Value) (sql.Value, error) {
	if v == nil {
		if c.NotNull {
			return nil, fmt.Errorf("column may not be NULL: %s", c.Name)
		}
		return nil, nil
	}

	switch c.Type {
	case sql.BooleanType:
		if s, ok := v.(string); ok {
			s = strings.Trim(s, " \t\n")
			if s == "t" || s == "true" || s == "y" || s == "yes" || s == "on" || s == "1" {
				return true, nil
			} else if s == "f" || s == "false" || s == "n" || s == "no" || s == "off" || s == "0" {
				return false, nil
			} else {
				return nil, fmt.Errorf("column: %s: expected a boolean value: %v", c.Name, v)
			}
		} else if _, ok := v.(bool); !ok {
			return nil, fmt.Errorf("column: %s: expected a boolean value: %v", c.Name, v)
		}
	case sql.CharacterType:
		if i, ok := v.(int64); ok {
			return strconv.FormatInt(i, 10), nil
		} else if f, ok := v.(float64); ok {
			return strconv.FormatFloat(f, 'g', -1, 64), nil
		} else if _, ok := v.(string); !ok {
			return nil, fmt.Errorf("column: %s: expected a string value: %v", c.Name, v)
		}
	case sql.DoubleType:
		if i, ok := v.(int64); ok {
			return float64(i), nil
		} else if s, ok := v.(string); ok {
			d, err := strconv.ParseFloat(strings.Trim(s, " \t\n"), 64)
			if err != nil {
				return nil, fmt.Errorf("column: %s: expected a float: %v: %s", c.Name, v,
					err.Error())
			}
			return d, nil
		} else if _, ok := v.(float64); !ok {
			return nil, fmt.Errorf("column: %s: expected a float value: %v", c.Name, v)
		}
	case sql.IntegerType:
		if f, ok := v.(float64); ok {
			return int64(f), nil
		} else if s, ok := v.(string); ok {
			i, err := strconv.ParseInt(strings.Trim(s, " \t\n"), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("column: %s: expected an integer: %v: %s", c.Name, v,
					err.Error())
			}
			return i, nil
		} else if _, ok := v.(int64); !ok {
			return nil, fmt.Errorf("column: %s: expected an integer value: %v", c.Name, v)
		}
	}

	return v, nil
}
