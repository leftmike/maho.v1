package db

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/sql"
)

const (
	MaxColumnSize = math.MaxUint32 - 1
)

type ColumnType struct {
	Type sql.DataType

	// Size of the column in bytes for integers and in characters for character columns
	Size   uint32
	Fixed  bool // fixed sized character column
	Binary bool // binary character column

	NotNull bool // not allowed to be NULL
	Default expr.Expr
}

func (ct ColumnType) DataType() string {
	switch ct.Type {
	case sql.BooleanType:
		return "BOOL"
	case sql.CharacterType:
		if ct.Binary {
			if ct.Fixed {
				return fmt.Sprintf("BINARY(%d)", ct.Size)
			} else if ct.Size == MaxColumnSize {
				return "BLOB"
			} else {
				return fmt.Sprintf("VARBINARY(%d)", ct.Size)
			}
		} else {
			if ct.Fixed {
				return fmt.Sprintf("CHAR(%d)", ct.Size)
			} else if ct.Size == MaxColumnSize {
				return "TEXT"
			} else {
				return fmt.Sprintf("VARCHAR(%d)", ct.Size)
			}
		}
	case sql.DoubleType:
		return "DOUBLE"
	case sql.IntegerType:
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

func (ct ColumnType) ConvertValue(n sql.Identifier, v sql.Value) (sql.Value, error) {
	if v == nil {
		if ct.NotNull {
			return nil, fmt.Errorf("column may not be NULL: %s", n)
		}
		return nil, nil
	}

	switch ct.Type {
	case sql.BooleanType:
		if sv, ok := v.(sql.StringValue); ok {
			s := strings.Trim(string(sv), " \t\n")
			if s == "t" || s == "true" || s == "y" || s == "yes" || s == "on" || s == "1" {
				return sql.BoolValue(true), nil
			} else if s == "f" || s == "false" || s == "n" || s == "no" || s == "off" || s == "0" {
				return sql.BoolValue(false), nil
			} else {
				return nil, fmt.Errorf("column: %s: expected a boolean value: %v", n, v)
			}
		} else if _, ok := v.(sql.BoolValue); !ok {
			return nil, fmt.Errorf("column: %s: expected a boolean value: %v", n, v)
		}
	case sql.CharacterType:
		if i, ok := v.(sql.Int64Value); ok {
			return sql.StringValue(strconv.FormatInt(int64(i), 10)), nil
		} else if f, ok := v.(sql.Float64Value); ok {
			return sql.StringValue(strconv.FormatFloat(float64(f), 'g', -1, 64)), nil
		} else if _, ok := v.(sql.StringValue); !ok {
			return nil, fmt.Errorf("column: %s: expected a string value: %v", n, v)
		}
	case sql.DoubleType:
		if i, ok := v.(sql.Int64Value); ok {
			return sql.Float64Value(i), nil
		} else if s, ok := v.(sql.StringValue); ok {
			d, err := strconv.ParseFloat(strings.Trim(string(s), " \t\n"), 64)
			if err != nil {
				return nil, fmt.Errorf("column: %s: expected a float: %v: %s", n, v,
					err.Error())
			}
			return sql.Float64Value(d), nil
		} else if _, ok := v.(sql.Float64Value); !ok {
			return nil, fmt.Errorf("column: %s: expected a float value: %v", n, v)
		}
	case sql.IntegerType:
		if f, ok := v.(sql.Float64Value); ok {
			return sql.Int64Value(f), nil
		} else if s, ok := v.(sql.StringValue); ok {
			i, err := strconv.ParseInt(strings.Trim(string(s), " \t\n"), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("column: %s: expected an integer: %v: %s", n, v,
					err.Error())
			}
			return sql.Int64Value(i), nil
		} else if _, ok := v.(sql.Int64Value); !ok {
			return nil, fmt.Errorf("column: %s: expected an integer value: %v", n, v)
		}
	}

	return v, nil
}
