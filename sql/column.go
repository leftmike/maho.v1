package sql

import (
	"fmt"
	"math"
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

	v, err := c.Type.ConvertTo(v)
	if err != nil {
		return nil, fmt.Errorf("column: %s: %s", c.Name, err)
	}
	return v, nil
}
