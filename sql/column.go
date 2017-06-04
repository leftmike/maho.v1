package sql

import (
	"fmt"
	"math"
)

type Column struct {
	Name Identifier
	Type DataType

	// Size of the column in bytes for integers and in characters for character columns
	Size     uint32
	Width    uint8 // display width of integers and doubles
	Fraction uint8 // display width of fractional part of doubles
	Fixed    bool  // fixed sized character column
	Binary   bool  // binary character column
}

func (col Column) DataType() string {
	switch col.Type {
	case BooleanType:
		return "BOOL"
	case CharacterType:
		if col.Binary {
			if col.Fixed {
				return fmt.Sprintf("BINARY(%d)", col.Size)
			} else if col.Size == math.MaxUint32-1 {
				return "BLOB"
			} else {
				return fmt.Sprintf("VARBINARY(%d)", col.Size)
			}
		} else {
			if col.Fixed {
				return fmt.Sprintf("CHAR(%d)", col.Size)
			} else if col.Size == math.MaxUint32-1 {
				return "TEXT"
			} else {
				return fmt.Sprintf("VARCHAR(%d)", col.Size)
			}
		}
	case DoubleType:
		return "DOUBLE"
	case IntegerType:
		switch col.Size {
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
