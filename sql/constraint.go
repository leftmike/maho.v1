package sql

import (
	"fmt"
)

type ConstraintType int

const (
	DefaultConstraint ConstraintType = iota + 1
	NotNullConstraint
	PrimaryConstraint
	UniqueConstraint
	CheckConstraint
)

type OutgoingFKRef struct {
	Name    Identifier
	Columns []int      // Foreign key columns in the outgoing table, ordered to match the index
	Table   TableName  // Incoming table
	Index   Identifier // Referenced index on the incoming table; 0 indicates primary
}

type IncomingFKRef struct {
	Name         Identifier // Name of the constraint in the outgoing table
	OutgoingCols []int      // Foreign key columns in the outgoing table
	Table        TableName  // Outgoing table
	IncomingCols []int      // Referenced columns in the incoming table
}

type Constraint struct {
	Type      ConstraintType
	Name      Identifier
	ColNum    int         // Default, NotNull, and Column Check constraints
	Key       []ColumnKey // Primary and Unique constraints
	Check     CExpr       // Check constraints
	CheckExpr string
}

func (ct ConstraintType) String() string {
	switch ct {
	case DefaultConstraint:
		return "DEFAULT"
	case NotNullConstraint:
		return "NOT NULL"
	case PrimaryConstraint:
		return "PRIMARY KEY"
	case UniqueConstraint:
		return "UNIQUE"
	case CheckConstraint:
		return "CHECK"
	default:
		panic(fmt.Sprintf("unexpected constraint type: %d", ct))
	}
}
