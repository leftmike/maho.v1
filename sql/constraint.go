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
	ForeignConstraint
)

type ForeignKey struct {
	FromColumns []int
	Table       TableName
	ToColumns   []int
}

type Constraint struct {
	Type       ConstraintType
	Name       Identifier
	ColNum     int         // Default, NotNull, and Column Check constraints
	Key        []ColumnKey // Primary and Unique constraints
	Check      CExpr       // Check constraints
	CheckExpr  string
	ForeignKey ForeignKey
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
	case ForeignConstraint:
		return "FOREIGN KEY"
	default:
		panic(fmt.Sprintf("unexpected constraint type: %d", ct))
	}
}
