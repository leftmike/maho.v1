package sql

import "fmt"

type ConstraintType int

const (
	DefaultConstraint ConstraintType = iota + 1
	NotNullConstraint
	PrimaryConstraint
	UniqueConstraint
	CheckConstraint
	ForeignConstraint
)

type Constraint struct {
	Type   ConstraintType
	Name   Identifier
	ColNum int
	Key    []ColumnKey // Primary and unique constraints
	Check  Expr        // Check constraints
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
