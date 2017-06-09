package sql

type Null struct{}

func (d Null) String() string {
	return "NULL"
}
