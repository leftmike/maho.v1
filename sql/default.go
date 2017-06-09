package sql

type Default struct{}

func (d Default) String() string {
	return "DEFAULT"
}
