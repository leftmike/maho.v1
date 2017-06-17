package sql

type Default struct{}

func (d Default) Format() string {
	return "DEFAULT"
}
