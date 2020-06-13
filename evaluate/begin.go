package evaluate

import (
	"github.com/leftmike/maho/engine"
)

type Begin struct{}

func (stmt *Begin) String() string {
	return "BEGIN"
}

func (stmt *Begin) Plan(ses *Session, tx engine.Transaction) (interface{}, error) {
	panic("do not call; begin handled by session")
	return nil, nil
}

func (stmt *Begin) Command(ses *Session) error {
	panic("do not call; begin handled by session")
	return nil
}
