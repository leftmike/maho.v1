package execute

import (
	"context"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type Session interface {
	engine.Session
	Transaction() *engine.Transaction
	SetTransaction(tx *engine.Transaction)
}

type session struct {
	eng  string
	name sql.Identifier
	tx   *engine.Transaction
}

func NewSession(eng string, name sql.Identifier) Session {
	return &session{
		eng:  eng,
		name: name,
	}
}

func (ses *session) Context() context.Context {
	return nil
}

func (ses *session) DefaultEngine() string {
	return ses.eng
}

func (ses *session) DefaultDatabase() sql.Identifier {
	return ses.name
}

func (ses *session) Transaction() *engine.Transaction {
	return ses.tx
}

func (ses *session) SetTransaction(tx *engine.Transaction) {
	ses.tx = tx
}
