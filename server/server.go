package server

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

var ErrServerClosed = errors.New("server: closed")

type Handler func(ses *evaluate.Session, rr io.RuneReader, w io.Writer)

type Server struct {
	Handler         Handler
	Engine          engine.Engine
	DefaultDatabase sql.Identifier

	mutex         sync.Mutex
	servers       map[server]struct{}
	sessions      map[*evaluate.Session]struct{}
	lastSessionID uint64
}

type server interface {
	Close() error
	Shutdown(ctx context.Context) error
}

func (svr *Server) addServer(s server) {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	if svr.servers == nil {
		svr.servers = map[server]struct{}{}
	}
	svr.servers[s] = struct{}{}
}

func (svr *Server) addSession(ses *evaluate.Session) {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	if svr.sessions == nil {
		svr.sessions = map[*evaluate.Session]struct{}{}
		svr.Engine.CreateSystemInfoTable(sql.ID("sessions"), svr.makeSessionsVirtual)
	}
	svr.sessions[ses] = struct{}{}
	svr.lastSessionID += 1
	ses.SetSessionID(svr.lastSessionID)
}

func (svr *Server) removeSession(ses *evaluate.Session) {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	delete(svr.sessions, ses)
}

func (svr *Server) Handle(rr io.RuneReader, w io.Writer, user, typ, addr string, interactive bool) {
	ses := &evaluate.Session{
		Engine:          svr.Engine,
		DefaultDatabase: svr.DefaultDatabase,
		DefaultSchema:   sql.PUBLIC,
		User:            user,
		Type:            typ,
		Addr:            addr,
		Interactive:     interactive,
	}
	svr.addSession(ses)
	svr.Handler(ses, rr, w)
	svr.removeSession(ses)
}

func (svr *Server) closeShutdown(cs func(s server) error) error {
	svr.mutex.Lock()
	cnt := len(svr.servers)
	errors := make(chan error, cnt)

	for s := range svr.servers {
		go func(s server) {
			errors <- cs(s)
		}(s)
		delete(svr.servers, s)
	}
	svr.mutex.Unlock()

	var err error
	for cnt > 0 {
		var e error = <-errors
		if e != nil && err == nil {
			err = e
		}
		cnt -= 1
	}

	return err
}

func (svr *Server) Close() error {
	return svr.closeShutdown(
		func(s server) error {
			return s.Close()
		})
}

func (svr *Server) Shutdown(ctx context.Context) error {
	return svr.closeShutdown(
		func(s server) error {
			return s.Shutdown(ctx)
		})
}

func (svr *Server) makeSessionsVirtual(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	values := [][]sql.Value{}
	for ses := range svr.sessions {
		var addr sql.Value
		if ses.Addr != "" {
			addr = sql.StringValue(ses.Addr)
		}
		values = append(values, []sql.Value{
			sql.StringValue(ses.String()),
			sql.StringValue(ses.User),
			sql.StringValue(ses.Type),
			addr,
			sql.BoolValue(ses.Interactive),
		})
	}

	return virtual.MakeTable(tn,
		[]sql.Identifier{sql.ID("session"), sql.ID("user"), sql.ID("type"), sql.ID("address"),
			sql.ID("interactive")},
		[]sql.ColumnType{sql.StringColType, sql.IdColType, sql.IdColType,
			sql.NullStringColType, sql.BoolColType}, values), nil
}
