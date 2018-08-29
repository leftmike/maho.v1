package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

var ErrServerClosed = errors.New("server: closed")

type Handler func(ses *evaluate.Session, rr io.RuneReader, w io.Writer)

type Server struct {
	Handler         Handler
	Manager         *engine.Manager
	DefaultEngine   string
	DefaultDatabase sql.Identifier

	mutex    sync.Mutex
	servers  map[server]struct{}
	sessions map[*evaluate.Session]struct{}
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

func (svr *Server) closeShutdown(cs func(s server) error) error {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	cnt := len(svr.servers)
	errors := make(chan error, cnt)

	for s := range svr.servers {
		go func(s server) {
			errors <- cs(s)
		}(s)
		delete(svr.servers, s)
	}

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

func (svr *Server) addSession(ses *evaluate.Session) {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	if svr.sessions == nil {
		svr.sessions = map[*evaluate.Session]struct{}{}
		svr.Manager.CreateSystemTable(sql.ID("sessions"), svr.makeSessionsVirtual)
	}
	svr.sessions[ses] = struct{}{}
}

func (svr *Server) removeSession(ses *evaluate.Session) {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	delete(svr.sessions, ses)
}

func (svr *Server) makeSessionsVirtual(ses engine.Session, tctx interface{}, d engine.Database,
	dbname, tblname sql.Identifier) (engine.Table, error) {

	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	values := [][]sql.Value{}
	for ses := range svr.sessions {
		var addr sql.Value
		if ses.Addr != "" {
			addr = sql.StringValue(ses.Addr)
		}
		values = append(values, []sql.Value{
			sql.Int64Value((int64)((uintptr)(unsafe.Pointer(ses)))),
			sql.StringValue(ses.User),
			sql.StringValue(ses.Type),
			addr,
			sql.BoolValue(ses.Interactive),
		})
	}

	return &engine.VirtualTable{
		Name: fmt.Sprintf("%s.%s", dbname, tblname),
		Cols: []sql.Identifier{sql.ID("session"), sql.ID("user"), sql.ID("type"), sql.ID("address"),
			sql.ID("interactive")},
		ColTypes: []sql.ColumnType{sql.Int64ColType, sql.IdColType, sql.IdColType,
			sql.NullStringColType, sql.BoolColType},
		Values: values,
	}, nil
}

func (svr *Server) Handle(rr io.RuneReader, w io.Writer, user, typ, addr string, interactive bool) {
	ses := &evaluate.Session{
		Manager:         svr.Manager,
		DefaultEngine:   svr.DefaultEngine,
		DefaultDatabase: svr.DefaultDatabase,
		User:            user,
		Type:            typ,
		Addr:            addr,
		Interactive:     interactive,
	}
	svr.addSession(ses)
	svr.Handler(ses, rr, w)
	svr.removeSession(ses)
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
