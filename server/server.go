package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

var ErrServerClosed = errors.New("server: closed")

type Handler func(ses *evaluate.Session, rr io.RuneReader, w io.Writer)

type Server struct {
	Handler         Handler
	Engine          sql.Engine
	DefaultDatabase sql.Identifier

	mutex         sync.Mutex
	listeners     map[net.Listener]struct{}
	activeConn    map[io.Closer]struct{}
	connCount     int32
	shutdown      bool
	closed        bool
	sessions      map[*evaluate.Session]struct{}
	lastSessionID uint64
}

func (svr *Server) addListener(l net.Listener) {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	if svr.listeners == nil {
		svr.listeners = map[net.Listener]struct{}{}
		svr.activeConn = map[io.Closer]struct{}{}
	}
	svr.listeners[l] = struct{}{}
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

func (svr *Server) Handle(rr io.RuneReader, w io.Writer, user, typ, addr string) {
	ses := evaluate.NewSession(svr.Engine, svr.DefaultDatabase, sql.PUBLIC)
	ses.User = user
	ses.Type = typ
	ses.Addr = addr

	svr.addSession(ses)
	svr.Handler(ses, rr, w)
	svr.removeSession(ses)
}

func (svr *Server) trackConn(conn io.Closer, add bool) bool {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	if svr.closed {
		return false
	}
	if add {
		svr.activeConn[conn] = struct{}{}
	} else {
		delete(svr.activeConn, conn)
	}
	return true
}

func (svr *Server) Close() error {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	if svr.closed {
		return nil
	}
	svr.closed = true

	var err error
	if !svr.shutdown {
		for l := range svr.listeners {
			e := l.Close()
			if e != nil && err == nil {
				err = e
			}
		}
		svr.shutdown = true
	}

	for conn := range svr.activeConn {
		e := conn.Close()
		if e != nil && err == nil {
			err = e
		}
		delete(svr.activeConn, conn)
	}
	return err
}

func (svr *Server) Shutdown(ctx context.Context) error {
	svr.mutex.Lock()
	if svr.closed {
		svr.mutex.Unlock()
		return nil
	}

	var err error
	if !svr.shutdown {
		for l := range svr.listeners {
			e := l.Close()
			if e != nil && err == nil {
				err = e
			}
		}
		svr.shutdown = true
	}
	svr.mutex.Unlock()

	last := int32(-1)
	for {
		cc := atomic.LoadInt32(&svr.connCount)
		if cc == 0 {
			break
		}
		if cc != last {
			p := ""
			if cc > 1 {
				p = "s"
			}
			fmt.Printf("%d active connection%s\n", cc, p)
			last = cc
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

func (svr *Server) makeSessionsVirtual(ctx context.Context, tx sql.Transaction,
	tn sql.TableName) (sql.Table, sql.TableType, error) {

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
		})
	}

	return engine.MakeVirtualTable(tn,
		[]sql.Identifier{sql.ID("session"), sql.ID("user"), sql.ID("type"), sql.ID("address")},
		[]sql.ColumnType{sql.StringColType, sql.IdColType, sql.IdColType,
			sql.NullStringColType}, values)
}
