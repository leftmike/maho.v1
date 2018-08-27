package server

import (
	"context"
	"errors"
	"io"
	"sync"

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

	mutex   sync.Mutex
	servers map[server]struct{}
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
	// XXX: need to keep track of the session
	svr.Handler(ses, rr, w)
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
