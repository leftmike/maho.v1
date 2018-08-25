package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
)

var ErrServerClosed = errors.New("server: closed")

type Client struct {
	RuneReader io.RuneReader
	Writer     io.Writer
	User       string
	Type       string
	Addr       net.Addr
}

type Handler func(c *Client)

type Server struct {
	Handler Handler
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
