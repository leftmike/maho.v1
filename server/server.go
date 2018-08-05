package server

import (
	"context"
	"io"
	"net"
)

type Client struct {
	RuneReader io.RuneReader
	Writer     io.Writer
	User       string
	Type       string
	Addr       net.Addr
}

type Handler interface {
	Serve(c *Client)
}

type HandlerFunc func(c *Client)

func (f HandlerFunc) Serve(c *Client) {
	f(c)
}

type Server interface {
	Close() error
	ListenAndServe(handler Handler) error
	Shutdown(ctx context.Context) error
}
