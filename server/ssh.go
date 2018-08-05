package server

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"net"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/leftmike/maho/engine"
)

type sshServer struct {
	cfg    *ssh.ServerConfig
	port   string
	prompt string
	mgr    *engine.Manager
}

func NewSSHServer(mgr *engine.Manager, port string, hostKeys []string, prompt string) (Server,
	error) {

	cfg := ssh.ServerConfig{
		//NoClientAuth: true, // XXX: remove this
		PasswordCallback: func(md ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			fmt.Printf("user: %s password: %s\n", md.User(), pass)
			return nil, nil
		},
		PublicKeyCallback: func(md ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			fmt.Printf("user: %s public key: %s\n", md.User(), ssh.MarshalAuthorizedKey(key))
			if md.User() != "michael" {
				return nil, fmt.Errorf("user must be michael")
			}
			return nil, nil
		},
		AuthLogCallback: func(md ssh.ConnMetadata, method string, err error) {
			addr := md.RemoteAddr()
			fmt.Printf("auth log: user: %s addr: %s:%s method: %s", md.User(), addr.Network(),
				addr.String(), method)
			if err != nil {
				fmt.Printf(" error: %s\n", err)
			} else {
				fmt.Println()
			}
		},
		BannerCallback: func(md ssh.ConnMetadata) string {
			return "maho <version>\n"
		},
	}

	for _, hostKey := range hostKeys {
		keyBytes, err := ioutil.ReadFile(hostKey)
		if err != nil {
			return nil, err
		}

		key, err := ssh.ParsePrivateKey(keyBytes)
		if err != nil {
			return nil, err
		}
		cfg.AddHostKey(key)
	}

	return &sshServer{
		cfg:    &cfg,
		port:   port,
		prompt: prompt,
		mgr:    mgr,
	}, nil
}

func (ss *sshServer) ListenAndServe(handler Handler) error {
	listener, err := net.Listen("tcp", ss.port)
	if err != nil {
		return err
	}

	for {
		tcp, err := listener.Accept()
		if err != nil {
			fmt.Printf("listener.Accept: %s\n", err)
			continue
		}
		conn, chans, reqs, err := ssh.NewServerConn(tcp, ss.cfg)
		if err != nil {
			fmt.Printf("ssh.NewServerConn: %s\n", err)
			continue
		}
		fmt.Printf("ssh connection: %s %s %s\n", conn.RemoteAddr(), conn.ClientVersion(),
			conn.User())

		go ssh.DiscardRequests(reqs)
		go ss.handleConn(conn, chans, handler)
	}
}

func (ss *sshServer) handleConn(conn *ssh.ServerConn, chans <-chan ssh.NewChannel,
	handler Handler) {

	for ch := range chans {
		go ss.handleChannel(conn, ch, handler)
	}
}

type termReader struct {
	term *terminal.Terminal
	line []byte
}

func (tr *termReader) Read(d []byte) (int, error) {
	if len(tr.line) == 0 {
		line, err := tr.term.ReadLine()
		if err != nil {
			return 0, err
		}
		tr.line = []byte(line + "\n")
	}

	n := len(d)
	if n > len(tr.line) {
		n = len(tr.line)
	}

	copy(d, tr.line[:n])
	tr.line = tr.line[n:]
	return n, nil
}

func (ss *sshServer) handleChannel(conn *ssh.ServerConn, nch ssh.NewChannel, handler Handler) {
	typ := nch.ChannelType()
	fmt.Printf("ssh channel type: %s\n", typ)
	if typ != "session" {
		nch.Reject(ssh.UnknownChannelType, typ)
		return
	}

	ch, reqs, err := nch.Accept()
	if err != nil {
		fmt.Printf("nch.Accept: %s\n", err)
		return
	}
	defer ch.Close()

	go func() {
		for req := range reqs {
			fmt.Printf("channel request: %s %v [%s]\n", req.Type, req.WantReply, req.Payload)
			if req.WantReply {
				req.Reply(true, nil)
			}
		}
		fmt.Printf("channel requests done\n")
	}()

	t := terminal.NewTerminal(ch, ss.prompt)
	tr := termReader{
		term: t,
	}
	handler.Serve(&Client{
		RuneReader: bufio.NewReader(&tr),
		Writer:     t,
		User:       conn.User(),
		Type:       "ssh",
		Addr:       conn.RemoteAddr(),
	})
}

func (ss *sshServer) Close() error {
	return fmt.Errorf("ssh server: close: not implemented yet") // XXX
}

func (ss *sshServer) Shutdown(ctx context.Context) error {
	return fmt.Errorf("ssh server: shutdown: not implemented yet") // XXX
}
