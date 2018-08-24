package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/terminal"
)

type sshServer struct {
	mutex      sync.Mutex
	cfg        *ssh.ServerConfig
	port       string
	prompt     string
	listener   net.Listener
	activeConn map[*ssh.ServerConn]struct{}
	done       bool
}

func NewSSHServer(port string, hostKeysBytes [][]byte, prompt string, authorizedBytes []byte,
	checkPassword func(user, password string) error) (Server, error) {

	cfg := ssh.ServerConfig{
		AuthLogCallback: func(md ssh.ConnMetadata, method string, err error) {
			if method != "none" {
				l := log.WithFields(log.Fields{
					"user":   md.User(),
					"addr":   md.RemoteAddr().String(),
					"method": method,
				})
				if err != nil {
					l.WithField("error", err.Error()).Error("authentication failed")
				} else {
					l.Info("authentication succeeded")
				}
			}
		},
		BannerCallback: func(md ssh.ConnMetadata) string {
			return "maho 0.1\n"
		},
	}

	for _, keyBytes := range hostKeysBytes {
		key, err := ssh.ParsePrivateKey(keyBytes)
		if err != nil {
			return nil, err
		}
		cfg.AddHostKey(key)
	}

	authorizedKeys := map[string]struct{}{}
	for len(authorizedBytes) > 0 {
		key, _, _, rest, err := ssh.ParseAuthorizedKey(authorizedBytes)
		if err != nil {
			return nil, err
		}
		authorizedKeys[string(key.Marshal())] = struct{}{}
		authorizedBytes = rest
	}

	if checkPassword == nil && len(authorizedKeys) == 0 {
		cfg.NoClientAuth = true
		log.Warn("ssh client auth: NONE")
	}

	if checkPassword != nil {
		cfg.PasswordCallback =
			func(md ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
				user := md.User()
				password := string(pass)
				log.WithFields(log.Fields{
					"user":     user,
					"password": password,
					"addr":     md.RemoteAddr().String(),
				}).Debug("ssh password callback")
				return nil, checkPassword(user, password)
			}
		log.Info("ssh client auth: password")
	}

	if len(authorizedKeys) > 0 {
		cfg.PublicKeyCallback =
			func(md ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
				pk := string(key.Marshal())
				log.WithFields(log.Fields{
					"user":       md.User(),
					"public key": pk,
					"addr":       md.RemoteAddr().String(),
				}).Debug("ssh public key callback")
				if _, ok := authorizedKeys[pk]; !ok {
					return nil, fmt.Errorf("unknown public key for %s", md.User())
				}
				return nil, nil
			}
		log.Info("ssh client auth: public key")
	}

	return &sshServer{
		cfg:        &cfg,
		port:       port,
		prompt:     prompt,
		activeConn: map[*ssh.ServerConn]struct{}{},
	}, nil
}

func (ss *sshServer) ListenAndServe(handler Handler) error {
	var err error
	ss.listener, err = net.Listen("tcp", ss.port)
	if err != nil {
		return err
	}

	for {
		tcp, err := ss.listener.Accept()
		if err != nil {
			ss.mutex.Lock()
			if ss.done {
				err = ErrServerClosed
			}
			ss.mutex.Unlock()
			log.WithField("error", err.Error()).Error("ssh accept")
			return err
		}
		conn, chans, reqs, err := ssh.NewServerConn(tcp, ss.cfg)
		if err != nil {
			log.WithField("error", err.Error()).Error("ssh new server connection")
			continue
		}
		entry := log.WithFields(log.Fields{
			"user": conn.User(),
			"addr": conn.RemoteAddr().String(),
		})
		entry.Info("ssh connection")

		go ssh.DiscardRequests(reqs)
		go ss.handleConn(conn, chans, handler, entry)
	}
}

func (ss *sshServer) trackConn(conn *ssh.ServerConn, add bool) bool {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()
	if ss.done {
		return false
	}
	if add {
		ss.activeConn[conn] = struct{}{}
	} else {
		delete(ss.activeConn, conn)
	}
	return true
}

func (ss *sshServer) handleConn(conn *ssh.ServerConn, chans <-chan ssh.NewChannel,
	handler Handler, entry *log.Entry) {

	if ss.trackConn(conn, true) {
		for ch := range chans {
			go ss.handleChannel(conn, ch, handler, entry)
		}
		if ss.trackConn(conn, false) {
			conn.Close()
		}
	} else {
		conn.Close()
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

func (ss *sshServer) handleChannel(conn *ssh.ServerConn, nch ssh.NewChannel, handler Handler,
	entry *log.Entry) {

	typ := nch.ChannelType()
	if typ != "session" {
		nch.Reject(ssh.UnknownChannelType, typ)
		entry.WithField("channel-type", typ).Error("unknown channel type")
		return
	}
	entry.WithField("channel-type", typ).Debug("new channel")

	ch, reqs, err := nch.Accept()
	if err != nil {
		entry.WithField("error", err.Error()).Error("new channel accept")
		return
	}
	defer ch.Close()

	go func() {
		for req := range reqs {
			entry.WithFields(log.Fields{
				"request-type": req.Type,
				"want-reply":   req.WantReply,
				"payload":      len(req.Payload),
			}).Debug("channel request")
			if req.WantReply {
				req.Reply(true, nil)
			}
		}
		entry.Debug("channel requests done")
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
	ss.mutex.Lock()
	defer ss.mutex.Unlock()
	ss.done = true
	err := ss.listener.Close()
	for conn := range ss.activeConn {
		conn.Close()
		delete(ss.activeConn, conn)
	}
	return err
}

func (ss *sshServer) Shutdown(ctx context.Context) error {
	return fmt.Errorf("ssh server: shutdown: not implemented yet") // XXX
}
