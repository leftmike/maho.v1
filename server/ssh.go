package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/terminal"
)

type SSHConfig struct {
	Address         string
	HostKeysBytes   [][]byte
	AuthorizedBytes []byte
	CheckPassword   func(user, password string) error
}

type sshServer struct {
	mutex      sync.Mutex
	cfg        *ssh.ServerConfig
	address    string
	listener   net.Listener
	activeConn map[*ssh.ServerConn]struct{}
	connCount  int32
	shutdown   bool
	closed     bool
}

func newSSHServer(sshCfg SSHConfig) (*sshServer, error) {
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

	for _, keyBytes := range sshCfg.HostKeysBytes {
		key, err := ssh.ParsePrivateKey(keyBytes)
		if err != nil {
			return nil, err
		}
		cfg.AddHostKey(key)
	}

	bytes := sshCfg.AuthorizedBytes
	authorizedKeys := map[string]struct{}{}
	for len(bytes) > 0 {
		key, _, _, rest, err := ssh.ParseAuthorizedKey(bytes)
		if err != nil {
			return nil, err
		}
		authorizedKeys[string(key.Marshal())] = struct{}{}
		bytes = rest
	}

	if sshCfg.CheckPassword == nil && len(authorizedKeys) == 0 {
		cfg.NoClientAuth = true
		log.Warn("ssh client auth: NONE")
	}

	if sshCfg.CheckPassword != nil {
		checkPassword := sshCfg.CheckPassword
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
		address:    sshCfg.Address,
		activeConn: map[*ssh.ServerConn]struct{}{},
	}, nil
}

func (svr *Server) ListenAndServeSSH(sshCfg SSHConfig) error {
	ss, err := newSSHServer(sshCfg)
	if err != nil {
		return err
	}

	ss.listener, err = net.Listen("tcp", ss.address)
	if err != nil {
		return err
	}
	svr.addServer(ss)

	for {
		tcp, err := ss.listener.Accept()
		if err != nil {
			ss.mutex.Lock()
			if ss.shutdown {
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
		entry.Info("ssh connected")

		go ssh.DiscardRequests(reqs)
		go ss.handleConn(conn, chans, svr, entry)
	}
}

func (ss *sshServer) trackConn(conn *ssh.ServerConn, add bool) bool {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	if ss.closed {
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
	svr *Server, entry *log.Entry) {

	atomic.AddInt32(&ss.connCount, 1)
	defer atomic.AddInt32(&ss.connCount, -1)

	if ss.trackConn(conn, true) {
		for ch := range chans {
			go ss.handleChannel(conn, ch, svr, entry)
		}
		if ss.trackConn(conn, false) {
			conn.Close()
		}
	} else {
		conn.Close()
	}
	entry.Info("ssh disconnected")
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

func (ss *sshServer) handleChannel(conn *ssh.ServerConn, nch ssh.NewChannel, svr *Server,
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

	t := terminal.NewTerminal(ch, "")
	tr := termReader{
		term: t,
	}
	svr.Handle(bufio.NewReader(&tr), t, conn.User(), "ssh", conn.RemoteAddr().String(), true)
}

func (ss *sshServer) Close() error {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	if ss.closed {
		return nil
	}
	ss.closed = true

	var err error
	if !ss.shutdown {
		err = ss.listener.Close()
		ss.shutdown = true
	}

	for conn := range ss.activeConn {
		conn.Close()
		delete(ss.activeConn, conn)
	}
	return err
}

func (ss *sshServer) Shutdown(ctx context.Context) error {
	var err error

	ss.mutex.Lock()
	if ss.closed {
		ss.mutex.Unlock()
		return nil
	}
	if !ss.shutdown {
		err = ss.listener.Close()
		ss.shutdown = true
	}
	ss.mutex.Unlock()

	last := int32(-1)
	for {
		cc := atomic.LoadInt32(&ss.connCount)
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
	return err
}
