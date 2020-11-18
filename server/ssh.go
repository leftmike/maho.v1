package server

import (
	"bufio"
	"fmt"
	"net"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/leftmike/maho/repl"
)

type SSHConfig struct {
	Address         string
	HostKeysBytes   [][]byte
	AuthorizedBytes []byte
	CheckPassword   func(user, password string) error
}

func sshServerConfig(sshCfg SSHConfig) (*ssh.ServerConfig, error) {
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

	return &cfg, nil
}

func (svr *Server) ListenAndServeSSH(sshCfg SSHConfig) error {
	cfg, err := sshServerConfig(sshCfg)
	if err != nil {
		return err
	}

	l, err := net.Listen("tcp", sshCfg.Address)
	if err != nil {
		return err
	}
	svr.addListener(l)

	for {
		tcp, err := l.Accept()
		if err != nil {
			svr.mutex.Lock()
			if svr.shutdown {
				err = ErrServerClosed
			}
			svr.mutex.Unlock()
			log.WithField("error", err.Error()).Error("ssh accept")
			return err
		}
		conn, chans, reqs, err := ssh.NewServerConn(tcp, cfg)
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
		go svr.handleSSHConn(conn, chans, entry)
	}
}

func (svr *Server) handleSSHConn(conn *ssh.ServerConn, chans <-chan ssh.NewChannel,
	entry *log.Entry) {

	atomic.AddInt32(&svr.connCount, 1)
	defer atomic.AddInt32(&svr.connCount, -1)

	if svr.trackConn(conn, true) {
		for ch := range chans {
			go svr.handleSSHChannel(conn, ch, entry)
		}
		if svr.trackConn(conn, false) {
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

var (
	// Set by ssh_test.go
	sshHandler = repl.Handler
)

func (svr *Server) handleSSHChannel(conn *ssh.ServerConn, nch ssh.NewChannel, entry *log.Entry) {
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
	svr.HandleSession(sshHandler(bufio.NewReader(&tr), t), conn.User(), "ssh",
		conn.RemoteAddr().String())
}
