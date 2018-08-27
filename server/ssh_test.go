package server_test

import (
	"fmt"
	"io"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/server"
)

const (
	id_rsa1 = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA0Emzd3rA+khdpliq6TRsgxYs/9k2D8u6TIVUDeUtAkyD14br
3W0gEGHsYeAP5HjKpgIJ9rjtGJcjA/aNfuOQ7aMeBoWIigqK2Utxma8KkkrZP0MM
o8m557cS19OR61gyiuMGBuZ6jA4JC5aKPS/b8XFFsCr8hBAniqCnhsb4PAGg37N4
2s+0mwhJd+WPzSp7XnJMGeIccv/AnOHLXRts5vCpl9rVGp61gSILo5HknWuNEOrr
sndjFWtB/yJlcsTl51QxPTQIv+rHkU/BXIRQv4aQpKU9ByM6lvyrDSvrikbJpHZK
6OQmPQyNul1YEHMCI73o275GNKkqgXxiPe1/hQIDAQABAoIBAQC6QB0+CsODSrCL
biAudpVNxseoZKgmCcvmXxbhcfwEYT1Hvbst/kW8wIOqpbwwVh8HwSz9tTE2WveR
tKekPoO7K7BOrpuArJqqYf8MKOzwEgQfwKoNz/XwENHFssd5xh0z+nvKMdCFaouG
FDA7NI+dX+er91RkFzn0iWIkb8lLhZ9foN6hqIuLqnghuSTcOj+UiGvRoZNIiBIt
DRtao9BYOGT0Cjoha7zCiZB0snnO6++s2LemvBiHrnFyyIvuQYuMA9+GlFFG6Ps+
nDC26FbxJq1Pt08jsEv3da4xDmRfHboDpof3qxphbgITXdXpc4knswzj6Bh0e6+D
ZFXI73LBAoGBAPa17ACNypc9/5VsFoMjW8K27eMVTdLhwMm8R1qN7oh3v/7F/+GK
BWdCL6iZEGLxHpKHz9ePb/yoB/Ah68xZmY4uqo8pK4tKAe8lHbqQSuqlZuv9OSPI
bRkCv4FBbApuIVIW7W3Oq0UgKxhfaEQlJ/qULOztHObMtoxyo1AJB8dRAoGBANgh
asNaBP3So65cremrxdY864qtDp2LaaGvSrPxj/9tJ9RLq8oolYEeagKzICM34DZS
duL8eqU9vOJRDc2SdsW0tRkF10tGkpvM0m+x9hdvJaFVEj8AOEwVnUPEXUq8LnGF
2kZAtRA4FLS9IxI8bMx+tTeb1im2OD2ZeqKAbg/1AoGAUl26RRURphzUz5Yejfmo
EQHxufbK3LTyIGfzipAIKxePygdgvnPOCgNW1fsENYQ6qMEe6uDAiqGuxMUObFMd
qRJ7bwBXwVEcBDNoEXlLCbve+Lq58PBhPBBmz0iAQZszFK0C574wAAwVVzXDIb0l
88q0lQU0uPlvmMRNMdNR/YECgYA1H/mrLPxfG6CKIlPJRSSq5WyMOL+H8BW13W2C
T5UylrUclMEUPueIxl4ObdMvdPOSxrBcWO0YotVD1+KJrj+tlx6QmQPzh9RPoYf0
Vo0D85kPT+bdP4OXCBUQ7Dp2PU05MEqXzB31b6N/TaJKQDoKBfcrRQ3eUOEAGzKu
sNzc5QKBgDmspV4qB5oWwV1BRQa9SMMYiebl20fQcIkBiV8QbpiGzxilWWblThXx
GSYMUpwiBQluJc1g+chHVGJ29Fg6Mn9RiPxXnm58Sf1f9W2yl+9OzflMuy3bkppR
dx3aR+566fGKtIZPrg0AFRXmIFehsXb93wRY3U8fXAxwsdSn5Cgu
-----END RSA PRIVATE KEY-----
`
	id_rsa1_pub = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDQSbN3esD6SF2mWKrpNGyDFiz/2TYPy7pMhVQN5S0CTIPXhuvdbSAQYexh4A/keMqmAgn2uO0YlyMD9o1+45Dtox4GhYiKCorZS3GZrwqSStk/QwyjybnntxLX05HrWDKK4wYG5nqMDgkLloo9L9vxcUWwKvyEECeKoKeGxvg8AaDfs3jaz7SbCEl35Y/NKnteckwZ4hxy/8Cc4ctdG2zm8KmX2tUanrWBIgujkeSda40Q6uuyd2MVa0H/ImVyxOXnVDE9NAi/6seRT8FchFC/hpCkpT0HIzqW/KsNK+uKRsmkdkro5CY9DI26XVgQcwIjvejbvkY0qSqBfGI97X+F no comment"

	id_rsa2 = `-----BEGIN RSA PRIVATE KEY-----
MIIEpgIBAAKCAQEA5tsShCHs8Fvz+cE37H46rxpEkaXmax1OOItBd07Aw1j4HtQI
1sapCWnL1Ym6G7OBy8LEvJp4bp/BR2PQDPzMGA7rE+RyowiOmBg586Ma6DWFYgxy
acBv2nwiUTbhVaEL82Z7IMZrKNlMdewZw5gD5+ZiLyB+gAIxEq+jMLTURhM+aTKL
QAcXulO+/M8Y4Azoqvv45GS0GtFinfeSdDTzuckO1PDUQsccG2gFWme6ObDsHCnB
LAFb3JtZGc7XH8jQrVj4/fOY9NEnXH0RC7AhCXgxwoEBnvPuBpyq/FsZAWZ0x5io
qqLawTkNkxiQCLOVvhsegL1mS5GlbS1lnNSBuwIDAQABAoIBAQDIy2WYjhWTnrdf
1gK5bbXHVfasJHt9UXkWz3D6wAIEJ59w2QZzIUonyOCldGzu6TyNKXSLg2Qa/FNV
r+hnU36FsnwfykU3rxqwCepmEsk1jk/cz2y4tMvVYsFKmIx4OWK2q72O7WaJmi/a
JajnKpLDIuzlhsLgvodGdIRhufcklrM5ldqu3x80ZVo5jz8NabrgPdHU9T8zibrq
EluKUrfVPAYrlA4282d+H6TIm2NtgrKOdkVUJG6BvVzSLvS73gF6BWg7bWQAtl9w
cTuqk12BH4+RoOYt/kvnjfOsz5rKgAA6WbDF1dvK9Td9wsehwCnEamuleE8HC9An
+A1fzsq5AoGBAPvQzNlrcLS7b5CXUGEcJEFYMa8DhmVr3wOyg4UY4HB6sepOljef
qHKOFbm15WWaimWvZxhmUIXTYIUuD+pUoBg4p5BxpjOcOc/OUJvs4eiNB78l95/m
4zrc2RftpAi1RRnfrNJfQ8H03AbR9HgPbhchlGViCX/0jyp5+rIA4bg9AoGBAOqx
HF54SPPi3pnDU+FIEhhdxH2KDXwIzYZPpWjwQTbVGogUEz3wqt1V1au+hSjjjDgE
ElgIBPDBthpfg/10ugL07wz0fG2YddtDdWCTLQ1UeBjk4ml4j3sjgRD6GswBw9A+
uBB7WbNLMYUnTQSuufYtT3z2reEtmnxn8w3Aw8lXAoGBAOkD2csG5JAZMa92lWaH
B/V/itoMRPzsyL0/Hvy2fFkY8DGE7vQhxVnbqLbkXpWaX56gpKGra6+qXZJfgVKx
ZEOFiWjcAZOYaNamb2kZ1iG+/wAePfm1SWdanXjK6hM0yfCbTeyNQsHjtwaImdPP
U2wMIql+ApRo1WHU5ep5bmVxAoGBAIXdEcZcq5fYjol73StBVXjqevn0NW7LApea
dEmfyELDWIfHk5Yf4QWwQNjeKcvBxqfowqHNqSu+AeWXF40I+FRZasSj6xkD/i1b
k+bK68aPqJTicLYiGwzsmmCZl5FHbG4qaAiWovpeaFd/rDGAi/d7rrwjY9htj5Fo
JT1x9vqvAoGBAKnGbCjsSv4E5/efjO/ApFFTj0S9gHEclw2cwF/AccuST8ZGaXcd
CMzn/q7I4sCod+TMX1YOXlprjMOYCx0LrKJFVl91bG0pIm2kKCF+hTiqolMBrjnW
hYMlsXEL8gabXe1xiH9CJ5+n0Q7D/nvwrdQS3MKVU5qcJXWIb/hPygwP
-----END RSA PRIVATE KEY-----
`
	id_rsa2_pub = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDm2xKEIezwW/P5wTfsfjqvGkSRpeZrHU44i0F3TsDDWPge1AjWxqkJacvVibobs4HLwsS8mnhun8FHY9AM/MwYDusT5HKjCI6YGDnzoxroNYViDHJpwG/afCJRNuFVoQvzZnsgxmso2Ux17BnDmAPn5mIvIH6AAjESr6MwtNRGEz5pMotABxe6U778zxjgDOiq+/jkZLQa0WKd95J0NPO5yQ7U8NRCxxwbaAVaZ7o5sOwcKcEsAVvcm1kZztcfyNCtWPj985j00SdcfRELsCEJeDHCgQGe8+4GnKr8WxkBZnTHmKiqotrBOQ2TGJAIs5W+Gx6AvWZLkaVtLWWc1IG7 no comment"
)

func testSSHServer(t *testing.T, fail bool, cfg *ssh.ClientConfig, port int, authorizedBytes []byte,
	checkPassword func(user, password string) error) {
	t.Helper()

	hostKeysBytes := [][]byte{([]byte)(id_rsa1)}
	publicKey, _, _, _, _ := ssh.ParseAuthorizedKey(([]byte)(id_rsa1_pub))
	addr := fmt.Sprintf("localhost:%d", port)

	served := make(chan struct{}, 1)
	s := server.Server{
		Handler: func(ses *evaluate.Session, rr io.RuneReader, w io.Writer) {
			served <- struct{}{}
		},
		Manager: engine.NewManager("testdata", map[string]engine.Engine{}),
	}

	go func() {
		err := s.ListenAndServeSSH(
			server.SSHConfig{
				Address:         addr,
				HostKeysBytes:   hostKeysBytes,
				AuthorizedBytes: authorizedBytes,
				CheckPassword:   checkPassword,
			})
		if err != server.ErrServerClosed {
			t.Fatalf("ListenAndServe() returned with %s", err)
		}
	}()

	cfg.User = "testing"
	cfg.HostKeyCallback = ssh.FixedHostKey(publicKey)

	conn, err := ssh.Dial("tcp", addr, cfg)
	if fail {
		if err == nil {
			t.Fatal("Dial() did not fail")
		}
	} else {
		if err != nil {
			t.Fatalf("Dial() failed with %s", err)
		}
		sess, err := conn.NewSession()
		if err != nil {
			t.Fatalf("NewSession() failed with %s", err)
		}
		sess.Close()
		<-served
	}
	s.Close()
}

func TestSSHServer(t *testing.T) {
	cfg1 := ssh.ClientConfig{}
	testSSHServer(t, false, &cfg1, 10001, nil, nil)

	cfg2 := ssh.ClientConfig{}
	testSSHServer(t, true, &cfg2, 10002, nil,
		func(user, password string) error {
			return fmt.Errorf("failed")
		})

	cfg3 := ssh.ClientConfig{
		Auth: []ssh.AuthMethod{ssh.Password("default")},
	}
	testSSHServer(t, false, &cfg3, 10003, nil,
		func(user, password string) error {
			if user != "testing" || password != "default" {
				return fmt.Errorf("failed")
			}
			return nil
		})

	key, _ := ssh.ParsePrivateKey(([]byte)(id_rsa2))
	cfg4 := ssh.ClientConfig{
		Auth: []ssh.AuthMethod{ssh.PublicKeys(key)},
	}
	testSSHServer(t, false, &cfg4, 10004, ([]byte)(id_rsa2_pub), nil)
}
