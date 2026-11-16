package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"wosh/internal/stream"
	"wosh/internal/wire"
)

// Forward is an ssh-style port forward.
//
//	-L [bind:]port:host:hostport   local listener -> host:hostport dialed remotely
//	-R [bind:]port:host:hostport   remote listener -> host:hostport dialed here
//	-D [bind:]port                 local SOCKS5 proxy, connections dialed remotely
type Forward struct {
	Kind     byte // 'L', 'R', 'D'
	Bind     string
	Port     int
	Host     string
	HostPort int
	id       int
}

// ParseForward parses the argument of -L/-R/-D.
func ParseForward(kind byte, spec string) (Forward, error) {
	f := Forward{Kind: kind}
	parts := splitSpec(spec)
	var err error
	switch {
	case kind == 'D' && len(parts) == 1:
		f.Port, err = strconv.Atoi(parts[0])
	case kind == 'D' && len(parts) == 2:
		f.Bind = parts[0]
		f.Port, err = strconv.Atoi(parts[1])
	case kind != 'D' && (len(parts) == 3 || len(parts) == 4):
		if len(parts) == 4 {
			f.Bind, parts = parts[0], parts[1:]
		}
		if f.Port, err = strconv.Atoi(parts[0]); err == nil {
			f.Host = parts[1]
			f.HostPort, err = strconv.Atoi(parts[2])
		}
	default:
		err = errors.New("bad format")
	}
	if err != nil || f.Port < 0 || f.Port > 65535 || f.HostPort < 0 || f.HostPort > 65535 {
		return f, fmt.Errorf("-%c %s: %v", kind, spec, err)
	}
	if f.Bind == "*" {
		f.Bind = ""
	} else if f.Bind == "" {
		f.Bind = "localhost"
	}
	return f, nil
}

// splitSpec splits on ':' outside [ipv6] brackets.
func splitSpec(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '[':
			depth++
		case ']':
			depth--
		case ':':
			if depth == 0 {
				out = append(out, strings.Trim(s[start:i], "[]"))
				start = i + 1
			}
		}
	}
	return append(out, strings.Trim(s[start:], "[]"))
}

func (f Forward) String() string {
	if f.Kind == 'D' {
		return fmt.Sprintf("-D %s:%d", f.Bind, f.Port)
	}
	return fmt.Sprintf("-%c %s:%d:%s:%d", f.Kind, f.Bind, f.Port, f.Host, f.HostPort)
}

// prepareForwards assigns ids to -R forwards (sent to the root daemon on
// every connect).
func (c *Client) prepareForwards() error {
	for i := range c.o.Forwards {
		f := &c.o.Forwards[i]
		f.id = i + 1
		if f.Kind == 'R' {
			c.rfwds = append(c.rfwds, wire.RemoteFwd{ID: f.id, Bind: f.Bind, Port: f.Port})
		}
	}
	return nil
}

// startForwards opens the local listeners of -L / -D (connections go to the
// current destination).
func (c *Client) startForwards(_ *Session) {
	for _, f := range c.o.Forwards {
		if f.Kind == 'R' {
			continue
		}
		l, err := net.Listen("tcp", net.JoinHostPort(f.Bind, strconv.Itoa(f.Port)))
		if err != nil {
			c.notice("%s: %v", f, err)
			continue
		}
		go func(f Forward) {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				go c.forwardConn(f, conn)
			}
		}(f)
	}
}

func (c *Client) forwardConn(f Forward, conn net.Conn) {
	dst := c.top() // forwards go to the current destination
	if dst == nil {
		_ = conn.Close()
		return
	}
	host, port := f.Host, f.HostPort
	if f.Kind == 'D' {
		var err error
		if host, port, err = socks5Handshake(conn); err != nil {
			_ = conn.Close()
			return
		}
	}
	ch, err := dst.openStream(wire.StreamSpec{K: "tcp", Host: host, Port: port})
	if err != nil {
		if f.Kind == 'D' {
			_, _ = conn.Write([]byte{5, 1, 0, 1, 0, 0, 0, 0, 0, 0})
		}
		_ = conn.Close()
		return
	}
	if f.Kind == 'D' {
		// optimistic success; a failed remote dial closes the connection
		_, _ = conn.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
	}
	stream.Splice(ch, conn)
}

// socks5Handshake implements the no-auth CONNECT subset of SOCKS5.
func socks5Handshake(c net.Conn) (string, int, error) {
	h := make([]byte, 2)
	if _, err := io.ReadFull(c, h); err != nil || h[0] != 5 {
		return "", 0, errors.New("not socks5")
	}
	methods := make([]byte, h[1])
	if _, err := io.ReadFull(c, methods); err != nil {
		return "", 0, err
	}
	if _, err := c.Write([]byte{5, 0}); err != nil {
		return "", 0, err
	}
	req := make([]byte, 4)
	if _, err := io.ReadFull(c, req); err != nil || req[1] != 1 {
		_, _ = c.Write([]byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0})
		return "", 0, errors.New("only CONNECT supported")
	}
	var host string
	switch req[3] {
	case 1:
		ip := make([]byte, 4)
		if _, err := io.ReadFull(c, ip); err != nil {
			return "", 0, err
		}
		host = net.IP(ip).String()
	case 4:
		ip := make([]byte, 16)
		if _, err := io.ReadFull(c, ip); err != nil {
			return "", 0, err
		}
		host = net.IP(ip).String()
	case 3:
		n := make([]byte, 1)
		if _, err := io.ReadFull(c, n); err != nil {
			return "", 0, err
		}
		name := make([]byte, n[0])
		if _, err := io.ReadFull(c, name); err != nil {
			return "", 0, err
		}
		host = string(name)
	default:
		return "", 0, errors.New("bad address type")
	}
	p := make([]byte, 2)
	if _, err := io.ReadFull(c, p); err != nil {
		return "", 0, err
	}
	return host, int(binary.BigEndian.Uint16(p)), nil
}

// fromRemote serves streams a remote host opened to us.
func (c *Client) fromRemote(s *Session, ch stream.Conn, spec wire.StreamSpec) {
	refuse := func(msg string) {
		_ = ch.Send(stream.Err, []byte(msg))
		_ = ch.Close()
	}
	switch spec.K {
	case "rfwd":
		for _, f := range c.o.Forwards {
			if f.Kind == 'R' && f.id == spec.ID {
				conn, err := net.Dial("tcp", net.JoinHostPort(f.Host, strconv.Itoa(f.HostPort)))
				if err != nil {
					refuse(err.Error())
					return
				}
				stream.Splice(ch, conn)
				return
			}
		}
		refuse("unknown forward")
	case "exec":
		c.serveSrcExec(s, ch, spec)
	case "agent":
		sock := os.Getenv("SSH_AUTH_SOCK")
		if !c.o.Agent || sock == "" {
			refuse("agent forwarding not enabled")
			return
		}
		conn, err := net.Dial("unix", sock)
		if err != nil {
			refuse(err.Error())
			return
		}
		stream.Splice(ch, conn)
	case "build":
		serveBuild(ch, spec.Cmd)
	case "src-ssh":
		if !c.allowSrc(s, "ssh to this machine") {
			refuse("src: access denied by the wosh client")
			return
		}
		conn, err := net.Dial("tcp", "127.0.0.1:22")
		if err != nil {
			refuse("no sshd on the wosh client: " + err.Error())
			return
		}
		stream.Splice(ch, conn)
	default:
		refuse("unsupported stream " + spec.K)
	}
}
