// Package shim implements the ssh/scp/sftp stand-ins that live in a wosh
// session's bin dir. They make the wosh client reachable as host "src"
// (`rsync src:dir .`, `scp file src:`, `sftp src`) and pass every other host
// to the real tools unchanged.
//
// Transfers use the wosh channel directly (primary). Anything that is not a
// file transfer, e.g. an interactive `ssh src`, falls back to real ssh into
// the client's own sshd, tunnelled through wosh (ProxyCommand).
package shim

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"wosh/internal/stream"
	"wosh/internal/wire"
)

// ssh options that take an argument (from ssh(1) getopt string).
const sshArgOpts = "BbcDEeFIiJLlmOoPpQRSWw"

type sshCall struct {
	host  string // as given (may include user@)
	user  string // -l
	sub   bool   // -s
	cmd   []string
	hostI int // index of host in args
}

func parseSSH(args []string) (sshCall, bool) {
	var c sshCall
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			i++
			if i < len(args) {
				c.host, c.hostI, c.cmd = args[i], i, args[i+1:]
				return c, true
			}
			return c, false
		}
		if len(a) > 1 && a[0] == '-' {
			for j := 1; j < len(a); j++ {
				o := a[j]
				if strings.IndexByte(sshArgOpts, o) >= 0 {
					val := a[j+1:]
					if val == "" && i+1 < len(args) {
						i++
						val = args[i]
					}
					if o == 'l' {
						c.user = val
					}
					break
				}
				if o == 's' {
					c.sub = true
				}
			}
			continue
		}
		c.host, c.hostI, c.cmd = a, i, args[i+1:]
		return c, true
	}
	return c, false
}

func srcName() string {
	if s := os.Getenv("WOSH_SRC"); s != "" {
		return s
	}
	return "src"
}

func isSrc(host string) bool {
	if i := strings.LastIndexByte(host, '@'); i >= 0 {
		host = host[i+1:]
	}
	return os.Getenv("WOSH_SOCK") != "" && host == srcName()
}

// realTool finds the real `name` in PATH, skipping our session bin dir.
func realTool(name string) string {
	ours := os.Getenv("WOSH_BIN_DIR")
	self, _ := os.Executable()
	selfReal, _ := filepath.EvalSymlinks(self)
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		if dir == "" || dir == ours {
			continue
		}
		p := filepath.Join(dir, name)
		st, err := os.Stat(p)
		if err != nil || st.IsDir() || st.Mode()&0o111 == 0 {
			continue
		}
		if r, _ := filepath.EvalSymlinks(p); r != "" && r == selfReal {
			continue
		}
		return p
	}
	return "/usr/bin/" + name
}

func execReal(name string, args []string) int {
	p := realTool(name)
	err := syscall.Exec(p, append([]string{name}, args...), os.Environ())
	fmt.Fprintf(os.Stderr, "wosh: exec %s: %v\n", p, err)
	return 127
}

// isTransfer: commands the client serves directly (see client/src.go).
func isTransfer(c sshCall) bool {
	if c.sub {
		return len(c.cmd) == 1 && c.cmd[0] == "sftp"
	}
	if len(c.cmd) == 0 {
		return false
	}
	line := strings.Join(c.cmd, " ")
	f := strings.Fields(line)
	return len(f) > 1 && (f[0] == "rsync" && strings.Contains(line, " --server") ||
		f[0] == "scp" && (strings.Contains(line, " -t") || strings.Contains(line, " -f")))
}

// SSH is the `ssh` stand-in.
func SSH(args []string) int {
	c, ok := parseSSH(args)
	if !ok || !isSrc(c.host) {
		return execReal("ssh", args)
	}
	if isTransfer(c) {
		return execOnClient(wire.StreamSpec{K: "exec", Cmd: strings.Join(c.cmd, " "), Sub: c.sub})
	}
	// fallback: real ssh to the client's sshd, tunnelled through wosh
	self := filepath.Join(os.Getenv("WOSH_BIN_DIR"), "wosh")
	extra := []string{"-o", "ProxyCommand=" + self + " _proxy", "-o", "HostKeyAlias=wosh-" + srcName(), "-o", "CheckHostIP=no"}
	if c.user == "" && !strings.Contains(c.host, "@") {
		if u := clientUser(); u != "" {
			extra = append(extra, "-l", u)
		}
	}
	return execReal("ssh", append(extra, args...))
}

// SCP / SFTP: the real tools, with our ssh stand-in as their transport
// (they hardcode /usr/bin/ssh otherwise).
func SCP(args []string) int  { return viaShim("scp", args) }
func SFTP(args []string) int { return viaShim("sftp", args) }

func viaShim(name string, args []string) int {
	for _, a := range args {
		if a == "-S" || strings.HasPrefix(a, "-S") && len(a) > 2 {
			return execReal(name, args)
		}
	}
	return execReal(name, append([]string{"-S", filepath.Join(os.Getenv("WOSH_BIN_DIR"), "ssh")}, args...))
}

func clientUser() string {
	m, err := stream.SessionInfo()
	if err != nil {
		return ""
	}
	return m.User
}

func openStream(spec wire.StreamSpec) (*stream.Framed, error) { return stream.ToClient(spec) }

// execOnClient runs a transfer command on the client with our stdio.
func execOnClient(spec wire.StreamSpec) int {
	s, err := openStream(spec)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wosh: %s: %v\n", srcName(), err)
		return 255
	}
	defer s.Close()
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := os.Stdin.Read(buf)
			if n > 0 && s.Send(stream.Data, buf[:n]) != nil {
				return
			}
			if err != nil {
				_ = s.Send(stream.Fin, nil)
				return
			}
		}
	}()
	for {
		t, p, err := s.Recv()
		if err != nil {
			fmt.Fprintf(os.Stderr, "wosh: %s: connection lost\n", srcName())
			return 255
		}
		switch t {
		case stream.Data:
			if _, err := os.Stdout.Write(p); err != nil {
				return 255
			}
		case stream.Stderr:
			_, _ = os.Stderr.Write(p)
		case stream.Err:
			fmt.Fprintf(os.Stderr, "wosh: %s: %s\n", srcName(), p)
			return 255
		case stream.Exit:
			return stream.ParseExit(p)
		}
	}
}

// Proxy is `wosh _proxy`: ssh ProxyCommand to the client's sshd.
func Proxy() int {
	s, err := openStream(wire.StreamSpec{K: "src-ssh"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "wosh: %s: %v\n", srcName(), err)
		return 255
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			t, p, err := s.Recv()
			if err != nil || t == stream.Fin {
				return
			}
			switch t {
			case stream.Data:
				if _, err := os.Stdout.Write(p); err != nil {
					return
				}
			case stream.Err:
				fmt.Fprintf(os.Stderr, "wosh: %s: %s\n", srcName(), p)
				return
			}
		}
	}()
	go func() {
		_, _ = io.Copy(writerFunc(func(p []byte) (int, error) { return len(p), s.Send(stream.Data, p) }), os.Stdin)
		_ = s.Send(stream.Fin, nil)
	}()
	<-done
	_ = s.Close()
	return 0
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// Ensure exec is linked for realTool consumers.
var _ = exec.LookPath
