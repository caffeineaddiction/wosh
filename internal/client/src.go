package client

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"

	"wosh/internal/stream"
	"wosh/internal/wire"
)

// src: lets a remote session reach this machine as host "src" (rsync/scp/
// sftp src:path). This is a door from the remote host into the client, like
// ssh agent forwarding, so:
//   - the user is asked once per session (unless --allow-src);
//   - only file-transfer server commands run (rsync --server, scp -t/-f,
//     the sftp subsystem), started in the home directory like sshd would;
//   - anything else (e.g. interactive `ssh src`) is refused here; the
//     remote shim then falls back to real ssh into this machine's sshd,
//     which does its own authentication.

func (c *Client) allowSrc(s *Session, what string) bool {
	if c.o.AllowSrc {
		return true
	}
	s.srcMu.Lock()
	defer s.srcMu.Unlock()
	if s.srcAllowed == 0 {
		ok := c.ask(fmt.Sprintf("%s wants %s via %s: - allow for this session? [y/N] ", s.name(), what, c.o.Src))
		s.srcAllowed = map[bool]int{true: 1, false: -1}[ok]
	}
	return s.srcAllowed == 1
}

// srcCommand validates a requested command; returns argv to run.
func srcCommand(spec wire.StreamSpec) ([]string, string, error) {
	if spec.Sub {
		if spec.Cmd != "sftp" {
			return nil, "", fmt.Errorf("subsystem %q not allowed", spec.Cmd)
		}
		for _, p := range []string{"/usr/lib/openssh/sftp-server", "/usr/libexec/openssh/sftp-server",
			"/usr/lib/ssh/sftp-server", "/usr/libexec/sftp-server", "/usr/lib/sftp-server"} {
			if st, err := os.Stat(p); err == nil && !st.IsDir() {
				return []string{p}, "sftp", nil
			}
		}
		return nil, "", fmt.Errorf("no sftp-server found on the wosh client")
	}
	line := spec.Cmd
	if strings.ContainsAny(line, ";&|`$<>\n\r") {
		return nil, "", fmt.Errorf("command not allowed via src: (shell metacharacters)")
	}
	f := strings.Fields(line)
	if len(f) == 0 {
		return nil, "", fmt.Errorf("empty command")
	}
	has := func(x string) bool {
		for _, a := range f[1:] {
			if a == x {
				return true
			}
		}
		return false
	}
	switch {
	case f[0] == "rsync" && has("--server"):
		return []string{"sh", "-c", line}, "rsync", nil
	case f[0] == "scp" && (has("-t") || has("-f")):
		return []string{"sh", "-c", line}, "scp", nil
	}
	return nil, "", fmt.Errorf("only rsync/scp/sftp transfers run via src:")
}

func (c *Client) serveSrcExec(s *Session, ch stream.Conn, spec wire.StreamSpec) {
	argv, what, err := srcCommand(spec)
	if err == nil && !c.allowSrc(s, what+" file transfer") {
		err = fmt.Errorf("src: access denied by the wosh client")
	}
	if err != nil {
		_ = ch.Send(stream.Stderr, []byte("wosh: "+err.Error()+"\n"))
		_ = ch.Send(stream.Exit, stream.ExitPayload(1))
		_ = ch.Close()
		return
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	if home, err := os.UserHomeDir(); err == nil {
		cmd.Dir = home
	}
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		_ = ch.Send(stream.Stderr, []byte("wosh: "+err.Error()+"\n"))
		_ = ch.Send(stream.Exit, stream.ExitPayload(127))
		_ = ch.Close()
		return
	}
	go func() {
		defer stdin.Close()
		for {
			t, p, err := ch.Recv()
			if err != nil || t == stream.Fin {
				return
			}
			if t == stream.Data {
				if _, err := stdin.Write(p); err != nil {
					return
				}
			}
		}
	}()
	var wg sync.WaitGroup
	pump := func(r io.Reader, typ byte) {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			n, err := r.Read(buf)
			if n > 0 && ch.Send(typ, buf[:n]) != nil {
				return
			}
			if err != nil {
				return
			}
		}
	}
	wg.Add(2)
	go pump(stdout, stream.Data)
	go pump(stderr, stream.Stderr)
	wg.Wait()
	code := 0
	if err := cmd.Wait(); err != nil {
		code = 1
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
	}
	_ = ch.Send(stream.Exit, stream.ExitPayload(code))
	_ = ch.Close()
}
