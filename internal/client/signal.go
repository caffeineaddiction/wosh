package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"wosh/internal/wire"
)

// sigChan is a signaling path to a remote relay.
type sigChan interface {
	send(m wire.Msg) error
	// recv waits for the next message for session id (any id if "").
	recv(ctx context.Context, id string) (wire.Msg, error)
	close()
}

// --- ssh / local process pipe ------------------------------------------------

type pipeWire struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	msgs    chan wire.Msg
	once    sync.Once
	pushErr atomic.Value // error: push handshake happened but no build could be sent
}

// exitCode of the ssh process (valid after close).
func (w *pipeWire) exitCode() int {
	if w.cmd.ProcessState == nil {
		return -1
	}
	return w.cmd.ProcessState.ExitCode()
}

// pipeCmd describes what to run on the remote host over `ssh -T`.
type pipeCmd struct {
	interactive bool   // bootstrap: ssh may prompt; stderr goes to the user
	push        bool   // remote side is the perl push stub: answer WOSHPUSH with a binary
	remote      string // full remote command line (already quoted)
}

// woshCmd runs an installed wosh with args (or pushes one when push is set).
func (c *Client) woshCmd(interactive bool, args ...string) pipeCmd {
	if c.o.Push {
		return pipeCmd{interactive: interactive, push: true, remote: perlCommand(pushStub, args...)}
	}
	if c.o.Local != "" {
		return pipeCmd{interactive: interactive, remote: RemoteCommand(c.o.Local, args...)}
	}
	return pipeCmd{interactive: interactive, remote: RemoteCommand(c.o.ServerCmd, args...)}
}

// startPipe launches `ssh -T <args> <remote>` (or the remote command locally
// in test mode) and scans its stdout for wosh OSC messages.
func (c *Client) startPipe(pc pipeCmd) (*pipeWire, error) {
	var cmd *exec.Cmd
	if c.o.Local != "" {
		cmd = exec.Command("sh", "-c", pc.remote)
	} else {
		args := []string{"-T"}
		if !pc.interactive {
			// reconnects happen while we own the terminal: no prompts
			args = append(args, "-o", "BatchMode=yes", "-o", "ConnectTimeout=10")
		}
		args = append(args, c.o.SSHArgs...)
		args = append(args, pc.remote)
		cmd = exec.Command(c.o.SSH, args...)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if pc.interactive {
		cmd.Stderr = c.errOut
	} else {
		cmd.Stderr = logWriter{}
	}
	log.Printf("exec %q", cmd.Args)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	w := &pipeWire{cmd: cmd, stdin: stdin, msgs: make(chan wire.Msg, 8)}
	show := func(out []byte) {
		if pc.interactive {
			_, _ = c.errOut.Write(out)
		} else {
			log.Printf("remote: %q", out)
		}
	}
	go func() {
		defer close(w.msgs)
		buf := make([]byte, 8192)
		var pre []byte // push mode: plain text until the WOSHPUSH line
		pushing := pc.push
		var f wire.OSCFilter
		for {
			n, err := stdout.Read(buf)
			data := buf[:n]
			if pushing {
				pre = append(pre, data...)
				data = nil
				if i := bytes.Index(pre, []byte("WOSHPUSH ")); i >= 0 {
					if j := bytes.IndexByte(pre[i:], '\n'); j >= 0 {
						show(pre[:i])
						line := string(pre[i : i+j])
						data = pre[i+j+1:]
						pushing = false
						log.Printf("push handshake: %s", line)
						go func() {
							if err := sendBinary(stdin, line); err != nil {
								w.pushErr.Store(err)
								_ = stdin.Close()
							}
						}()
					}
				} else if k := bytes.LastIndexByte(pre, '\n'); k >= 0 {
					show(pre[:k+1])
					pre = pre[k+1:]
				}
			}
			out, msgs := f.Write(data)
			if len(out) > 0 {
				show(out)
			}
			for _, m := range msgs {
				w.msgs <- m
			}
			if err != nil {
				return
			}
		}
	}()
	return w, nil
}

func (w *pipeWire) send(m wire.Msg) error {
	_, err := w.stdin.Write(wire.EncodeInput(m))
	return err
}

func (w *pipeWire) recv(ctx context.Context, id string) (wire.Msg, error) {
	for {
		select {
		case m, ok := <-w.msgs:
			if !ok {
				return m, errors.New("remote side closed before answering (is wosh installed on the destination? see --server-cmd)")
			}
			if id == "" || m.ID == id || m.ID == "" {
				return m, nil
			}
		case <-ctx.Done():
			return wire.Msg{}, ctx.Err()
		}
	}
}

func (w *pipeWire) close() {
	w.once.Do(func() {
		_ = w.stdin.Close()
		done := make(chan struct{})
		go func() { _ = w.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = w.cmd.Process.Kill()
		}
	})
}

// --- through a parent wosh session's terminal -------------------------------

type parentWire struct {
	parent *Session
	id     string
	ch     chan wire.Msg
}

func newParentWire(parent *Session, id string) *parentWire {
	w := &parentWire{parent: parent, id: id, ch: make(chan wire.Msg, 4)}
	parent.addWaiter(id, w.ch)
	return w
}

func (w *parentWire) send(m wire.Msg) error {
	w.parent.sendInput(wire.EncodeInput(m))
	return nil
}

func (w *parentWire) recv(ctx context.Context, id string) (wire.Msg, error) {
	select {
	case m := <-w.ch:
		return m, nil
	case <-w.parent.done:
		return wire.Msg{}, errors.New("parent session ended")
	case <-ctx.Done():
		return wire.Msg{}, ctx.Err()
	}
}

func (w *parentWire) close() { w.parent.removeWaiter(w.id) }

// remotePath is appended to PATH on the remote side so a wosh installed in a
// user directory is found by non-interactive ssh commands.
const remotePath = `$PATH:$HOME/.local/bin:$HOME/go/bin:/usr/local/bin`

// RemoteCommand builds the command string ssh hands to the remote user's
// login shell (`$SHELL -c <string>`). That shell may be bash, zsh, fish,
// tcsh, nushell, ... so the string only uses syntax they all agree on:
// plain words and single-quoted strings with no quotes or backslashes
// inside. Everything shell-specific runs under /bin/sh.
//
//	sh -c 'PATH="..."; exec "$0" "$@"' <wosh> <args...>
func RemoteCommand(wosh string, args ...string) string {
	if wosh == "" {
		wosh = "wosh"
	}
	parts := []string{"sh", "-c", `'PATH="` + remotePath + `"; exec "$0" "$@"'`, quoteWord(wosh)}
	for _, a := range args {
		parts = append(parts, quoteWord(a))
	}
	return strings.Join(parts, " ")
}

// quoteWord quotes a word portably across sh/fish/csh. Words containing a
// single quote or backslash cannot be quoted identically in all of them.
func quoteWord(w string) string {
	safe := w != ""
	for _, r := range w {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("/._-+=:,@%", r)) {
			safe = false
			break
		}
	}
	if safe {
		return w
	}
	return "'" + strings.NewReplacer("'", "", `\`, "").Replace(w) + "'"
}

type logWriter struct{}

func (logWriter) Write(p []byte) (int, error) {
	log.Printf("ssh: %s", strings.TrimSpace(string(p)))
	return len(p), nil
}
