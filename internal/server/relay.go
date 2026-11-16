package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"wosh/internal/wire"
)

// RelayMode selects how the signaling relay behaves.
type RelayMode int

const (
	// ModeServer starts a new daemon, relays the first negotiation and stays
	// until stdin closes (the client decides how long to keep ssh open).
	// Used as the remote command of `ssh -T host wosh server`.
	ModeServer RelayMode = iota
	// ModeUp starts a new daemon and stays in the foreground ("parked") so
	// the client can renegotiate through this terminal later. Used when a
	// user runs `wosh up` inside an ssh hop of an existing wosh session.
	ModeUp
	// ModeAttach relays one negotiation for an existing daemon (reconnect).
	ModeAttach
)

// ErrNoSession is reported when attaching to a session that no longer exists.
const ErrNoSession = "no such session"

// RelayOptions configures a relay.
type RelayOptions struct {
	Mode    RelayMode
	ID      string   // for ModeAttach
	Command []string // optional command instead of login shell
	Linger  time.Duration
	In      io.Reader
	Out     io.Writer
}

// RunRelay runs the signaling relay on stdin/stdout.
func RunRelay(o RelayOptions) error {
	if o.In == nil {
		o.In = os.Stdin
	}
	if o.Out == nil {
		o.Out = os.Stdout
	}
	id := o.ID
	if o.Mode != ModeAttach {
		id = NewID()
		if err := spawnDaemon(id, o.Command, o.Linger); err != nil {
			return fmt.Errorf("start daemon: %w", err)
		}
	} else if !validID(id) {
		return errors.New("invalid session id")
	}
	sock, err := SocketPath(id)
	if err != nil {
		return err
	}
	conn, err := net.Dial("unix", sock)
	if err != nil {
		_, _ = o.Out.Write(wire.EncodeOSC(wire.Msg{T: wire.TError, ID: id, Err: ErrNoSession}))
		return fmt.Errorf("session %s not found: %w", id, err)
	}
	defer conn.Close()

	isTTY := false
	if f, ok := o.In.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		isTTY = true
		st, err := term.MakeRaw(int(f.Fd()))
		if err == nil {
			defer term.Restore(int(f.Fd()), st)
		}
	}
	say := func(format string, a ...any) {
		if isTTY {
			fmt.Fprintf(o.Out, "\r\n[wosh] "+format+"\r\n", a...)
		} else {
			fmt.Fprintf(os.Stderr, "wosh: "+format+"\n", a...)
		}
	}

	host, _ := os.Hostname()
	uname := ""
	if u, err := user.Current(); err == nil {
		uname = u.Username
	}
	if _, err := o.Out.Write(wire.EncodeOSC(wire.Msg{T: wire.THello, ID: id, Host: host, User: uname, Sock: sock, OS: runtime.GOOS})); err != nil {
		return err
	}

	dmsgs := make(chan wire.Msg)
	go func() {
		defer close(dmsgs)
		sc := bufio.NewScanner(conn)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		for sc.Scan() {
			var m wire.Msg
			if json.Unmarshal(sc.Bytes(), &m) == nil {
				dmsgs <- m
			}
		}
	}()

	type inEv struct {
		m   wire.Msg
		err error
	}
	inCh := make(chan inEv)
	go func() {
		var lr wire.LineReader
		buf := make([]byte, 4096)
		for {
			n, err := o.In.Read(buf)
			lines, lerr := lr.Feed(buf[:n])
			for _, l := range lines {
				if m, ok, _ := wire.ParseInputLine(l); ok {
					inCh <- inEv{m: m}
				}
			}
			if lerr != nil {
				inCh <- inEv{err: lerr}
				return
			}
			if err != nil {
				inCh <- inEv{err: io.EOF}
				return
			}
		}
	}()

	enc := json.NewEncoder(conn)
	connected := o.Mode == ModeAttach
	var firstWait <-chan time.Time
	if !connected {
		firstWait = time.After(30 * time.Second)
	}
	for {
		select {
		case <-firstWait:
			_ = enc.Encode(wire.Msg{T: wire.TBye})
			say("no wosh client answered (is this terminal inside `wosh`?)")
			return errors.New("no client")
		case ev := <-inCh:
			if ev.err != nil {
				if !connected {
					_ = enc.Encode(wire.Msg{T: wire.TBye})
				}
				if errors.Is(ev.err, wire.ErrInterrupt) {
					say("detached")
					return nil
				}
				return nil
			}
			switch ev.m.T {
			case wire.TBye:
				if !connected {
					_ = enc.Encode(wire.Msg{T: wire.TBye})
				}
				return nil
			case wire.TOffer:
				if ev.m.ID != "" && ev.m.ID != id {
					continue // meant for another session
				}
				ev.m.Anchor = o.Mode == ModeServer && !connected
				if err := enc.Encode(ev.m); err != nil {
					return err
				}
				var ans wire.Msg
				var ok bool
				select {
				case ans, ok = <-dmsgs:
				case <-time.After(30 * time.Second):
					ans, ok = wire.Msg{T: wire.TError, ID: id, Err: "daemon timeout"}, true
				}
				if !ok {
					return errors.New("session ended")
				}
				ans.ID = id
				if _, err := o.Out.Write(wire.EncodeOSC(ans)); err != nil {
					return err
				}
				if ans.T == wire.TAnswer {
					connected = true
					firstWait = nil
					if o.Mode == ModeAttach {
						return nil
					}
					_ = enc.Encode(wire.Msg{T: wire.TPark})
					// ModeServer stays until the client closes the ssh pipe:
					// on macOS the daemon may only reach LAN peers while the
					// ssh session that started it is alive (Local Network
					// privacy), so the client keeps it open as an anchor.
				}
			}
		case _, ok := <-dmsgs:
			if !ok { // daemon exited: session over (tell a chained-hop relay)
				_, _ = o.Out.Write(wire.EncodeOSC(wire.Msg{T: wire.TBye, ID: id}))
				return nil
			}
		}
	}
}

// spawnDaemon starts a detached daemon process and waits for it to listen.
func spawnDaemon(id string, command []string, linger time.Duration) error {
	exe, err := selfExe()
	if err != nil {
		return err
	}
	dir, err := RuntimeDir()
	if err != nil {
		return err
	}
	logf, err := os.OpenFile(filepath.Join(dir, id+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logf.Close()
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		return err
	}
	defer devnull.Close()
	r, w, err := os.Pipe()
	if err != nil {
		return err
	}
	defer r.Close()

	args := []string{"_daemon", "--id", id, "--linger", linger.String()}
	if p := os.Getenv("WOSH_UNLINK"); p != "" {
		// pushed temp file (no memfd): the daemon keeps it in the session's
		// private bin dir for `wosh`/shims and deletes it with the session
		args = append(args, "--own-binary", p)
	}
	if len(command) > 0 {
		args = append(args, "--")
		args = append(args, command...)
	}
	cmd := exec.Command(exe, args...)
	cmd.Args[0] = "wosh" // readable in ps even when exe is /proc/self/exe or a memfd
	cmd.Env = cleanEnv()
	cmd.Stdin, cmd.Stdout, cmd.Stderr = devnull, logf, logf
	cmd.ExtraFiles = []*os.File{w}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		w.Close()
		return err
	}
	w.Close()
	go cmd.Wait() // reap if we are still around; normally we exit first

	ready := make(chan error, 1)
	go func() {
		b := make([]byte, 1)
		_, err := r.Read(b)
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			return fmt.Errorf("daemon failed to start (see %s)", logf.Name())
		}
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("daemon start timeout")
	}
}

// selfExe returns a path that re-executes this very binary. On Linux this is
// /proc/self/exe, which also works when wosh was pushed into a memfd and
// never existed on disk (os.Executable would return a dangling path).
func selfExe() (string, error) {
	if _, err := os.Stat("/proc/self/exe"); err == nil {
		return "/proc/self/exe", nil
	}
	return os.Executable()
}

// cleanEnv drops variables that only make sense for this process.
func cleanEnv() []string {
	var env []string
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "WOSH_UNLINK=") {
			continue
		}
		env = append(env, e)
	}
	return env
}
