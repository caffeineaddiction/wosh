// Package client implements the wosh terminal client: it keeps a stack of
// sessions (root reached by ssh, children upgraded in-band), routes the
// keyboard to the top one and reconnects sessions as the network changes.
package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"os/user"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"
	"golang.org/x/term"

	"wosh/internal/rtc"
	"wosh/internal/wire"
)

// Options configures the client.
type Options struct {
	SSH       string   // ssh binary
	SSHArgs   []string // passed to ssh before the remote command (host, -J, -p, ...)
	ServerCmd string   // path of the wosh binary on remote hosts (default "wosh", searched in PATH + ~/.local/bin, ~/go/bin)
	Local     string   // test mode: run this command locally instead of ssh
	ICE       []wire.ICEServer
	RelayOnly bool
	Auto      bool // accept in-band upgrade requests without asking
	Term      string
	Resume    string            // "id:key" of a detached root session to reattach to
	Env       map[string]string // extra variables for remote session shells (--env)
	Src       string            // host alias for the client in remote sessions ("src")
	AllowSrc  bool              // don't ask before serving src: requests
	Forwards  []Forward         // -L / -R / -D
	Push      bool              // remote side via perl stub: use a compatible installed wosh, else upload one (CLI default)
	ForcePush bool              // upload even if a compatible wosh is installed

	In  io.Reader
	Out io.Writer
	Err io.Writer
}

// Client is a running wosh client.
type Client struct {
	o      Options
	api    *webrtc.API
	errOut io.Writer

	mu    sync.Mutex
	stack []*Session

	rootPending *Session // connected but not yet shown (see begin)
	rfwds       []wire.RemoteFwd

	outMu      sync.Mutex
	status     string
	prompt     chan byte
	finished   chan struct{}
	finishOnce sync.Once
	exitCode   int
	ttyFd      int
	isTTY      bool
}

// Run connects and runs an interactive session; returns the exit status.
func Run(o Options) (int, error) {
	c, err := Start(o)
	if err != nil {
		return 255, err
	}
	// take over the terminal only after ssh had its chance to prompt
	if f, ok := c.o.In.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		st, err := term.MakeRaw(int(f.Fd()))
		if err == nil {
			c.isTTY = true
			defer term.Restore(int(f.Fd()), st)
		}
	}
	c.begin()
	return c.Wait(), nil
}

func (o *Options) setDefaults() {
	if o.In == nil {
		o.In = os.Stdin
	}
	if o.Out == nil {
		o.Out = os.Stdout
	}
	if o.Err == nil {
		o.Err = os.Stderr
	}
	if o.SSH == "" {
		o.SSH = "ssh"
	}
	if o.Term == "" {
		o.Term = os.Getenv("TERM")
	}
	if o.ICE == nil {
		o.ICE = []wire.ICEServer{{URLs: rtc.DefaultSTUN}}
	}
	if o.Src == "" {
		o.Src = "src"
	}
}

// Start bootstraps the root session over ssh (may prompt for passwords).
func Start(o Options) (*Client, error) {
	o.setDefaults()
	c := &Client{o: o, api: rtc.NewAPI(), errOut: o.Err, finished: make(chan struct{}), ttyFd: -1}
	if err := c.prepareForwards(); err != nil {
		return nil, err
	}
	if f, ok := o.Out.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		c.ttyFd = int(f.Fd())
	}
	root, err := c.connectRoot()
	if err != nil {
		return nil, err
	}
	c.rootPending = root
	return c, nil
}

// begin shows the root session (output received so far is flushed now, after
// the terminal is in raw mode, so replies to terminal queries round-trip)
// and starts the input / resize / roaming loops.
func (c *Client) begin() {
	if r := c.rootPending; r != nil {
		c.rootPending = nil
		c.push(r)
		c.startForwards(r)
	}
	go c.inputLoop()
	go c.winchLoop()
	go c.netWatch()
}

// Wait blocks until all sessions are over (or the user quit/detached).
func (c *Client) Wait() int {
	<-c.finished
	c.clearStatus()
	return c.exitCode
}

func (c *Client) connectRoot() (*Session, error) {
	remote := []string{"server"}
	var rid, rkey string
	if c.o.Resume != "" {
		var ok bool
		rid, rkey, ok = strings.Cut(c.o.Resume, ":")
		if !ok {
			return nil, fmt.Errorf("--resume wants ID:KEY")
		}
		remote = []string{"attach", rid}
	}
	w, hello, err := c.startRemote(remote)
	if err != nil {
		return nil, err
	}
	s := newSession(c, hello.ID, hello.Host, hello.User, nil)
	s.sock = hello.Sock
	if rkey != "" {
		if hello.ID != rid {
			w.close()
			return nil, fmt.Errorf("resumed wrong session %q", hello.ID)
		}
		s.key = rkey
	}
	err = s.negotiate(w)
	if err == nil && hello.OS == "darwin" && rkey == "" {
		// macOS only lets the daemon talk to LAN peers while the ssh session
		// that started it exists: keep that (idle) ssh open as an anchor.
		s.anchor = w
	} else {
		w.close()
	}
	// The daemon is up and knows our key; transient ICE failures are retried
	// through the cheap reattach path (no new daemon, no re-upload).
	for attempt := 1; err != nil && attempt <= 2 && hello.Sock != ""; attempt++ {
		fmt.Fprintf(c.errOut, "wosh: %v; retrying (%d/2)\n", err, attempt)
		err = s.reconnect()
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}

// startRemote runs the remote relay over ssh (pushing / falling back to an
// installed wosh as needed) and returns its hello.
func (c *Client) startRemote(remote []string) (*pipeWire, wire.Msg, error) {
	w, hello, err := c.bootstrap(remote)
	if err != nil && c.o.Push && w != nil {
		// fall back to an installed wosh when the remote has no perl (exit
		// 127) or we have no build for its platform
		pe, _ := w.pushErr.Load().(error)
		if w.exitCode() == 127 || pe != nil {
			why := "no perl on remote"
			if pe != nil {
				why = pe.Error()
			}
			fmt.Fprintf(c.errOut, "wosh: cannot push (%s); trying installed wosh\n", why)
			c.o.Push = false
			w, hello, err = c.bootstrap(remote)
		}
	}
	if err != nil {
		return nil, wire.Msg{}, err
	}
	if hello.T == wire.TError {
		w.close()
		return nil, wire.Msg{}, fmt.Errorf("remote: %s", hello.Err)
	}
	if hello.T != wire.THello {
		w.close()
		return nil, wire.Msg{}, fmt.Errorf("unexpected %q from remote", hello.T)
	}
	return w, hello, nil
}

// bootstrap runs the remote relay over ssh and waits for its hello.
func (c *Client) bootstrap(remote []string) (*pipeWire, wire.Msg, error) {
	w, err := c.startPipe(c.woshCmd(true, remote...))
	if err != nil {
		return nil, wire.Msg{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	hello, err := w.recv(ctx, "")
	if err != nil {
		w.close()
		return w, hello, err
	}
	return w, hello, nil
}

// --- session stack -----------------------------------------------------------

func (c *Client) push(s *Session) {
	s.outMu.Lock()
	c.mu.Lock()
	c.stack = append(c.stack, s)
	c.mu.Unlock()
	// output that arrived before the session was on screen (e.g. the
	// shell's startup queries, motd, first prompt) is shown now
	s.shown = true
	if len(s.early) > 0 {
		c.writeOut(s.early)
		s.early = nil
	}
	s.outMu.Unlock()
	go s.heartbeat()
	go s.maintain()
	cols, rows := c.size()
	s.send(wire.ResizeFrame(cols, rows))
}

func (c *Client) top() *Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.stack) == 0 {
		return nil
	}
	return c.stack[len(c.stack)-1]
}

func (c *Client) root() *Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.stack) == 0 || c.stack[0].parent != nil {
		return nil
	}
	return c.stack[0]
}

func (c *Client) isTop(s *Session) bool { return c.top() == s }

func (c *Client) remove(s *Session) {
	c.mu.Lock()
	wasTop := len(c.stack) > 0 && c.stack[len(c.stack)-1] == s
	c.stack = slices.DeleteFunc(c.stack, func(x *Session) bool { return x == s })
	empty := len(c.stack) == 0
	var next *Session
	if !empty {
		next = c.stack[len(c.stack)-1]
	}
	c.mu.Unlock()
	if empty {
		c.finish(s.exitCode)
		return
	}
	if wasTop {
		c.clearStatus()
		cols, rows := c.size()
		next.send(wire.ResizeFrame(cols, rows))
		c.updateStatus()
	}
}

func (c *Client) finish(code int) {
	c.finishOnce.Do(func() {
		c.exitCode = code
		close(c.finished)
	})
}

// onHello: a `wosh up` somewhere below session s wants to be connected to.
func (c *Client) onHello(parent *Session, m wire.Msg) {
	if !c.isTop(parent) {
		log.Printf("ignoring hello from background session %s", parent.id)
		return
	}
	go func() {
		name := m.Host
		if m.User != "" {
			name = m.User + "@" + m.Host
		}
		parent.mu.Lock()
		trusted := m.Token != "" && parent.token != "" && m.Token == parent.token
		parent.mu.Unlock()
		// a chained hop (`wosh host` run inside this session) proves itself
		// with the session token; anything else printing a hello must ask
		if !trusted && !c.o.Auto && !c.ask(fmt.Sprintf("upgrade to direct wosh connection to %s? [y/N] ", name)) {
			parent.sendInput(wire.EncodeInput(wire.Msg{T: wire.TBye, ID: m.ID}))
			return
		}
		child := newSession(c, m.ID, m.Host, m.User, parent)
		child.delegated = trusted
		w := newParentWire(parent, m.ID)
		err := child.negotiate(w)
		w.close()
		if err != nil {
			c.notice("upgrade to %s failed: %v", name, err)
			parent.sendInput(wire.EncodeInput(wire.Msg{T: wire.TBye, ID: m.ID}))
			return
		}
		c.push(child)
	}()
}

// --- terminal I/O -----------------------------------------------------------

func (c *Client) writeOut(p []byte) {
	c.outMu.Lock()
	_, _ = c.o.Out.Write(p)
	c.outMu.Unlock()
}

func (c *Client) size() (uint16, uint16) {
	if c.ttyFd >= 0 {
		if w, h, err := term.GetSize(c.ttyFd); err == nil && w > 0 && h > 0 {
			return uint16(w), uint16(h)
		}
	}
	return 80, 24
}

func (c *Client) winchLoop() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGWINCH, syscall.SIGUSR1)
	for sig := range ch {
		if sig == syscall.SIGUSR1 { // simulate roaming: drop and re-establish every link
			c.dropAll("SIGUSR1")
			continue
		}
		if s := c.top(); s != nil {
			cols, rows := c.size()
			s.send(wire.ResizeFrame(cols, rows))
		}
	}
}

func (c *Client) ask(q string) bool {
	ch := make(chan byte, 1)
	c.outMu.Lock()
	c.prompt = ch
	fmt.Fprintf(c.o.Out, "\r\n\x1b[1m[wosh]\x1b[0m %s", q)
	c.outMu.Unlock()
	var b byte
	select {
	case b = <-ch:
	case <-time.After(60 * time.Second):
	case <-c.finished:
	}
	c.outMu.Lock()
	c.prompt = nil
	yes := b == 'y' || b == 'Y'
	ans := "no"
	if yes {
		ans = "yes"
	}
	fmt.Fprintf(c.o.Out, "%s\r\n", ans)
	c.outMu.Unlock()
	return yes
}

func (c *Client) notice(format string, a ...any) {
	c.writeOut([]byte(fmt.Sprintf("\r\n\x1b[1m[wosh]\x1b[0m "+format+"\r\n", a...)))
}

const escapeHelp = "Supported escape sequences (after Enter):\r\n" +
	"  ~.  terminate all sessions and exit\r\n" +
	"  ~&  detach: exit client, leave sessions running\r\n" +
	"  ~s  show session stack\r\n" +
	"  ~?  this help\r\n" +
	"  ~~  send a literal ~\r\n"

// inputLoop reads the keyboard, handles ~ escapes and feeds the top session.
func (c *Client) inputLoop() {
	buf := make([]byte, 4096)
	lineStart, tilde := true, false
	for {
		n, err := c.o.In.Read(buf)
		if n > 0 {
			c.outMu.Lock()
			pr := c.prompt
			c.outMu.Unlock()
			if pr != nil {
				select {
				case pr <- buf[0]:
				default:
				}
				continue
			}
			out := make([]byte, 0, n)
			for _, b := range buf[:n] {
				if tilde {
					tilde = false
					switch b {
					case '.':
						c.killAll()
						return
					case '&':
						c.notice("detached; sessions keep running on their hosts")
						if r := c.root(); r != nil {
							c.notice("resume with: wosh --resume=%s:%s %s", r.id, r.key, strings.Join(c.o.SSHArgs, " "))
						}
						c.finish(0)
						return
					case '?':
						c.writeOut([]byte("\r\n" + escapeHelp))
						continue
					case 's':
						c.writeOut([]byte("\r\n" + c.describe()))
						continue
					case '~':
						out = append(out, '~')
						lineStart = false
						continue
					default:
						out = append(out, '~')
					}
				} else if lineStart && b == '~' {
					tilde = true
					continue
				}
				out = append(out, b)
				lineStart = b == '\r' || b == '\n'
			}
			if s := c.top(); s != nil && len(out) > 0 {
				s.sendInput(out)
			}
		}
		if err != nil {
			// stdin closed (non-interactive use): leave sessions as-is
			<-c.finished
			return
		}
	}
}

func (c *Client) killAll() {
	c.mu.Lock()
	all := slices.Clone(c.stack)
	c.mu.Unlock()
	for _, s := range all {
		s.send([]byte{wire.FKill})
	}
	time.Sleep(200 * time.Millisecond)
	c.notice("connection terminated")
	c.finish(0)
}

func (c *Client) describe() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var b strings.Builder
	for i, s := range c.stack {
		state := "connected"
		if !s.isAlive() {
			state = "reconnecting"
		}
		via := "ssh"
		if s.parent != nil {
			via = "via " + s.parent.name()
		}
		marker := " "
		if i == len(c.stack)-1 {
			marker = "*"
		}
		s.mu.Lock()
		path := s.path
		s.mu.Unlock()
		fmt.Fprintf(&b, " %s %d. %s [%s] (%s, %s)\r\n      %s\r\n", marker, i, s.name(), s.id, via, state, path)
	}
	return b.String()
}

// --- status line ------------------------------------------------------------

func (c *Client) statusShown() bool {
	c.outMu.Lock()
	defer c.outMu.Unlock()
	return c.status != ""
}

func (c *Client) drawStatus(text string) {
	if !c.isTTY {
		return
	}
	_, rows := c.size()
	c.outMu.Lock()
	c.status = text
	fmt.Fprintf(c.o.Out, "\x1b7\x1b[%d;1H\x1b[0;7m %s \x1b[0m\x1b[K\x1b8", rows, text)
	c.outMu.Unlock()
}

func (c *Client) clearStatus() {
	c.outMu.Lock()
	shown := c.status != ""
	c.status = ""
	if shown && c.isTTY {
		_, rows := c.size()
		fmt.Fprintf(c.o.Out, "\x1b7\x1b[%d;1H\x1b[K\x1b8", rows)
	}
	c.outMu.Unlock()
}

func (c *Client) updateStatus() {
	s := c.top()
	if s == nil {
		return
	}
	if !s.isAlive() {
		c.drawStatus(fmt.Sprintf("wosh: reconnecting to %s ... (~. to quit)", s.name()))
	}
}

func (c *Client) setStatusErr(s *Session, err error) {
	if c.isTop(s) {
		msg := err.Error()
		if len(msg) > 60 {
			msg = msg[:60] + "..."
		}
		c.drawStatus(fmt.Sprintf("wosh: reconnecting to %s: %s (~. to quit)", s.name(), msg))
	}
}

// --- roaming ------------------------------------------------------------------

func addrSet() map[string]bool {
	m := map[string]bool{}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return m
	}
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok || ipn.IP.IsLoopback() || ipn.IP.IsLinkLocalUnicast() {
			continue
		}
		m[ipn.IP.String()] = true
	}
	return m
}

// netWatch forces a reconnect as soon as a local address disappears (we
// moved networks) instead of waiting for the ping timeout.
func (c *Client) netWatch() {
	prev := addrSet()
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.finished:
			return
		case <-t.C:
		}
		cur := addrSet()
		var lost []string
		for a := range prev {
			if !cur[a] {
				lost = append(lost, a)
			}
		}
		prev = cur
		if len(lost) == 0 {
			continue
		}
		sort.Strings(lost)
		log.Printf("addresses lost %v: reconnecting all sessions", lost)
		c.dropAll("network change")
	}
}

func (c *Client) dropAll(why string) {
	c.mu.Lock()
	all := slices.Clone(c.stack)
	c.mu.Unlock()
	for _, s := range all {
		s.markDead(0, why)
	}
}

// remoteEnv: variables for remote shells - locale and color hints (like
// mosh/ssh SendEnv defaults) plus --env.
func (c *Client) remoteEnv() map[string]string {
	env := map[string]string{}
	for _, e := range os.Environ() {
		k, v, _ := strings.Cut(e, "=")
		if k == "LANG" || k == "LANGUAGE" || strings.HasPrefix(k, "LC_") || k == "COLORTERM" {
			env[k] = v
		}
	}
	for k, v := range c.o.Env {
		env[k] = v
	}
	return env
}

func localUser() string {
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return os.Getenv("USER")
}
