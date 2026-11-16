package server

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/pion/webrtc/v4"

	"wosh/internal/rtc"
	"wosh/internal/stream"
	"wosh/internal/wire"
)

const (
	ringSize       = 4 << 20
	maxBuffered    = 1 << 20
	firstOfferWait = 60 * time.Second
	// bootstrap failed for good (client gave up): don't leave a shell behind
	neverConnectedWait = 3 * time.Minute
)

// DaemonOptions configures a session daemon.
type DaemonOptions struct {
	ID      string
	Linger  time.Duration // how long to keep an exited session around for a client to collect it
	Command []string      // command to run instead of the login shell
	ReadyFD *os.File      // written to once the control socket is listening
	// OwnBinary: pushed temp file this daemon runs from (no memfd); kept in
	// the session bin dir and deleted with the session.
	OwnBinary string
}

// Daemon owns one PTY session and serves WebRTC peers for it.
type Daemon struct {
	opts DaemonOptions

	mu       sync.Mutex
	key      string
	ptmx     *os.File
	cmd      *exec.Cmd
	out      *ring
	inRx     uint64
	cur      *peer
	started  bool
	exited   bool
	exitCode int
	exitAck  bool
	exitSent bool // FExit may be sent (parked relays are gone)
	// exitDelivered: FExit handed to a peer. If the ack is lost (client
	// exited first) we stop once no peer has been connected for a while.
	exitDelivered bool
	everOpen      bool // some peer connected at least once
	conns         map[net.Conn]chan struct{}

	token      string // shared with the client; proves a chained hop's hello
	clientUser string // client's local user (src: ssh fallback)
	src        string // host alias meaning "the client"
	baseDir    string // <runtime>/<id>.d
	binDir     string // session bin dir (wosh + shims), first in PATH
	rfwd       map[int]net.Listener

	kick chan struct{}
	quit chan struct{}
	once sync.Once
	ln   net.Listener
}

type peer struct {
	pc      *webrtc.PeerConnection
	dc      *webrtc.DataChannel
	sent    uint64
	open    bool
	exitOut bool
	low     chan struct{}
}

// RunDaemon runs a session daemon until the session ends.
func RunDaemon(opts DaemonOptions) error {
	if !validID(opts.ID) {
		return errors.New("invalid session id")
	}
	sock, err := SocketPath(opts.ID)
	if err != nil {
		return err
	}
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return err
	}
	_ = os.Chmod(sock, 0o600)
	defer os.Remove(sock)

	d := &Daemon{
		opts:  opts,
		out:   newRing(ringSize),
		kick:  make(chan struct{}, 1),
		quit:  make(chan struct{}),
		ln:    ln,
		conns: map[net.Conn]chan struct{}{},
		token: newToken(),
		rfwd:  map[int]net.Listener{},
	}
	if err := d.setupBinDir(); err != nil {
		log.Printf("session bin dir: %v", err)
	}
	if opts.ReadyFD != nil {
		_, _ = opts.ReadyFD.Write([]byte{1})
		_ = opts.ReadyFD.Close()
	}
	log.Printf("daemon %s listening on %s", opts.ID, sock)

	go d.acceptLoop()
	go d.sender()
	go d.watchdog()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		<-sigs
		d.mu.Lock()
		if d.cmd != nil && d.cmd.Process != nil && !d.exited {
			_ = syscall.Kill(-d.cmd.Process.Pid, syscall.SIGHUP)
		}
		d.mu.Unlock()
		d.stop()
	}()
	<-d.quit
	_ = ln.Close()
	d.mu.Lock()
	if d.cur != nil {
		_ = d.cur.pc.Close()
	}
	d.mu.Unlock()
	for _, l := range d.rfwd {
		_ = l.Close()
	}
	if d.baseDir != "" {
		_ = os.RemoveAll(d.baseDir)
	}
	log.Printf("daemon %s exiting", opts.ID)
	if dir, err := RuntimeDir(); err == nil && os.Getenv("WOSH_KEEP_LOGS") == "" {
		_ = os.Remove(filepath.Join(dir, opts.ID+".log"))
	}
	return nil
}

func (d *Daemon) stop() { d.once.Do(func() { close(d.quit) }) }

func (d *Daemon) poke() {
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

// watchdog ends the daemon if no client ever shows up, or when an exited
// session has been collected (or lingered too long).
func (d *Daemon) watchdog() {
	start := time.Now()
	var exitedAt, orphanSince, goneSince time.Time
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-d.quit:
			return
		case <-t.C:
		}
		d.mu.Lock()
		started, exited, acked := d.started, d.exited, d.exitAck
		delivered, connected := d.exitDelivered, d.cur != nil && d.cur.open
		d.mu.Unlock()
		if delivered && !connected {
			if orphanSince.IsZero() {
				orphanSince = time.Now()
			}
			if time.Since(orphanSince) > 10*time.Second {
				log.Printf("exit delivered, client gone")
				d.stop()
				return
			}
		} else {
			orphanSince = time.Time{}
		}
		d.mu.Lock()
		everOpen := d.everOpen
		d.mu.Unlock()
		if !everOpen && time.Since(start) > neverConnectedWait {
			log.Printf("no peer ever connected; giving up")
			d.stop()
			return
		}
		if !started && time.Since(start) > firstOfferWait {
			log.Printf("no client connected; giving up")
			d.stop()
			return
		}
		if exited {
			if exitedAt.IsZero() {
				exitedAt = time.Now()
			}
			if !connected {
				if goneSince.IsZero() {
					goneSince = time.Now()
				}
			} else {
				goneSince = time.Time{}
			}
			// the shell is gone and so is the client: nobody will collect
			// the exit status unless the client comes back within a minute
			if !goneSince.IsZero() && time.Since(goneSince) > time.Minute {
				log.Printf("shell exited, client gone")
				d.stop()
				return
			}
			if acked || (d.opts.Linger > 0 && time.Since(exitedAt) > d.opts.Linger) {
				time.Sleep(200 * time.Millisecond)
				d.stop()
				return
			}
		}
	}
}

// --- control socket ---------------------------------------------------------

// Control protocol: JSON wire.Msg per line. A relay sends an offer and gets
// an answer (or err). TBye from a relay before any client connected aborts.
func (d *Daemon) acceptLoop() {
	for {
		c, err := d.ln.Accept()
		if err != nil {
			return
		}
		go d.serveConn(c)
	}
}

func (d *Daemon) serveConn(c net.Conn) {
	gone := make(chan struct{})
	d.mu.Lock()
	d.conns[c] = gone
	d.mu.Unlock()
	defer func() {
		c.Close()
		d.mu.Lock()
		delete(d.conns, c)
		d.mu.Unlock()
		close(gone)
	}()
	go func() { <-d.quit; c.Close() }()
	r := bufio.NewReaderSize(c, 64*1024)
	enc := json.NewEncoder(c)
	anchored := false
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		var m wire.Msg
		if err := json.Unmarshal(line, &m); err != nil {
			_ = enc.Encode(wire.Msg{T: wire.TError, Err: "bad message"})
			continue
		}
		switch m.T {
		case wire.TOffer:
			ans, err := d.handleOffer(m)
			if err != nil {
				log.Printf("offer failed: %v", err)
				_ = enc.Encode(wire.Msg{T: wire.TError, ID: d.opts.ID, Err: err.Error()})
				continue
			}
			_ = enc.Encode(ans)
		case wire.TBye:
			d.mu.Lock()
			started := d.started
			d.mu.Unlock()
			if !started {
				d.stop()
			}
		case wire.TPark:
			anchored = true
		case wire.TInfo:
			d.mu.Lock()
			u, src := d.clientUser, d.src
			d.mu.Unlock()
			_ = enc.Encode(wire.Msg{T: wire.TInfo, User: u, Src: src})
		case wire.TType:
			d.mu.Lock()
			ptmx := d.ptmx
			d.mu.Unlock()
			if ptmx == nil || len(m.Text) > 4096 {
				_ = enc.Encode(wire.Msg{T: wire.TError, Err: "no session terminal"})
				continue
			}
			_, _ = ptmx.Write([]byte(m.Text))
			_ = enc.Encode(wire.Msg{T: wire.TOK})
		case wire.TOpen:
			// a local helper (ssh/scp shim) wants a stream to the client;
			// after the reply the connection carries stream frames
			if m.Stream == nil {
				continue
			}
			ch, err := d.openToClient(*m.Stream)
			if err != nil {
				_ = enc.Encode(wire.Msg{T: wire.TError, Err: err.Error()})
				return
			}
			_ = enc.Encode(wire.Msg{T: wire.TOK})
			stream.Relay(ch, stream.NewFramed(r, c, c))
			return
		}
	}
	d.mu.Lock()
	exited := d.exited
	d.mu.Unlock()
	if anchored && runtime.GOOS == "darwin" && !exited {
		// The ssh session that launched us is gone. macOS will block our LAN
		// sends within ~1s (Local Network privacy); close the peer now while
		// the goodbye still gets through, so the client reconnects at once
		// (and the new offer is probed) instead of waiting for a timeout.
		d.mu.Lock()
		p := d.cur
		d.mu.Unlock()
		if p != nil {
			log.Printf("anchor ssh closed: dropping peer so the client renegotiates")
			_ = p.pc.Close()
		}
	}
}

// --- session ----------------------------------------------------------------

func (d *Daemon) startShell(m wire.Msg) error {
	var cmd *exec.Cmd
	if len(d.opts.Command) > 0 {
		cmd = exec.Command(d.opts.Command[0], d.opts.Command[1:]...)
	} else {
		sh := LoginShell()
		cmd = exec.Command(sh)
		base := sh[strings.LastIndex(sh, "/")+1:]
		cmd.Args = []string{"-" + base} // login shell
	}
	if home, err := os.UserHomeDir(); err == nil {
		cmd.Dir = home
	}
	term := m.Term
	if term == "" {
		term = "xterm-256color"
	}
	cmd.Env = d.sessionEnv(term, m.Env)
	size := &pty.Winsize{Cols: m.Cols, Rows: m.Rows}
	if size.Cols == 0 || size.Rows == 0 {
		size = &pty.Winsize{Cols: 80, Rows: 24}
	}
	ptmx, err := pty.StartWithSize(cmd, size)
	if err != nil {
		return err
	}
	d.ptmx, d.cmd = ptmx, cmd
	go d.readPTY()
	return nil
}

func (d *Daemon) readPTY() {
	buf := make([]byte, 32*1024)
	for {
		n, err := d.ptmx.Read(buf)
		if n > 0 {
			d.mu.Lock()
			d.out.append(buf[:n])
			d.mu.Unlock()
			d.poke()
		}
		if err != nil {
			break
		}
	}
	code := 0
	if err := d.cmd.Wait(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else {
			code = 255
		}
	}
	log.Printf("shell exited with %d", code)
	d.mu.Lock()
	d.exited, d.exitCode = true, code
	// Disconnect relays parked in a parent terminal (`wosh up`) *before*
	// telling the client, so the parent shell owns its tty again by the
	// time the client routes keystrokes back to it.
	var waits []chan struct{}
	for c, gone := range d.conns {
		_ = c.Close()
		waits = append(waits, gone)
	}
	d.mu.Unlock()
	for _, g := range waits {
		select {
		case <-g:
		case <-time.After(time.Second):
		}
	}
	if len(waits) > 0 {
		time.Sleep(150 * time.Millisecond) // let the relay restore the tty
	}
	d.mu.Lock()
	d.exitSent = true
	d.mu.Unlock()
	d.poke()
}

// sender streams retained output to the current peer.
func (d *Daemon) sender() {
	for {
		select {
		case <-d.quit:
			return
		case <-d.kick:
		case <-time.After(500 * time.Millisecond):
		}
		for d.sendSome() {
		}
	}
}

// sendSome sends one chunk; returns true if more may be pending.
func (d *Daemon) sendSome() bool {
	d.mu.Lock()
	p := d.cur
	if p == nil || !p.open {
		d.mu.Unlock()
		return false
	}
	if p.dc.BufferedAmount() > maxBuffered {
		d.mu.Unlock()
		select {
		case <-p.low:
		case <-time.After(time.Second):
		case <-d.quit:
		}
		return true
	}
	data, off := d.out.slice(p.sent, wire.MaxChunk)
	if len(data) == 0 {
		if d.exitSent && !p.exitOut {
			p.exitOut = true
			code := d.exitCode
			d.mu.Unlock()
			if p.dc.Send(wire.ExitFrame(code)) == nil {
				d.mu.Lock()
				d.exitDelivered = true
				d.mu.Unlock()
			}
			return false
		}
		d.mu.Unlock()
		return false
	}
	frame := wire.OffsetFrame(wire.FOutput, off, data)
	p.sent = off + uint64(len(data))
	d.mu.Unlock()
	if err := p.dc.Send(frame); err != nil {
		return false
	}
	return true
}

func (d *Daemon) handleOffer(m wire.Msg) (wire.Msg, error) {
	d.mu.Lock()
	if d.key == "" {
		if len(m.Key) < 16 {
			d.mu.Unlock()
			return wire.Msg{}, errors.New("missing session key")
		}
		d.key = m.Key
	} else if subtle.ConstantTimeCompare([]byte(d.key), []byte(m.Key)) != 1 {
		d.mu.Unlock()
		return wire.Msg{}, errors.New("bad session key")
	}
	if m.User != "" {
		d.clientUser = m.User
	}
	if !d.started {
		d.src = m.Src
		if err := d.startShell(m); err != nil {
			d.mu.Unlock()
			return wire.Msg{}, fmt.Errorf("start shell: %w", err)
		}
		d.started = true
	}
	d.mu.Unlock()

	pc, err := rtc.NewAPI().NewPeerConnection(rtc.Config(m.ICE, m.Relay))
	if err != nil {
		return wire.Msg{}, err
	}
	p := &peer{pc: pc, sent: m.RxOut, low: make(chan struct{}, 1)}

	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if spec, ok := stream.ParseLabel(dc.Label()); ok {
			go d.fromClient(stream.Wrap(dc), spec)
			return
		}
		if dc.Label() != "wosh" {
			return
		}
		p.dc = dc
		dc.SetBufferedAmountLowThreshold(maxBuffered / 2)
		dc.OnBufferedAmountLow(func() {
			select {
			case p.low <- struct{}{}:
			default:
			}
		})
		dc.OnOpen(func() {
			d.mu.Lock()
			old := d.cur
			d.cur = p
			p.open = true
			d.everOpen = true
			d.mu.Unlock()
			if old != nil && old != p {
				go old.pc.Close()
			}
			log.Printf("peer connected")
			_ = dc.Send(wire.U64Frame(wire.FAck, d.inputRx()))
			_ = dc.Send(wire.ControlFrame(wire.Control{Op: "info", Token: d.token}))
			d.poke()
		})
		dc.OnMessage(func(msg webrtc.DataChannelMessage) { d.onFrame(p, msg.Data) })
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Printf("peer state %s", s)
		if s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed {
			d.mu.Lock()
			if d.cur == p {
				p.open = false
			}
			d.mu.Unlock()
			if s == webrtc.PeerConnectionStateFailed {
				_ = pc.Close()
			}
		}
	})

	offerSDP := m.SDP
	if runtime.GOOS == "darwin" && !m.Anchor {
		// (an offer relayed by the ssh session that launched us needs no
		// probe: LAN sends are allowed while that session exists)
		offerSDP = rtc.DropUnreachableCandidates(offerSDP)
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offerSDP}); err != nil {
		pc.Close()
		return wire.Msg{}, err
	}
	ans, err := pc.CreateAnswer(nil)
	if err != nil {
		pc.Close()
		return wire.Msg{}, err
	}
	if err := pc.SetLocalDescription(ans); err != nil {
		pc.Close()
		return wire.Msg{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = rtc.WaitGathered(ctx, pc)
	return wire.Msg{T: wire.TAnswer, ID: d.opts.ID, SDP: pc.LocalDescription().SDP}, nil
}

func (d *Daemon) inputRx() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inRx
}

func (d *Daemon) onFrame(p *peer, b []byte) {
	if len(b) == 0 {
		return
	}
	switch b[0] {
	case wire.FInput:
		off, data, ok := wire.ParseOffset(b)
		if !ok {
			return
		}
		d.mu.Lock()
		if off+uint64(len(data)) <= d.inRx {
			d.mu.Unlock()
			return
		}
		if off < d.inRx {
			data = data[d.inRx-off:]
		}
		d.inRx = off + uint64(len(b)-9)
		ptmx, rx := d.ptmx, d.inRx
		d.mu.Unlock()
		if ptmx != nil {
			_, _ = ptmx.Write(data)
		}
		_ = p.dc.Send(wire.U64Frame(wire.FAck, rx))
	case wire.FResize:
		if cols, rows, ok := wire.ParseResize(b); ok && cols > 0 && rows > 0 {
			d.mu.Lock()
			ptmx := d.ptmx
			d.mu.Unlock()
			if ptmx != nil {
				_ = pty.Setsize(ptmx, &pty.Winsize{Cols: cols, Rows: rows})
			}
		}
	case wire.FRedraw:
		d.mu.Lock()
		ptmx := d.ptmx
		d.mu.Unlock()
		if ptmx != nil {
			if sz, err := pty.GetsizeFull(ptmx); err == nil && sz.Rows > 1 {
				// jiggle the size so full-screen apps get SIGWINCH and repaint
				_ = pty.Setsize(ptmx, &pty.Winsize{Cols: sz.Cols, Rows: sz.Rows - 1})
				time.Sleep(20 * time.Millisecond)
				_ = pty.Setsize(ptmx, sz)
			}
		}
	case wire.FPing:
		b2 := append([]byte(nil), b...)
		b2[0] = wire.FPong
		_ = p.dc.Send(b2)
	case wire.FExitAck:
		d.mu.Lock()
		d.exitAck = true
		d.mu.Unlock()
	case wire.FControl:
		if c, ok := wire.ParseControl(b); ok && c.Op == "rfwd" {
			d.listenRemote(c.Rfwd)
		}
	case wire.FKill:
		d.mu.Lock()
		cmd, exited := d.cmd, d.exited
		d.mu.Unlock()
		if cmd != nil && cmd.Process != nil && !exited {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGHUP)
			_ = cmd.Process.Signal(syscall.SIGHUP)
		}
		go func() {
			time.Sleep(3 * time.Second)
			d.stop()
		}()
	}
}

// --- streams --------------------------------------------------------------------

// openToClient opens a stream from this host to the client.
func (d *Daemon) openToClient(spec wire.StreamSpec) (*stream.Chan, error) {
	deadline := time.Now().Add(20 * time.Second)
	for {
		d.mu.Lock()
		p := d.cur
		d.mu.Unlock()
		if p != nil && p.open {
			return stream.Open(p.pc, spec)
		}
		if time.Now().After(deadline) {
			return nil, errors.New("wosh client not connected")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// fromClient serves a stream the client opened: -L / -D forwards, which this
// host dials (like ssh's direct-tcpip).
func (d *Daemon) fromClient(ch *stream.Chan, spec wire.StreamSpec) {
	if spec.K != "tcp" || spec.Port <= 0 || spec.Port > 65535 {
		_ = ch.Send(stream.Err, []byte("unsupported stream"))
		_ = ch.Close()
		return
	}
	c, err := net.DialTimeout("tcp", net.JoinHostPort(spec.Host, fmt.Sprint(spec.Port)), 10*time.Second)
	if err != nil {
		_ = ch.Send(stream.Err, []byte(err.Error()))
		_ = ch.Close()
		return
	}
	stream.Splice(ch, c)
}

// listenRemote starts -R listeners (idempotent: the client resends its list
// after every reconnect).
func (d *Daemon) listenRemote(fwds []wire.RemoteFwd) {
	for _, f := range fwds {
		d.mu.Lock()
		_, exists := d.rfwd[f.ID]
		d.mu.Unlock()
		if exists {
			continue
		}
		bind := f.Bind
		if bind == "" {
			bind = "localhost"
		}
		l, err := net.Listen("tcp", net.JoinHostPort(bind, fmt.Sprint(f.Port)))
		if err != nil {
			log.Printf("-R %s:%d: %v", bind, f.Port, err)
			continue
		}
		d.mu.Lock()
		d.rfwd[f.ID] = l
		d.mu.Unlock()
		log.Printf("-R listening on %s (id %d)", l.Addr(), f.ID)
		go func(id int) {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				go func() {
					ch, err := d.openToClient(wire.StreamSpec{K: "rfwd", ID: id})
					if err != nil {
						_ = c.Close()
						return
					}
					stream.Splice(ch, c)
				}()
			}
		}(f.ID)
	}
}
