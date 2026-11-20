package server

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
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
	defaultTail    = 64 << 10 // TTail without N
	maxTail        = 1 << 20
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
	// ICEInterfaces: gather candidates on these interfaces only (none: all).
	ICEInterfaces []string
	// RingSize: output bytes kept for reconnecting peers (0: ringSize).
	RingSize int
	// NoForward refuses every stream: -L/-D dials, -R listeners, the agent
	// socket and src: transfers. The session is a terminal and nothing else.
	NoForward bool
}

var errNoForward = errors.New("forwarding is disabled on this session")

// Daemon owns one PTY session and serves WebRTC peers for it.
type Daemon struct {
	opts DaemonOptions

	mu       sync.Mutex
	key      string
	ptmx     *os.File
	cmd      *exec.Cmd
	out      *ring
	inRx     uint64
	peers    map[*peer]bool // open or opening peers
	pending  map[*peer]bool // answered, data channel not open yet
	ctrl     *peer          // the peer in control (input, resize, kill, forwards)
	ctrlGone time.Time      // when the controller went away (viewers get promoted later)
	joined   uint64         // peer join counter (oldest viewer is promoted first)
	cols     uint16         // session terminal size, told to every peer with a CID
	rows     uint16
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
	inLog      []inputMark // input written to the pty, for echo acks
	echoed     uint64
	streams    *stream.Registry

	sock     string      // control socket path
	sockInfo os.FileInfo // ...and the file we created (a new daemon may reuse the path)

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
	echoOut uint64 // last echo ack sent to this peer
	low     chan struct{}
	cid     string // client instance (supports roles); "" = legacy, single peer
	seq     uint64 // join order
	passive bool   // watches only (recorder): never controls
	dropped bool   // closed by TDrop: never (re)joins
	// streams this peer opened or resumed (-L/-D): killed by TDrop rather
	// than left waiting SuspendFor to be resumed
	streams []*stream.Resumable
}

// promoteAfter: when the controller is gone this long (and did not reconnect,
// e.g. after roaming), the longest-watching viewer gets control.
const promoteAfter = 10 * time.Second

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
	sockInfo, _ := os.Stat(sock)
	defer func() {
		// remove the socket only if it is still ours
		if st, err := os.Stat(sock); err == nil && sockInfo != nil && os.SameFile(st, sockInfo) {
			_ = os.Remove(sock)
		}
	}()

	keep := opts.RingSize
	if keep <= 0 {
		keep = ringSize
	}
	d := &Daemon{
		opts:    opts,
		out:     newRing(keep),
		kick:    make(chan struct{}, 1),
		quit:    make(chan struct{}),
		ln:      ln,
		conns:   map[net.Conn]chan struct{}{},
		peers:   map[*peer]bool{},
		pending: map[*peer]bool{},
		token:   newToken(),
		rfwd:    map[int]net.Listener{},
		streams: stream.NewRegistry(),
		sock:    sock, sockInfo: sockInfo,
	}
	if err := d.setupBinDir(); err != nil {
		log.Printf("session bin dir: %v", err)
	}
	if opts.ReadyFD != nil {
		_, _ = opts.ReadyFD.Write([]byte{1})
		_ = opts.ReadyFD.Close()
	}
	log.Printf("daemon %s listening on %s", opts.ID, sock)
	for _, n := range opts.ICEInterfaces {
		if _, err := net.InterfaceByName(n); err != nil {
			log.Printf("--ice-iface %s: %v (no candidates from it)", n, err)
		}
	}

	go d.acceptLoop()
	go d.sender()
	go d.watchdog()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		<-sigs
		d.hangUp()
		d.stop()
	}()
	<-d.quit
	_ = ln.Close()
	d.mu.Lock()
	for p := range d.peers {
		_ = p.pc.Close()
	}
	d.mu.Unlock()
	for _, l := range d.rfwd {
		_ = l.Close()
	}
	d.streams.KillAll()
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

// unreachableWait: how long a daemon without a control socket and without a
// client lives on.
const unreachableWait = 5 * time.Second

// sockOK reports whether our control socket file is still there (and ours).
func (d *Daemon) sockOK() bool {
	if d.sockInfo == nil {
		return true // never knew it: do not guess
	}
	st, err := os.Stat(d.sock)
	return err == nil && os.SameFile(st, d.sockInfo)
}

// hangUp sends SIGHUP to the shell's process group (as a terminal hangup).
func (d *Daemon) hangUp() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cmd != nil && d.cmd.Process != nil && !d.exited {
		_ = syscall.Kill(-d.cmd.Process.Pid, syscall.SIGHUP)
	}
}

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
	var exitedAt, orphanSince, goneSince, unreachableSince time.Time
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
		delivered, connected := d.exitDelivered, d.anyOpenLocked()
		d.promoteLocked()
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
		// Nobody can reach a daemon whose control socket was deleted (its
		// runtime dir removed: tests, logout without linger). Once no client
		// is connected either, the session is lost for good: hang it up.
		if !connected && !d.sockOK() {
			if unreachableSince.IsZero() {
				unreachableSince = time.Now()
			}
			if time.Since(unreachableSince) > unreachableWait {
				log.Printf("control socket %s is gone and no client is connected: hanging up", d.sock)
				d.hangUp()
				d.stop()
				return
			}
		} else {
			unreachableSince = time.Time{}
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
		case wire.TDrop:
			// an embedding server revoked these peers: close their peer
			// connections, which also ends every stream channel on them
			d.dropPeers(m.CID)
			_ = enc.Encode(wire.Msg{T: wire.TOK})
		case wire.TKill:
			_ = enc.Encode(wire.Msg{T: wire.TOK})
			d.killSession()
		case wire.TTail:
			// recent output for a local tool (e.g. a "what is on screen"
			// peek) without connecting a peer
			n := m.N
			if n <= 0 {
				n = defaultTail
			}
			n = min(n, maxTail)
			d.mu.Lock()
			data, off := d.out.tail(n)
			d.mu.Unlock()
			_ = enc.Encode(wire.Msg{T: wire.TOK, Data: data, Off: off})
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
		var ps []*peer
		for p := range d.peers {
			ps = append(ps, p)
		}
		d.mu.Unlock()
		if len(ps) > 0 {
			log.Printf("anchor ssh closed: dropping peers so clients renegotiate")
		}
		for _, p := range ps {
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
	env := m.Env
	if m.Agent && d.baseDir != "" && !d.opts.NoForward {
		if sock, err := d.listenAgent(); err == nil {
			env = maps.Clone(env)
			if env == nil {
				env = map[string]string{}
			}
			env["SSH_AUTH_SOCK"] = sock
			env["WOSH_AGENT_SOCK"] = sock // survives rc files that set SSH_AUTH_SOCK (--fix-shell)
		} else {
			log.Printf("agent forwarding: %v", err)
		}
	}
	cmd.Env = d.sessionEnv(term, env)
	size := &pty.Winsize{Cols: m.Cols, Rows: m.Rows}
	if size.Cols == 0 || size.Rows == 0 {
		size = &pty.Winsize{Cols: 80, Rows: 24}
	}
	ptmx, err := pty.StartWithSize(cmd, size)
	if err != nil {
		return err
	}
	d.ptmx, d.cmd = ptmx, cmd
	d.cols, d.rows = size.Cols, size.Rows
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
		case <-time.After(d.senderWait()):
		}
		for d.sendSome() {
		}
	}
}

// sendSome sends one chunk to every peer that can take it; returns true if
// more may be pending. A slow peer is skipped (its low-water callback pokes
// the sender), so it cannot hold up the others.
func (d *Daemon) sendSome() bool {
	d.mu.Lock()
	var ps []*peer
	for p := range d.peers {
		if p.open {
			ps = append(ps, p)
		}
	}
	d.mu.Unlock()
	more := false
	for _, p := range ps {
		if d.sendTo(p) {
			more = true
		}
	}
	return more
}

func (d *Daemon) sendTo(p *peer) bool {
	if p.dc.BufferedAmount() > maxBuffered {
		return false
	}
	d.mu.Lock()
	if !p.open {
		d.mu.Unlock()
		return false
	}
	data, off := d.out.slice(p.sent, wire.MaxChunk)
	if len(data) == 0 {
		// output is caught up: report which input the app has had time to
		// answer (mosh's echo ack) so the client can judge its predictions
		if echo := d.echoAckLocked(); echo > p.echoOut && p == d.ctrl {
			p.echoOut = echo
			d.mu.Unlock()
			_ = p.dc.Send(wire.U64Frame(wire.FEchoAck, echo))
			return true
		}
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

// --- roles ------------------------------------------------------------------

func (d *Daemon) anyOpenLocked() bool {
	for p := range d.peers {
		if p.open {
			return true
		}
	}
	return false
}

// setCtrlLocked gives p control. The previous controller is told it now
// watches; a legacy one (no CID) is dropped instead, as before roles.
func (d *Daemon) setCtrlLocked(p *peer) {
	old := d.ctrl
	d.ctrl = p
	d.ctrlGone = time.Time{}
	if old == nil || old == p {
		return
	}
	if old.cid == "" {
		delete(d.peers, old)
		old.open = false
		go old.pc.Close()
		return
	}
	go d.sendRole(old, "view")
}

func (d *Daemon) sendRole(p *peer, role string) {
	if p.cid == "" || p.dc == nil {
		return
	}
	_ = p.dc.Send(wire.ControlFrame(wire.Control{Op: "role", Role: role}))
	if role == "control" {
		// the input offset the new controller must continue from
		_ = p.dc.Send(wire.U64Frame(wire.FAck, d.inputRx()))
	}
}

// take gives control to p (a viewer that asked for it).
func (d *Daemon) take(p *peer) {
	d.mu.Lock()
	if !p.open || p.cid == "" || p.passive || d.ctrl == p {
		d.mu.Unlock()
		return
	}
	d.setCtrlLocked(p)
	d.mu.Unlock()
	log.Printf("control taken by %s", p.cid)
	d.sendRole(p, "control")
}

// promoteLocked hands control to the longest-watching viewer once the
// controller has been gone for promoteAfter.
func (d *Daemon) promoteLocked() {
	if d.ctrl != nil || d.ctrlGone.IsZero() || time.Since(d.ctrlGone) < promoteAfter {
		return
	}
	var best *peer
	for p := range d.peers {
		if p.open && p.cid != "" && !p.passive && (best == nil || p.seq < best.seq) {
			best = p
		}
	}
	if best == nil {
		return
	}
	d.setCtrlLocked(best)
	log.Printf("controller gone: control passed to %s", best.cid)
	go d.sendRole(best, "control")
}

func (d *Daemon) size() (uint16, uint16) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cols, d.rows
}

// setSize records the terminal size the controller set and tells the other
// peers, so watchers draw the same grid (scaled to their screens).
func (d *Daemon) setSize(cols, rows uint16) {
	d.mu.Lock()
	if cols == d.cols && rows == d.rows {
		d.mu.Unlock()
		return
	}
	d.cols, d.rows = cols, rows
	var ps []*peer
	for p := range d.peers {
		if p.open && p.cid != "" {
			ps = append(ps, p)
		}
	}
	d.mu.Unlock()
	f := wire.ControlFrame(wire.Control{Op: "size", Cols: cols, Rows: rows})
	for _, p := range ps {
		_ = p.dc.Send(f)
	}
}

func (d *Daemon) isCtrl(p *peer) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ctrl == p
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

	pc, err := rtc.NewAPI(rtc.Interfaces(d.opts.ICEInterfaces)).NewPeerConnection(rtc.Config(m.ICE, m.Relay))
	if err != nil {
		return wire.Msg{}, err
	}
	p := &peer{pc: pc, sent: m.RxOut, low: make(chan struct{}, 1), cid: m.CID, passive: m.Watch && m.CID != ""}
	join := (m.Join || p.passive) && m.CID != ""
	d.mu.Lock()
	d.pending[p] = true
	d.mu.Unlock()

	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if spec, ok := stream.ParseLabel(dc.Label()); ok {
			r, isNew := stream.Accept(dc, spec, d.streams.Get)
			if r == nil {
				return
			}
			d.mu.Lock()
			p.streams = append(p.streams, r)
			dropped := p.dropped
			d.mu.Unlock()
			if dropped {
				r.Kill()
				return
			}
			if isNew {
				d.streams.Add(r)
				go d.fromClient(r, spec)
			}
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
			d.poke()
		})
		dc.OnOpen(func() {
			d.mu.Lock()
			delete(d.pending, p)
			if p.dropped {
				d.mu.Unlock()
				_ = pc.Close()
				return
			}
			// an earlier connection of the same client is stale (it roamed)
			sameAsCtrl := false
			for q := range d.peers {
				if q != p && q.cid != "" && q.cid == p.cid {
					if q == d.ctrl {
						sameAsCtrl = true
						d.ctrl = nil
					}
					delete(d.peers, q)
					q.open = false
					go q.pc.Close()
				}
			}
			hadCtrl := d.ctrl != nil
			d.joined++
			p.seq = d.joined
			d.peers[p] = true
			p.open = true
			d.everOpen = true
			control := !p.passive && (!join || sameAsCtrl || d.ctrl == nil)
			if control {
				d.setCtrlLocked(p)
			}
			d.mu.Unlock()
			role := map[bool]string{true: "control", false: "view"}[control]
			log.Printf("peer connected (%s%s)", role, map[bool]string{true: ", " + p.cid, false: ""}[p.cid != ""])
			_ = dc.Send(wire.U64Frame(wire.FAck, d.inputRx()))
			_ = dc.Send(wire.ControlFrame(wire.Control{Op: "info", Token: d.token}))
			if p.cid != "" {
				_ = dc.Send(wire.ControlFrame(wire.Control{Op: "role", Role: role}))
				if c, r := d.size(); c > 0 {
					_ = dc.Send(wire.ControlFrame(wire.Control{Op: "size", Cols: c, Rows: r}))
				}
			}
			if control && (hadCtrl || sameAsCtrl) {
				// streams we opened (-R, src:, agent) continue on the new link
				d.streams.ReopenAll(pc)
			}
			d.poke()
		})
		dc.OnMessage(func(msg webrtc.DataChannelMessage) { d.onFrame(p, msg.Data) })
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Printf("peer state %s", s)
		if s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed {
			d.mu.Lock()
			p.open = false
			delete(d.pending, p)
			if d.peers[p] {
				delete(d.peers, p)
				if d.ctrl == p {
					d.ctrl = nil
					d.ctrlGone = time.Now()
				}
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
	case wire.FInput, wire.FResize, wire.FKill:
		if !d.isCtrl(p) {
			return // viewers watch; only the controller types, resizes or kills
		}
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
		d.mu.Lock()
		d.inLog = append(d.inLog, inputMark{off: rx, at: time.Now()})
		d.mu.Unlock()
		d.poke()
		_ = p.dc.Send(wire.U64Frame(wire.FAck, rx))
	case wire.FResize:
		if cols, rows, ok := wire.ParseResize(b); ok && cols > 0 && rows > 0 {
			d.mu.Lock()
			ptmx := d.ptmx
			d.mu.Unlock()
			if ptmx != nil {
				_ = pty.Setsize(ptmx, &pty.Winsize{Cols: cols, Rows: rows})
				d.setSize(cols, rows)
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
		c, ok := wire.ParseControl(b)
		switch {
		case !ok:
		case c.Op == "rfwd" && d.isCtrl(p) && !d.opts.NoForward:
			d.listenRemote(c.Rfwd)
		case c.Op == "take":
			d.take(p)
		}
	case wire.FKill:
		d.killSession()
	}
}

// killSession hangs up the shell and ends the daemon shortly after (the
// controller's FKill, or TKill on the control socket).
func (d *Daemon) killSession() {
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

// dropPeers closes the peers of client instance cid (every peer if cid is
// ""), including ones still connecting. Closing the peer connection ends its
// data channels: the terminal and any stream channels on it.
func (d *Daemon) dropPeers(cid string) {
	d.mu.Lock()
	var ps []*peer
	for p := range d.pending {
		if cid == "" || p.cid == cid {
			p.dropped = true
			delete(d.pending, p)
			ps = append(ps, p)
		}
	}
	for p := range d.peers {
		if cid == "" || p.cid == cid {
			p.dropped = true
			p.open = false
			delete(d.peers, p)
			if d.ctrl == p {
				d.ctrl = nil
				d.ctrlGone = time.Now()
			}
			ps = append(ps, p)
		}
	}
	var rs []*stream.Resumable
	for _, p := range ps {
		rs = append(rs, p.streams...)
		p.streams = nil
	}
	d.mu.Unlock()
	if len(ps) > 0 {
		log.Printf("dropping %d peer(s) (cid %q)", len(ps), cid)
	}
	for _, p := range ps {
		_ = p.pc.Close()
	}
	for _, r := range rs {
		r.Kill()
	}
}

// --- streams --------------------------------------------------------------------

// openToClient opens a stream from this host to the client.
func (d *Daemon) openToClient(spec wire.StreamSpec) (*stream.Resumable, error) {
	if d.opts.NoForward {
		return nil, errNoForward
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		d.mu.Lock()
		p := d.ctrl
		d.mu.Unlock()
		if p != nil && p.open {
			r, err := stream.OpenResumable(p.pc, spec)
			if err == nil {
				d.streams.Add(r)
			}
			return r, err
		}
		if time.Now().After(deadline) {
			return nil, errors.New("wosh client not connected")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// fromClient serves a stream the client opened: -L / -D forwards, which this
// host dials (like ssh's direct-tcpip).
func (d *Daemon) fromClient(ch stream.Conn, spec wire.StreamSpec) {
	if d.opts.NoForward {
		_ = ch.Send(stream.Err, []byte(errNoForward.Error()))
		_ = ch.Close()
		return
	}
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
	// the list is the desired set: drop listeners not in it (the client
	// moved its destination elsewhere)
	want := map[int]bool{}
	for _, f := range fwds {
		want[f.ID] = true
	}
	d.mu.Lock()
	for id, l := range d.rfwd {
		if !want[id] {
			_ = l.Close()
			delete(d.rfwd, id)
		}
	}
	d.mu.Unlock()
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
		var l net.Listener
		var err error
		for try := 0; try < 25; try++ { // the previous destination may still hold the port
			if l, err = net.Listen("tcp", net.JoinHostPort(bind, fmt.Sprint(f.Port))); err == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
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

// listenAgent exposes the client's ssh-agent in this session (-A).
func (d *Daemon) listenAgent() (string, error) {
	sock := filepath.Join(d.baseDir, "agent.sock")
	_ = os.Remove(sock)
	l, err := net.Listen("unix", sock)
	if err != nil {
		return "", err
	}
	_ = os.Chmod(sock, 0o600)
	go func() {
		<-d.quit
		_ = l.Close()
	}()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				ch, err := d.openToClient(wire.StreamSpec{K: "agent"})
				if err != nil {
					_ = c.Close()
					return
				}
				stream.Splice(ch, c)
			}()
		}
	}()
	return sock, nil
}

// echoDelay: how long the application gets to echo a keystroke before the
// client may judge its prediction (mosh uses 50ms).
const echoDelay = 50 * time.Millisecond

type inputMark struct {
	off uint64
	at  time.Time
}

// echoAckLocked returns the input offset that reached the pty at least
// echoDelay ago (d.mu held).
func (d *Daemon) echoAckLocked() uint64 {
	cut := time.Now().Add(-echoDelay)
	var echo uint64
	i := 0
	for i < len(d.inLog) && !d.inLog[i].at.After(cut) {
		echo = d.inLog[i].off
		i++
	}
	if i > 0 {
		d.inLog = d.inLog[i:]
		d.echoed = echo
	}
	return d.echoed
}

// senderWait: wake up soon while echo acks are pending.
func (d *Daemon) senderWait() time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.inLog) > 0 {
		return 10 * time.Millisecond
	}
	return 500 * time.Millisecond
}
