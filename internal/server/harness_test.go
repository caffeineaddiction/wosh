package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"wosh/internal/rtc"
	"wosh/internal/wire"
)

// Harness for tests that drive a real `wosh _daemon` through its control
// socket and a WebRTC peer, the way a relay (or an embedding server) does.
// Everything stays on loopback: the daemon gets --ice-iface <lo> and the
// test peer gathers on the loopback interface only.

var (
	buildOnce sync.Once
	buildBin  string
	buildErr  error
	buildOut  []byte
)

func TestMain(m *testing.M) {
	code := m.Run()
	if buildBin != "" {
		_ = os.RemoveAll(filepath.Dir(buildBin))
	}
	os.Exit(code)
}

func woshBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "wosh-test-bin")
		if err != nil {
			buildErr = err
			return
		}
		buildBin = filepath.Join(dir, "wosh")
		cmd := exec.Command("go", "build", "-o", buildBin, "wosh")
		cmd.Dir = "../.."
		buildOut, buildErr = cmd.CombinedOutput()
	})
	if buildErr != nil {
		t.Fatalf("build: %v\n%s", buildErr, buildOut)
	}
	return buildBin
}

func loopbackIface(t *testing.T) string {
	t.Helper()
	ifs, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range ifs {
		if i.Flags&net.FlagLoopback != 0 && i.Flags&net.FlagUp != 0 {
			return i.Name
		}
	}
	t.Skip("no loopback interface")
	return ""
}

type testDaemon struct {
	t    *testing.T
	id   string
	sock string
	key  string
	cmd  *exec.Cmd
	done chan struct{}
	log  string
}

// startDaemon runs `wosh _daemon --id X [args] -- /bin/sh` in a private
// runtime dir and waits until its control socket listens.
func startDaemon(t *testing.T, args ...string) *testDaemon {
	t.Helper()
	if testing.Short() {
		t.Skip("e2e")
	}
	bin := woshBinary(t)
	tmp := t.TempDir()
	// short runtime dir: t.TempDir() embeds the test name, and the control
	// socket path must fit sun_path (104 bytes on macOS)
	rt, err := os.MkdirTemp("", "wosh")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(rt) })
	lo := loopbackIface(t)
	id := NewID()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	argv := append([]string{"_daemon", "--id", id, "--ice-iface", lo}, args...)
	argv = append(argv, "--", "/bin/sh")
	cmd := exec.Command(bin, argv...)
	cmd.Env = []string{
		"WOSH_RUNTIME_DIR=" + rt, "WOSH_KEEP_LOGS=1",
		"HOME=" + tmp, "PATH=/usr/bin:/bin", "SHELL=/bin/sh", "ENV=", "PS1=$ ",
	}
	logPath := filepath.Join(tmp, "daemon.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer lf.Close()
	cmd.Stdout, cmd.Stderr = lf, lf
	cmd.ExtraFiles = []*os.File{w} // fd 3: ready
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	_ = lf.Close()
	d := &testDaemon{t: t, id: id, sock: filepath.Join(rt, id+".sock"), key: "test-session-key-0123456789",
		cmd: cmd, done: make(chan struct{}), log: logPath}
	go func() { _ = cmd.Wait(); close(d.done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-d.done
		if t.Failed() {
			b, _ := os.ReadFile(logPath)
			t.Logf("daemon log:\n%s", b)
		}
	})
	ready := make(chan error, 1)
	go func() {
		b := make([]byte, 1)
		_, err := r.Read(b)
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("daemon not ready: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon not ready")
	}
	return d
}

// ctl sends one control socket message and returns the reply.
func (d *testDaemon) ctl(m wire.Msg) wire.Msg {
	d.t.Helper()
	c, err := net.Dial("unix", d.sock)
	if err != nil {
		d.t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(20 * time.Second))
	if err := json.NewEncoder(c).Encode(m); err != nil {
		d.t.Fatal(err)
	}
	line, err := bufio.NewReaderSize(c, 4<<20).ReadBytes('\n')
	if err != nil {
		d.t.Fatalf("ctl %s: %v", m.T, err)
	}
	var reply wire.Msg
	if err := json.Unmarshal(line, &reply); err != nil {
		d.t.Fatal(err)
	}
	return reply
}

// exited reports whether the daemon process ended within wait.
func (d *testDaemon) exited(wait time.Duration) bool {
	select {
	case <-d.done:
		return true
	case <-time.After(wait):
		return false
	}
}

type testPeer struct {
	t        *testing.T
	pc       *webrtc.PeerConnection
	dc       *webrtc.DataChannel
	opened   chan struct{}
	closed   chan struct{}
	firstAck chan struct{}
	answer   string // the daemon's SDP answer

	mu    sync.Mutex
	out   bytes.Buffer
	role  string
	inTx  uint64
	acked bool
}

// connect makes a WebRTC peer and connects it through the control socket,
// as a relay would. cid "" is a legacy (single) peer.
func (d *testDaemon) connect(cid string, join bool) *testPeer {
	t := d.t
	t.Helper()
	lo := loopbackIface(t)
	api := rtc.NewAPI(func(se *webrtc.SettingEngine) {
		se.SetInterfaceFilter(func(n string) bool { return n == lo })
		se.SetIncludeLoopbackCandidate(true)
	})
	pc, err := api.NewPeerConnection(rtc.Config(nil, false))
	if err != nil {
		t.Fatal(err)
	}
	p := &testPeer{t: t, pc: pc, opened: make(chan struct{}), closed: make(chan struct{}), firstAck: make(chan struct{})}
	t.Cleanup(func() { _ = pc.Close() })
	var closeOnce sync.Once
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateClosed || s == webrtc.PeerConnectionStateFailed {
			closeOnce.Do(func() { close(p.closed) })
		}
	})
	dc, err := pc.CreateDataChannel("wosh", nil)
	if err != nil {
		t.Fatal(err)
	}
	p.dc = dc
	dc.OnOpen(func() { close(p.opened) })
	dc.OnClose(func() { closeOnce.Do(func() { close(p.closed) }) })
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		b := msg.Data
		if len(b) == 0 {
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		switch b[0] {
		case wire.FOutput:
			if _, data, ok := wire.ParseOffset(b); ok {
				p.out.Write(data)
			}
		case wire.FAck:
			// the session's input offset: a new controller continues from it
			if off, _, ok := wire.ParseOffset(b); ok && off > p.inTx {
				p.inTx = off
			}
			if !p.acked {
				p.acked = true
				close(p.firstAck)
			}
		case wire.FControl:
			if c, ok := wire.ParseControl(b); ok && c.Op == "role" {
				p.role = c.Role
			}
		}
	})
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = rtc.WaitGathered(ctx, pc)
	ans := d.ctl(wire.Msg{T: wire.TOffer, Key: d.key, SDP: pc.LocalDescription().SDP,
		CID: cid, Join: join, Term: "dumb", Cols: 80, Rows: 24})
	if ans.T != wire.TAnswer {
		t.Fatalf("offer: %+v", ans)
	}
	p.answer = ans.SDP
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: ans.SDP}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.firstAck: // sent right after open
	case <-time.After(20 * time.Second):
		t.Fatal("data channel did not open")
	}
	return p
}

// typ sends keystrokes (only the controller's reach the shell).
func (p *testPeer) typ(s string) {
	p.mu.Lock()
	off := p.inTx
	p.inTx += uint64(len(s))
	p.mu.Unlock()
	_ = p.dc.Send(wire.OffsetFrame(wire.FInput, off, []byte(s)))
}

func (p *testPeer) output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.String()
}

func (p *testPeer) currentRole() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.role
}

// waitOutput waits until the peer has received s.
func (p *testPeer) waitOutput(s string, wait time.Duration) {
	p.t.Helper()
	waitUntil(p.t, "output "+s, wait, func() bool { return strings.Contains(p.output(), s) })
}

// isClosed reports whether the peer connection ended within wait.
func (p *testPeer) isClosed(wait time.Duration) bool {
	select {
	case <-p.closed:
		return true
	case <-time.After(wait):
		return false
	}
}

func waitUntil(t *testing.T, what string, wait time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestHarness: a peer connects, controls the shell and sees its output.
func TestHarness(t *testing.T) {
	d := startDaemon(t)
	p := d.connect("c1", false)
	waitUntil(t, "control role", 10*time.Second, func() bool { return p.currentRole() == "control" })
	p.typ("echo harness-$((6*7))\r")
	p.waitOutput("harness-42", 10*time.Second)
}

// TestICEInterfaces: with --ice-iface <loopback> the daemon's answer offers
// loopback candidates only (none from the LAN or container bridges).
func TestICEInterfaces(t *testing.T) {
	d := startDaemon(t)
	p := d.connect("c1", false)
	n := 0
	for _, l := range strings.Split(p.answer, "\n") {
		if !strings.HasPrefix(l, "a=candidate:") {
			continue
		}
		n++
		f := strings.Fields(l) // foundation component proto prio addr port typ ...
		if len(f) < 8 {
			t.Fatalf("odd candidate %q", l)
		}
		if ip := net.ParseIP(f[4]); ip == nil || !ip.IsLoopback() {
			t.Errorf("non-loopback candidate %q", l)
		}
	}
	if n == 0 {
		t.Fatal("no candidates in the answer")
	}
	// the limit carries over to the session shell, for daemons started there
	waitUntil(t, "control role", 10*time.Second, func() bool { return p.currentRole() == "control" })
	p.typ("echo ifaces=$WOSH_ICE_IFACES.\r")
	p.waitOutput("ifaces="+loopbackIface(t)+".", 10*time.Second)
}
