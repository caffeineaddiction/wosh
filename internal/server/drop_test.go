package server

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"wosh/internal/stream"
	"wosh/internal/wire"
)

// TestDropByCID: drop closes only that client's peer, within a second or
// two. The other peers carry on.
func TestDropByCID(t *testing.T) {
	d := startDaemon(t)
	a := d.connect("aaaa", false)
	waitUntil(t, "a controls", 10*time.Second, func() bool { return a.currentRole() == "control" })
	b := d.connect("bbbb", true)
	waitUntil(t, "b views", 10*time.Second, func() bool { return b.currentRole() == "view" })

	if r := d.ctl(wire.Msg{T: wire.TDrop, CID: "bbbb"}); r.T != wire.TOK {
		t.Fatalf("drop: %+v", r)
	}
	if !b.isClosed(5 * time.Second) {
		t.Fatal("dropped viewer still connected")
	}
	if a.isClosed(500 * time.Millisecond) {
		t.Fatal("drop closed the wrong peer")
	}
	a.typ("echo still-$((1+1))\r")
	a.waitOutput("still-2", 10*time.Second)

	// drop the controller: its input must not reach the shell any more
	d.ctl(wire.Msg{T: wire.TDrop, CID: "aaaa"})
	if !a.isClosed(5 * time.Second) {
		t.Fatal("dropped controller still connected")
	}
	a.typ("echo after-drop-$((2+3))\r")
	c := d.connect("cccc", false)
	c.typ("echo probe-$((3+4))\r")
	c.waitOutput("probe-7", 10*time.Second)
	if strings.Contains(c.output(), "after-drop-5") {
		t.Fatal("input from a dropped peer reached the shell")
	}
}

// TestDropAll: an empty CID drops every peer, legacy ones too. The session
// keeps running and a new peer can attach.
func TestDropAll(t *testing.T) {
	d := startDaemon(t)
	a := d.connect("", false) // legacy peer (no CID)
	a.typ("X=kept; echo set-$((1+1))\r")
	a.waitOutput("set-2", 10*time.Second)
	d.ctl(wire.Msg{T: wire.TDrop})
	if !a.isClosed(5 * time.Second) {
		t.Fatal("legacy peer still connected")
	}
	b := d.connect("bbbb", false)
	b.typ("echo X=$X-$((1+1))\r")
	b.waitOutput("X=kept-2", 10*time.Second)
}

// TestDropEndsStreams: a dropped peer's forwarded connection is closed on
// the daemon side at once, not kept for resumption.
func TestDropEndsStreams(t *testing.T) {
	d := startDaemon(t)
	target := forwardTarget(t, d.connect("aaaa", false))
	d.ctl(wire.Msg{T: wire.TDrop, CID: "aaaa"})
	expectEOF(t, target)
}

// TestDropEndsStreamsAfterRoam: the client reconnected and has not resumed
// its forward on the new link yet. Drop still ends it at once.
func TestDropEndsStreamsAfterRoam(t *testing.T) {
	d := startDaemon(t)
	target := forwardTarget(t, d.connect("aaaa", false))
	d.connect("aaaa", false) // same client again: the daemon closes the old link
	d.ctl(wire.Msg{T: wire.TDrop, CID: "aaaa"})
	expectEOF(t, target)
}

// forwardTarget opens a -L style stream from peer a to a local listener and
// returns the connection the daemon dialed.
func forwardTarget(t *testing.T, a *testPeer) net.Conn {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	conns := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			conns <- c
		}
	}()
	s, err := stream.OpenResumable(a.pc, wire.StreamSpec{K: "tcp", Host: "127.0.0.1", Port: l.Addr().(*net.TCPAddr).Port})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(stream.Data, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	var target net.Conn
	select {
	case target = <-conns:
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not dial the forward target")
	}
	t.Cleanup(func() { _ = target.Close() })
	buf := make([]byte, 2)
	_ = target.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(target, buf); err != nil || string(buf) != "hi" {
		t.Fatalf("forward: %q %v", buf, err)
	}
	return target
}

func expectEOF(t *testing.T, c net.Conn) {
	t.Helper()
	buf := make([]byte, 1)
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Read(buf); err != io.EOF {
		t.Fatalf("forwarded connection not closed after drop: %v", err)
	}
}

// TestKillOp: kill hangs up the shell and the daemon exits.
func TestKillOp(t *testing.T) {
	d := startDaemon(t)
	a := d.connect("aaaa", false)
	a.typ("echo up\r")
	a.waitOutput("up", 10*time.Second)
	if r := d.ctl(wire.Msg{T: wire.TKill}); r.T != wire.TOK {
		t.Fatalf("kill: %+v", r)
	}
	if !d.exited(10 * time.Second) {
		t.Fatal("daemon still running after kill")
	}
}

// TestKillBeforeStart: kill works on a daemon nobody connected to yet, and
// an offer that comes before it exits starts no shell.
func TestKillBeforeStart(t *testing.T) {
	d := startDaemon(t)
	d.ctl(wire.Msg{T: wire.TKill})
	if r := d.ctl(wire.Msg{T: wire.TOffer, Key: d.key, SDP: "v=0"}); r.T != wire.TError || !strings.Contains(r.Err, "killed") {
		t.Fatalf("offer after kill: %+v", r)
	}
	if r := d.ctl(wire.Msg{T: wire.TStart, Key: d.key}); r.T != wire.TError || !strings.Contains(r.Err, "killed") {
		t.Fatalf("start after kill: %+v", r)
	}
	if st := d.status(); st != nil && st.Started {
		t.Fatalf("a shell started after kill: %+v", st)
	}
	if !d.exited(10 * time.Second) {
		t.Fatal("daemon still running after kill")
	}
}
