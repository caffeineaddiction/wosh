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
// two; its keystrokes no longer reach the shell, and the others carry on.
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

// TestDropAll: an empty CID drops every peer, legacy ones too; the session
// keeps running and a new peer can attach.
func TestDropAll(t *testing.T) {
	d := startDaemon(t)
	a := d.connect("", false) // legacy peer (no CID)
	a.typ("X=kept\r")
	time.Sleep(200 * time.Millisecond)
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
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	conns := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			conns <- c
		}
	}()
	a := d.connect("aaaa", false)
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
	defer target.Close()
	buf := make([]byte, 2)
	_ = target.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(target, buf); err != nil || string(buf) != "hi" {
		t.Fatalf("forward: %q %v", buf, err)
	}

	d.ctl(wire.Msg{T: wire.TDrop, CID: "aaaa"})
	_ = target.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := target.Read(buf); err != io.EOF {
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

// TestKillBeforeStart: kill works on a daemon nobody connected to yet.
func TestKillBeforeStart(t *testing.T) {
	d := startDaemon(t)
	d.ctl(wire.Msg{T: wire.TKill})
	if !d.exited(10 * time.Second) {
		t.Fatal("daemon still running after kill")
	}
}
