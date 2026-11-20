package server

import (
	"testing"
	"time"

	"wosh/internal/wire"
)

// TestStartHeadless: start runs the shell with no peer, type reaches it, and
// a later peer (with the start key) sees the output in the replay.
func TestStartHeadless(t *testing.T) {
	d := startDaemon(t)
	if r := d.ctl(wire.Msg{T: wire.TType, Text: "x"}); r.T != wire.TError {
		t.Fatalf("type before start: %+v", r)
	}
	if r := d.ctl(wire.Msg{T: wire.TStart, Key: d.key, Term: "dumb", Cols: 100, Rows: 30,
		Env: map[string]string{"WB_TEST": "env-ok"}}); r.T != wire.TOK {
		t.Fatalf("start: %+v", r)
	}
	if r := d.ctl(wire.Msg{T: wire.TStart, Key: d.key}); r.T != wire.TOK {
		t.Fatalf("second start: %+v", r)
	}
	if r := d.ctl(wire.Msg{T: wire.TStart, Key: "another-key-0123456789"}); r.T != wire.TError {
		t.Fatalf("start with a different key: %+v", r)
	}
	if r := d.ctl(wire.Msg{T: wire.TType, Text: "echo typed-$((4*4)) $WB_TEST $COLUMNS\r"}); r.T != wire.TOK {
		t.Fatalf("type: %+v", r)
	}
	p := d.connect("aaaa", false)
	p.waitOutput("typed-16 env-ok", 10*time.Second)
}

// TestStartKeyRequired: a key set by start is checked on offers.
func TestStartKeyRequired(t *testing.T) {
	d := startDaemon(t)
	d.ctl(wire.Msg{T: wire.TStart, Key: "start-key-0123456789"})
	if r := d.ctl(wire.Msg{T: wire.TOffer, Key: d.key, SDP: "x"}); r.T != wire.TError || r.Err != "bad session key" {
		t.Fatalf("offer with another key: %+v", r)
	}
}

// TestStatus: state and peers with their roles.
func TestStatus(t *testing.T) {
	d := startDaemon(t)
	st := d.ctl(wire.Msg{T: wire.TStatus}).Status
	if st == nil || st.Started || st.EverOpen || len(st.Peers) != 0 {
		t.Fatalf("fresh: %+v", st)
	}
	a := d.connect("aaaa", false)
	waitUntil(t, "a controls", 10*time.Second, func() bool { return a.currentRole() == "control" })
	b := d.connect("bbbb", true)
	waitUntil(t, "b views", 10*time.Second, func() bool { return b.currentRole() == "view" })
	st = d.ctl(wire.Msg{T: wire.TStatus}).Status
	roles := map[string]string{}
	for _, p := range st.Peers {
		roles[p.CID] = p.Role
	}
	if !st.Started || !st.EverOpen || st.Exited || roles["aaaa"] != "control" || roles["bbbb"] != "view" {
		t.Fatalf("two peers: %+v", st)
	}
	a.typ("exit 3\r")
	waitUntil(t, "exited", 10*time.Second, func() bool {
		s := d.ctl(wire.Msg{T: wire.TStatus}).Status
		return s != nil && s.Exited && s.ExitCode == 3
	})
}
