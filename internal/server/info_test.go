package server

import (
	"strings"
	"testing"
	"time"
)

// TestInfoOutputEnd: a peer learns where replay ends (info "out") before any
// output arrives, so it can tell old terminal queries from new ones.
func TestInfoOutputEnd(t *testing.T) {
	d := startDaemon(t)
	a := d.connect("aaaa", false)
	a.typ("echo before-$((2*21))\r")
	// the prompt after the output: the shell is idle, so the output stops here
	waitUntil(t, "prompt after before-42", 10*time.Second, func() bool {
		o := a.output()
		i := strings.Index(o, "before-42")
		return i >= 0 && strings.HasSuffix(o[i:], "$ ")
	})
	seen := uint64(len(a.output()))
	a.mu.Lock()
	aOut, aSeen := a.infoOut, a.infoSeen
	a.mu.Unlock()
	// the shell started before a connected, but nobody answered its startup
	// queries: none of it is replay
	if !aSeen || aOut != 0 {
		t.Fatalf("first peer: info seen %v, out %d, want 0", aSeen, aOut)
	}

	b := d.connect("bbbb", true) // replays the ring from offset 0
	b.waitOutput("before-42", 10*time.Second)
	b.mu.Lock()
	infoOut, infoSeen, early := b.infoOut, b.infoSeen, b.outBeforeInfo
	b.mu.Unlock()
	if !infoSeen || early {
		t.Fatalf("info seen %v, output before info %v", infoSeen, early)
	}
	if infoOut != seen {
		t.Fatalf("info out %d, session output was %d", infoOut, seen)
	}
	waitUntil(t, "replay up to info out", 10*time.Second, func() bool { return uint64(len(b.output())) >= infoOut })
	if !strings.Contains(b.output()[:infoOut], "before-42") {
		t.Fatal("replayed output is not below info out")
	}

	a.typ("echo after-$((3*3))\r")
	b.waitOutput("after-9", 10*time.Second)
	if strings.Contains(b.output()[:infoOut], "after-9") {
		t.Fatal("new output landed below info out")
	}
}
