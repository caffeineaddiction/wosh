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
	a.waitOutput("before-42", 10*time.Second)
	time.Sleep(300 * time.Millisecond) // let the next prompt arrive too
	seen := uint64(len(a.output()))

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
	if !strings.Contains(b.output()[:infoOut], "before-42") {
		t.Fatal("replayed output is not below info out")
	}

	a.typ("echo after-$((3*3))\r")
	b.waitOutput("after-9", 10*time.Second)
	if strings.Contains(b.output()[:infoOut], "after-9") {
		t.Fatal("new output landed below info out")
	}
}
