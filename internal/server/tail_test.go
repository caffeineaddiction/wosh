package server

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"wosh/internal/wire"
)

func TestRingTail(t *testing.T) {
	r := newRing(10)
	if b, off := r.tail(5); len(b) != 0 || off != 0 {
		t.Fatalf("empty: %v %d", b, off)
	}
	for i := 0; i < 50; i++ {
		r.append([]byte{byte(i)})
	}
	b, off := r.tail(3)
	if !bytes.Equal(b, []byte{47, 48, 49}) || off != 47 {
		t.Fatalf("tail 3: %v %d", b, off)
	}
	b, off = r.tail(1000) // clamped to what is retained
	if off != r.start || len(b) != int(r.end()-r.start) || b[len(b)-1] != 49 {
		t.Fatalf("tail all: len %d off %d", len(b), off)
	}
	b[0] = 99 // a copy: the ring is untouched
	if r.data[0] == 99 {
		t.Fatal("tail aliases the ring")
	}
}

// TestTailOp: tail returns the recent output, with its offset, before and
// after a peer connects.
func TestTailOp(t *testing.T) {
	d := startDaemon(t)
	if r := d.ctl(wire.Msg{T: wire.TTail}); r.T != wire.TOK || len(r.Data) != 0 {
		t.Fatalf("tail before start: %+v", r)
	}
	a := d.connect("aaaa", false)
	a.typ("echo tail-$((20+3)); printf 'z%.0s' $(seq 2000); echo; echo last-$((1+1))\r")
	a.waitOutput("last-2", 10*time.Second)

	r := d.ctl(wire.Msg{T: wire.TTail, N: 100})
	if r.T != wire.TOK || len(r.Data) != 100 {
		t.Fatalf("tail 100: %s %d bytes", r.T, len(r.Data))
	}
	if !strings.Contains(string(r.Data), "last-2") {
		t.Fatalf("tail misses the newest output: %q", r.Data)
	}
	out := a.output()
	if want := uint64(len(out)) - 100; r.Off < want-200 || r.Off > want+200 {
		// the shell may print a prompt between the peer's read and ours
		t.Fatalf("offset %d, peer saw %d bytes", r.Off, len(out))
	}

	all := d.ctl(wire.Msg{T: wire.TTail})
	if all.Off != 0 || !strings.Contains(string(all.Data), "tail-23") || !strings.Contains(string(all.Data), "last-2") {
		t.Fatalf("default tail: off %d, %d bytes", all.Off, len(all.Data))
	}
}
