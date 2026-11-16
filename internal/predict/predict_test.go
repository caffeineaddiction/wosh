package predict

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/hinshun/vt10x"
)

// harness: "real" is the user's terminal (everything the engine emits),
// "server" is what the remote side drew (output only).
type harness struct {
	t      *testing.T
	e      *Engine
	real   vt10x.Terminal
	server vt10x.Terminal
	off    uint64
}

func newHarness(t *testing.T, srtt time.Duration) *harness {
	h := &harness{t: t, e: New(Adaptive, 40, 5),
		real:   vt10x.New(vt10x.WithSize(40, 5), vt10x.WithWriter(io.Discard)),
		server: vt10x.New(vt10x.WithSize(40, 5), vt10x.WithWriter(io.Discard))}
	h.e.SetRTT(srtt)
	return h
}

func (h *harness) out(s string) {
	_, _ = h.real.Write(h.e.Output([]byte(s)))
	_, _ = h.server.Write([]byte(s))
}

func (h *harness) key(s string) {
	_, _ = h.real.Write(h.e.Input([]byte(s), h.off))
	h.off += uint64(len(s))
}

func (h *harness) ack() { h.e.Acked(h.off) }

func line(v vt10x.Terminal, y int) string {
	var b strings.Builder
	cols, _ := v.Size()
	for x := 0; x < cols; x++ {
		c := v.Cell(x, y).Char
		if c == 0 {
			c = ' '
		}
		b.WriteRune(c)
	}
	return strings.TrimRight(b.String(), " ")
}

func (h *harness) realLine() string { return line(h.real, h.real.Cursor().Y) }

func (h *harness) sameAsServer() {
	h.t.Helper()
	for y := 0; y < 5; y++ {
		if a, b := line(h.real, y), line(h.server, y); a != b {
			h.t.Fatalf("row %d: real %q != server %q", y, a, b)
		}
	}
	if h.real.Cursor().X != h.server.Cursor().X || h.real.Cursor().Y != h.server.Cursor().Y {
		h.t.Fatalf("cursor real %+v server %+v", h.real.Cursor(), h.server.Cursor())
	}
}

func TestPredictsAfterConfirmation(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.out("$ ")
	h.key("l")
	if got := h.realLine(); got != "$" {
		t.Fatalf("tentative epoch must not show: %q", got)
	}
	h.ack()
	h.out("l") // server echo confirms the epoch
	h.key("s -la")
	if got := h.realLine(); got != "$ ls -la" {
		t.Fatalf("prediction not shown: %q", got)
	}
	if h.real.Cell(3, 0).Mode&attrUnderline == 0 {
		t.Fatal("high latency predictions should be underlined")
	}
	h.ack()
	h.out("s -l") // partial echo
	if got := h.realLine(); got != "$ ls -la" {
		t.Fatalf("after partial echo: %q", got)
	}
	h.out("a")
	h.sameAsServer()
	if len(h.e.preds) != 0 {
		t.Fatal("predictions left over")
	}
}

func TestPasswordNeverShown(t *testing.T) {
	h := newHarness(t, 300*time.Millisecond)
	h.out("Password: ")
	h.key("hunter2")
	h.ack()
	if got := h.realLine(); got != "Password:" {
		t.Fatalf("password leaked: %q", got)
	}
	time.Sleep(10 * time.Millisecond)
	_, _ = h.real.Write(h.e.Tick())
	h.sameAsServer()
}

func TestMispredictionWithdrawn(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.out("> ")
	h.key("a")
	h.ack()
	h.out("a")
	h.key("bc")
	h.ack()
	h.out("X") // server drew something else at the predicted cell
	h.e.mu.Lock()
	for i := range h.e.preds {
		h.e.preds[i].ackedAt = time.Now().Add(-time.Hour)
	}
	h.e.mu.Unlock()
	_, _ = h.real.Write(h.e.Tick())
	h.sameAsServer()
	if h.e.confirmed {
		t.Fatal("misprediction should restart tentatively")
	}
}

func TestLowLatencyAdaptiveHidesPredictions(t *testing.T) {
	h := newHarness(t, 5*time.Millisecond)
	h.out("$ ")
	h.key("a")
	h.ack()
	h.out("a")
	h.key("b")
	if got := h.realLine(); got != "$ a" {
		t.Fatalf("adaptive mode predicted on a fast link: %q", got)
	}
	h.out("b")
	h.sameAsServer()
}

func TestBackspaceAndEnter(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.out("$ ")
	h.key("e")
	h.ack()
	h.out("e")
	h.key("cx")
	h.key("\x7f") // take back 'x'
	if got := h.realLine(); got != "$ ec" {
		t.Fatalf("backspace: %q", got)
	}
	h.key("\r") // new epoch
	h.ack()
	h.out("c\r\nnext$ ")
	h.sameAsServer()
	h.key("z")
	if got := h.realLine(); got != "next$" {
		t.Fatalf("new line must start tentative: %q", got)
	}
}

func TestColoredCellsRestored(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.out("\x1b[32m$\x1b[0m ")
	h.key("a")
	h.ack()
	h.out("a")
	h.key("b")
	h.ack()
	h.out("\r\x1b[K\x1b[31mred\x1b[0m") // redraw elsewhere: prediction cell must be restored
	h.e.mu.Lock()
	for i := range h.e.preds {
		h.e.preds[i].ackedAt = time.Now().Add(-time.Hour)
	}
	h.e.mu.Unlock()
	_, _ = h.real.Write(h.e.Tick())
	h.sameAsServer()
}
