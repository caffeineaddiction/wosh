package predict

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/vt"
)

func newVT(cols, rows int) *vt.Emulator {
	v := vt.NewEmulator(cols, rows)
	go func() { _, _ = io.Copy(io.Discard, v) }()
	return v
}

// harness: "real" is the user's terminal (everything the engine emits),
// "server" is what the remote side drew (output only).
type harness struct {
	t            *testing.T
	e            *Engine
	real, server *vt.Emulator
	cols, rows   int
	off          uint64
}

func newHarness(t *testing.T, srtt time.Duration) *harness { return newHarnessSize(t, srtt, 40, 6) }

func newHarnessSize(t *testing.T, srtt time.Duration, cols, rows int) *harness {
	h := &harness{t: t, e: New(Adaptive, cols, rows), real: newVT(cols, rows), server: newVT(cols, rows), cols: cols, rows: rows}
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

// expire makes every outstanding prediction overdue and re-evaluates.
func (h *harness) expire() {
	h.e.mu.Lock()
	for i := range h.e.preds {
		h.e.preds[i].ackedAt = time.Now().Add(-time.Hour)
	}
	h.e.mu.Unlock()
	_, _ = h.real.Write(h.e.Tick())
}

func row(v *vt.Emulator, y, cols int) string {
	var b strings.Builder
	for x := 0; x < cols; x++ {
		c := v.CellAt(x, y)
		if c == nil {
			continue
		}
		if c.Content == "" && c.Width == 0 {
			continue
		}
		if c.Content == "" {
			b.WriteByte(' ')
		} else {
			b.WriteString(c.Content)
		}
	}
	return strings.TrimRight(b.String(), " ")
}

func (h *harness) realRow(y int) string { return row(h.real, y, h.cols) }

func (h *harness) sameAsServer() {
	h.t.Helper()
	for y := 0; y < h.rows; y++ {
		if a, b := row(h.real, y, h.cols), row(h.server, y, h.cols); a != b {
			h.t.Fatalf("row %d: real %q != server %q", y, a, b)
		}
		for x := 0; x < h.cols; x++ {
			rc, sc := h.real.CellAt(x, y), h.server.CellAt(x, y)
			if rc != nil && sc != nil && !rc.Style.Equal(&sc.Style) && rc.Content != " " {
				h.t.Fatalf("style differs at %d,%d (%q)", x, y, rc.Content)
			}
		}
	}
	if h.real.CursorPosition() != h.server.CursorPosition() {
		h.t.Fatalf("cursor real %v server %v", h.real.CursorPosition(), h.server.CursorPosition())
	}
}

func TestPredictsAfterConfirmation(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.out("$ ")
	h.key("l")
	if got := h.realRow(0); got != "$" {
		t.Fatalf("tentative epoch must not show: %q", got)
	}
	h.ack()
	h.out("l") // server echo confirms the epoch
	h.key("s -la")
	if got := h.realRow(0); got != "$ ls -la" {
		t.Fatalf("prediction not shown: %q", got)
	}
	if c := h.real.CellAt(3, 0); c == nil || c.Style.Underline == 0 {
		t.Fatal("high latency predictions should be underlined")
	}
	h.ack()
	h.out("s -l") // partial echo
	if got := h.realRow(0); got != "$ ls -la" {
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
	if got := h.realRow(0); got != "Password:" {
		t.Fatalf("password leaked: %q", got)
	}
	h.expire()
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
	h.expire()
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
	if got := h.realRow(0); got != "$ a" {
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
	if got := h.realRow(0); got != "$ ec" {
		t.Fatalf("backspace: %q", got)
	}
	h.key("\r") // new epoch
	h.ack()
	h.out("c\r\nnext$ ")
	h.sameAsServer()
	h.key("z")
	if got := h.realRow(1); got != "next$" {
		t.Fatalf("new line must start tentative: %q", got)
	}
}

func TestColoredCellsAndPenRestored(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.out("\x1b[32m$\x1b[0m \x1b[1;35m") // server leaves a bold magenta pen active
	h.key("a")
	h.ack()
	h.out("a")
	h.key("bc")
	h.ack()
	h.out("b") // drawn with the server's pen after our undraw
	h.out("c")
	h.sameAsServer()
	h.out("\r\x1b[K\x1b[31mred\x1b[0m")
	h.sameAsServer()
}

// Typing in the middle of a line shifts the rest right (shells, vim/nano
// insert mode) instead of hiding it.
func TestInsertMidLine(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.out("$ echo world\x1b[5D") // cursor before "world"
	h.key("b")
	h.ack()
	h.out("bworld\x1b[5D") // shell inserts and redraws the tail
	h.key("ig ")
	if got := h.realRow(0); got != "$ echo big world" {
		t.Fatalf("insert prediction: %q", got)
	}
	h.ack()
	h.out("ig world\x1b[5D")
	h.sameAsServer()
}

func TestVimInsertAndNormalMode(t *testing.T) {
	h := newHarnessSize(t, 200*time.Millisecond, 40, 8)
	// full-screen editor: alt screen, text, cursor parked at row 2 col 6
	h.out("\x1b[?1049h\x1b[H\x1b[2Jline one\r\nhello world\r\n~\r\n~\x1b[2;7H")
	// normal mode: "x" deletes a char; vim redraws, nothing is echoed
	h.key("x")
	h.ack()
	h.out("\x1b[2;1Hhello orld\x1b[K\x1b[2;7H")
	h.expire()
	if got := h.realRow(1); got != "hello orld" {
		t.Fatalf("normal-mode key shown as text: %q", got)
	}
	h.sameAsServer()
	// insert mode ("i" is consumed by vim; then typing inserts)
	h.key("i")
	h.ack()
	h.out("\x1b[8;1H-- INSERT --\x1b[2;7H")
	h.expire()
	h.key("n")
	h.ack()
	h.out("norld\x1b[2;8H") // vim redraws from the cursor
	h.key("ew w")
	if got := h.realRow(1); got != "hello new world" {
		t.Fatalf("vim insert prediction: %q", got)
	}
	h.ack()
	h.out("ew world\x1b[2;12H")
	h.sameAsServer()
	h.key("\x1b") // Esc: back to normal mode, new tentative epoch
	h.key("dd")
	h.ack()
	h.out("\x1b[2;1Hline three\x1b[K\x1b[2;1H")
	h.expire()
	h.sameAsServer()
}

func TestWideCharacters(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.out("$ ")
	h.key("e")
	h.ack()
	h.out("e")
	h.key("cho 日本語 ok")
	if got := h.realRow(0); got != "$ echo 日本語 ok" {
		t.Fatalf("wide prediction: %q", got)
	}
	if h.real.CursorPosition().X != len("$ echo ")+6+3 {
		t.Fatalf("cursor after wide chars at %v", h.real.CursorPosition())
	}
	h.ack()
	h.out("cho 日本")
	h.out("語 ok")
	h.sameAsServer()
	if len(h.e.preds) != 0 || !h.e.confirmed {
		t.Fatalf("wide predictions not confirmed by the matching echo: %d left", len(h.e.preds))
	}
}

func TestWrapAtRightMargin(t *testing.T) {
	h := newHarnessSize(t, 200*time.Millisecond, 20, 6)
	h.out("$ ")
	h.key("a")
	h.ack()
	h.out("a")
	long := strings.Repeat("bcdefghij", 3) // runs past column 20
	h.key(long)
	if got := h.realRow(0) + "|" + h.realRow(1); got != "$ abcdefghijbcdefghi|jbcdefghij" {
		t.Fatalf("wrapped prediction: %q", got)
	}
	h.ack()
	h.out(long[:10])
	h.out(long[10:])
	h.sameAsServer()
}

func TestNoPredictionOnBottomRowWrap(t *testing.T) {
	h := newHarnessSize(t, 200*time.Millisecond, 10, 3)
	h.out("\r\n\r\n$ ")
	h.key("a")
	h.ack()
	h.out("a")
	h.key("bcdefghijk") // would scroll the screen
	h.ack()
	h.out("bcdefghijk")
	h.sameAsServer()
}

func TestPenAccess(t *testing.T) {
	e := New(Always, 20, 4)
	_, _ = e.vt.Write([]byte("\x1b[1;31m"))
	p := e.pen()
	if p.IsZero() {
		t.Fatal("cannot read the emulator's pen (charmbracelet/x/vt internals changed?)")
	}
}

// Shells like fish draw an autosuggestion after the cursor; it must not
// confirm keystrokes the server has not processed yet.
func TestAutosuggestionDoesNotConfirm(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.out("$ ")
	h.key("e")
	h.ack()
	h.out("e\x1b[90mcho hello\x1b[0m\x1b[9D") // echo + grey suggestion
	h.key("cho")                              // not yet received by the server
	h.e.mu.Lock()
	pending := len(h.e.preds)
	h.e.mu.Unlock()
	if pending != 3 {
		t.Fatalf("suggestion confirmed unprocessed keys: %d predictions left", pending)
	}
	if got := h.realRow(0); got != "$ echo hello" {
		t.Fatalf("display: %q", got)
	}
	h.ack()
	h.out("cho\x1b[90m hello\x1b[0m\x1b[6D")
	h.sameAsServer()
	if len(h.e.preds) != 0 {
		t.Fatal("echoed keys not confirmed")
	}
}
