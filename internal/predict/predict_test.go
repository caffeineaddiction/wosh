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

// out: server output only (keystrokes not yet processed).
func (h *harness) out(s string) {
	_, _ = h.real.Write(h.e.Output([]byte(s)))
	_, _ = h.server.Write([]byte(s))
}

// srv: server output, then its echo ack for all input typed so far.
func (h *harness) srv(s string) {
	if s != "" {
		h.out(s)
	}
	_, _ = h.real.Write(h.e.EchoAck(h.off))
}

func (h *harness) key(s string) {
	_, _ = h.real.Write(h.e.Input([]byte(s), h.off))
	h.off += uint64(len(s))
}

func row(v *vt.Emulator, y, cols int) string {
	var b strings.Builder
	for x := 0; x < cols; x++ {
		c := v.CellAt(x, y)
		if c == nil || (c.Content == "" && c.Width == 0) {
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

func (h *harness) realCursorX() int { return h.real.CursorPosition().X }

func (h *harness) idle() bool {
	h.e.mu.Lock()
	defer h.e.mu.Unlock()
	return len(h.e.overlay) == 0 && len(h.e.cursors) == 0
}

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

// confirm types one character and lets the server echo it, so the epoch is
// confirmed and later predictions are shown.
func (h *harness) confirm(ch, echo string) {
	h.t.Helper()
	h.key(ch)
	h.srv(echo)
	if h.e.confirmedEpoch < h.e.epoch {
		h.t.Fatalf("epoch not confirmed after echo %q", echo)
	}
}

func TestPredictsAfterConfirmation(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.key("l")
	if got := h.realRow(0); got != "$" {
		t.Fatalf("tentative epoch must not show: %q", got)
	}
	h.srv("l") // the echo confirms the epoch
	h.key("s -la")
	if got := h.realRow(0); got != "$ ls -la" {
		t.Fatalf("prediction not shown: %q", got)
	}
	if h.realCursorX() != 8 {
		t.Fatalf("predicted cursor at %d", h.realCursorX())
	}
	if c := h.real.CellAt(3, 0); c == nil || c.Style.Underline == 0 {
		t.Fatal("high latency predictions should be underlined")
	}
	h.out("s -l") // partial echo, not yet acked
	if got := h.realRow(0); got != "$ ls -la" {
		t.Fatalf("after partial echo: %q", got)
	}
	h.srv("a")
	h.sameAsServer()
	if !h.idle() {
		t.Fatal("predictions left over")
	}
}

func TestPasswordNeverShown(t *testing.T) {
	h := newHarness(t, 300*time.Millisecond)
	h.srv("Password: ")
	h.key("hunter2")
	if got := h.realRow(0); got != "Password:" {
		t.Fatalf("password leaked: %q", got)
	}
	h.srv("") // processed, nothing echoed
	h.sameAsServer()
}

func TestMispredictionWithdrawn(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("> ")
	h.confirm("a", "a")
	h.key("bc")
	h.srv("X") // the app drew something else
	h.sameAsServer()
	if h.e.confirmedEpoch >= h.e.epoch {
		t.Fatal("misprediction should restart tentatively")
	}
}

func TestLowLatencyAdaptiveHidesPredictions(t *testing.T) {
	h := newHarness(t, 5*time.Millisecond)
	h.srv("$ ")
	h.confirm("a", "a")
	h.key("b")
	if got := h.realRow(0); got != "$ a" {
		t.Fatalf("adaptive mode predicted on a fast link: %q", got)
	}
	h.srv("b")
	h.sameAsServer()
}

func TestBackspaceOwnPrediction(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("e", "e")
	h.key("cx")
	h.key("\x7f")
	if got := h.realRow(0); got != "$ ec" || h.realCursorX() != 4 {
		t.Fatalf("backspace: %q cursor %d", got, h.realCursorX())
	}
	h.srv("cx\b \b")
	h.sameAsServer()
}

// readline-style redraws: Backspace in the middle of a line shifts the rest
// left; the cursor follows.
func TestBackspaceMidLine(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ hello world\x1b[6D") // cursor after "hello"
	h.confirm("X", "X world\x1b[6D")
	h.key("\x7f\x7f") // remove "X" and "o"
	if got := h.realRow(0); got != "$ hell world" || h.realCursorX() != 6 {
		t.Fatalf("mid-line backspace: %q cursor %d", got, h.realCursorX())
	}
	h.srv("\b\b world  \x1b[8D")
	h.sameAsServer()
	if !h.idle() {
		t.Fatal("predictions left over")
	}
}

func TestDeleteKey(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ abcdef\x1b[4D") // cursor on 'c'
	h.confirm("Z", "Zcdef\x1b[4D")
	h.key("\x1b[3~") // delete 'c'
	if got := h.realRow(0); got != "$ abZdef" || h.realCursorX() != 5 {
		t.Fatalf("delete: %q cursor %d", got, h.realCursorX())
	}
	h.srv("def \x1b[4D")
	h.sameAsServer()
}

func TestCursorKeysThenInsert(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("a", "a")
	h.key("bc")
	h.key("\x1b[D\x1b[D") // left twice: cursor before 'b'
	if h.realCursorX() != 3 {
		t.Fatalf("left prediction: cursor %d", h.realCursorX())
	}
	h.key("X")
	if got := h.realRow(0); got != "$ aXbc" || h.realCursorX() != 4 {
		t.Fatalf("insert after left: %q cursor %d", got, h.realCursorX())
	}
	h.key("\x1b[C") // right
	if h.realCursorX() != 5 {
		t.Fatalf("right prediction: cursor %d", h.realCursorX())
	}
	h.srv("bc\b\bXbc\x1b[2D\x1b[C")
	h.sameAsServer()
	if !h.idle() {
		t.Fatal("predictions left over")
	}
}

func TestRightStopsAtEndOfText(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("a", "a")
	h.key("\x1b[C") // nothing to the right: cursor stays
	if h.realCursorX() != 3 {
		t.Fatalf("right at end of text moved the cursor to %d", h.realCursorX())
	}
	h.srv("")
	h.sameAsServer()
}

func TestNothingPredictedAfterEnterUntilProcessed(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("l", "l")
	h.key("s\rpwd") // typed ahead after Enter: the next prompt is unknown
	if got := h.realRow(0) + "|" + h.realRow(1); got != "$ ls|" {
		t.Fatalf("predicted past Enter: %q", got)
	}
	h.srv("s\r\nfile\r\n$ pwd")
	h.sameAsServer()
}

func TestVimInsertAndNormalMode(t *testing.T) {
	h := newHarnessSize(t, 200*time.Millisecond, 40, 8)
	h.srv("\x1b[?1049h\x1b[H\x1b[2Jline one\r\nhello world\r\n~\r\n~\x1b[2;7H")
	// normal mode: "x" deletes a char; vim redraws, nothing is echoed
	h.key("x")
	h.srv("\x1b[2;1Hhello orld\x1b[K\x1b[2;7H")
	if got := h.realRow(1); got != "hello orld" {
		t.Fatalf("normal-mode key shown as text: %q", got)
	}
	h.sameAsServer()
	h.key("i") // enters insert mode, not echoed
	h.srv("\x1b[8;1H-- INSERT --\x1b[2;7H")
	h.confirm("n", "norld\x1b[2;8H")
	h.key("ew w")
	if got := h.realRow(1); got != "hello new world" {
		t.Fatalf("vim insert prediction: %q", got)
	}
	h.key("\x1bOD") // vim's application-mode Left
	if h.realCursorX() != 10 {
		t.Fatalf("vim left arrow: cursor %d", h.realCursorX())
	}
	h.srv("ew world\x1b[2;12H\x1b[2;11H")
	h.sameAsServer()
	h.key("\x1b") // Esc: normal mode, new tentative epoch
	h.key("dd")
	h.srv("\x1b[2;1Hline three\x1b[K\x1b[2;1H")
	h.sameAsServer()
}

func TestWideCharacters(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("e", "e")
	h.key("cho 日本語 ok")
	if got := h.realRow(0); got != "$ echo 日本語 ok" {
		t.Fatalf("wide prediction: %q", got)
	}
	if h.realCursorX() != len("$ echo ")+6+3 {
		t.Fatalf("cursor after wide chars at %d", h.realCursorX())
	}
	h.out("cho 日本")
	h.srv("語 ok")
	h.sameAsServer()
	if !h.idle() || h.e.confirmedEpoch < h.e.epoch {
		t.Fatal("wide predictions not confirmed by the matching echo")
	}
}

func TestWideBackspace(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("a", "a")
	h.key("日本")
	h.key("\x7f")
	if got := h.realRow(0); got != "$ a日" || h.realCursorX() != 5 {
		t.Fatalf("wide backspace: %q cursor %d", got, h.realCursorX())
	}
	h.srv("日本\b\b  \b\b")
	h.sameAsServer()
}

func TestWrapAtRightMargin(t *testing.T) {
	h := newHarnessSize(t, 200*time.Millisecond, 20, 6)
	h.srv("$ ")
	h.confirm("a", "a")
	long := strings.Repeat("bcdefghij", 3) // runs past column 20
	h.key(long)
	if got := h.realRow(0) + "|" + h.realRow(1); got != "$ abcdefghijbcdefghi|jbcdefghij" {
		t.Fatalf("wrapped prediction: %q", got)
	}
	h.out(long[:10])
	h.srv(long[10:])
	h.sameAsServer()
}

func TestNoPredictionOnBottomRowWrap(t *testing.T) {
	h := newHarnessSize(t, 200*time.Millisecond, 10, 3)
	h.srv("\r\n\r\n$ ")
	h.confirm("a", "a")
	h.key("bcdefghijk") // would scroll the screen
	h.srv("bcdefghijk")
	h.sameAsServer()
}

func TestColoredCellsAndPenRestored(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("\x1b[32m$\x1b[0m \x1b[1;35m") // server leaves a bold magenta pen active
	h.confirm("a", "a")
	h.key("bc")
	h.out("b") // drawn with the server's pen after our undraw
	h.srv("c")
	h.sameAsServer()
	h.srv("\r\x1b[K\x1b[31mred\x1b[0m")
	h.sameAsServer()
}

// Shells like fish draw an autosuggestion after the cursor; it must not
// confirm keystrokes the server has not processed yet.
func TestAutosuggestionDoesNotConfirm(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("e", "e\x1b[90mcho hello\x1b[0m\x1b[9D") // echo + grey suggestion
	h.key("cho")                                       // not yet processed by the server
	if got := h.realRow(0); got != "$ echo hello" {
		t.Fatalf("display over suggestion: %q", got)
	}
	h.srv("cho\x1b[90m hello\x1b[0m\x1b[6D")
	h.sameAsServer()
	if !h.idle() {
		t.Fatal("echoed keys not settled")
	}
}

func TestPenAccess(t *testing.T) {
	e := New(Always, 20, 4)
	_, _ = e.vt.Write([]byte("\x1b[1;31m"))
	if p := e.pen(); p.IsZero() {
		t.Fatal("cannot read the emulator's pen (charmbracelet/x/vt internals changed?)")
	}
}

func TestHomeEndInShell(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("h", "h")
	h.key("ello world")
	h.key("\x1b[H") // Home: start of input, not column 0
	if h.realCursorX() != 2 {
		t.Fatalf("Home: cursor %d", h.realCursorX())
	}
	h.key("\x05") // Ctrl-E: end
	if h.realCursorX() != 13 {
		t.Fatalf("Ctrl-E: cursor %d", h.realCursorX())
	}
	h.key("\x01") // Ctrl-A
	h.key(">")
	if got := h.realRow(0); got != "$ >hello world" || h.realCursorX() != 3 {
		t.Fatalf("insert after Ctrl-A: %q cursor %d", got, h.realCursorX())
	}
	h.srv("ello world\x1b[11D>hello world\x1b[11D")
	h.sameAsServer()
	if !h.idle() {
		t.Fatal("predictions left over")
	}
}

func TestInputStartRelearnedAfterEnter(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("a", "a")
	h.key("\r")
	h.srv("\r\nlong-prompt> ")
	h.confirm("x", "x")
	h.key("yz\x1b[H")
	if h.realCursorX() != len("long-prompt> ") {
		t.Fatalf("Home on the new prompt: cursor %d", h.realCursorX())
	}
	h.srv("yz\x1b[3D")
	h.sameAsServer()
}

func TestWordMovesInShell(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("e", "e")
	h.key("cho foo-bar baz")
	h.key("\x1bb") // Alt-B: start of "baz"
	if h.realCursorX() != 15 {
		t.Fatalf("Alt-B: cursor %d", h.realCursorX())
	}
	h.key("\x1b[1;5D") // Ctrl-Left: start of "bar"
	if h.realCursorX() != 11 {
		t.Fatalf("Ctrl-Left: cursor %d", h.realCursorX())
	}
	h.key("\x1bf") // Alt-F: end of "bar"
	if h.realCursorX() != 14 {
		t.Fatalf("Alt-F: cursor %d", h.realCursorX())
	}
	h.srv("cho foo-bar baz\x1b[3D\x1b[4D\x1b[3C")
	h.sameAsServer()
	if !h.idle() {
		t.Fatal("predictions left over")
	}
}

func TestCtrlWAndAltBackspace(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("$ ")
	h.confirm("e", "e")
	h.key("cho foo-bar baz")
	h.key("\x17") // Ctrl-W: back to whitespace
	if got := h.realRow(0); got != "$ echo foo-bar" || h.realCursorX() != 15 {
		t.Fatalf("Ctrl-W: %q cursor %d", got, h.realCursorX())
	}
	h.key("\x7f\x1b\x7f") // Backspace (the space), Alt-Backspace: "bar"
	if got := h.realRow(0); got != "$ echo foo-" || h.realCursorX() != 11 {
		t.Fatalf("Alt-Backspace: %q cursor %d", got, h.realCursorX())
	}
	h.srv("cho foo-bar baz\b\b\b   \b\b\b\b \b\b\b\b   \b\b\b")
	h.sameAsServer()
}

func TestWordMovesInVimAndCtrlWNotPredicted(t *testing.T) {
	h := newHarnessSize(t, 200*time.Millisecond, 40, 8)
	h.srv("\x1b[?1049h\x1b[H\x1b[2Jfoo bar baz\r\n~\r\n~\x1b[1;1H")
	h.key("i")
	h.srv("\x1b[8;1H-- INSERT --\x1b[1;1H")
	h.confirm("X", "Xfoo bar baz\x1b[1;2H")
	h.key("\x1b[1;5C") // Ctrl-Right in vim: start of the next word
	if h.realCursorX() != 5 {
		t.Fatalf("vim Ctrl-Right: cursor %d", h.realCursorX())
	}
	h.key("\x1b[1;5D") // Ctrl-Left: start of previous word
	if h.realCursorX() != 0 {
		t.Fatalf("vim Ctrl-Left: cursor %d", h.realCursorX())
	}
	h.srv("\x1b[1;6H\x1b[1;1H")
	h.sameAsServer()
	h.key("\x1b[1;5C") // back to column 5, confirmed by the server
	h.srv("\x1b[1;6H")
	h.key("\x17") // Ctrl-W in a full-screen app (nano: search): not predicted
	if h.realCursorX() != 5 || h.realRow(0) != "Xfoo bar baz" {
		t.Fatalf("Ctrl-W predicted in vim: %q cursor %d", h.realRow(0), h.realCursorX())
	}
	h.srv("")
	h.sameAsServer()
}

func TestUpDownInVim(t *testing.T) {
	h := newHarnessSize(t, 200*time.Millisecond, 40, 8)
	// title bar, three lines (middle one short), then vim's ~ rows
	h.srv("\x1b[?1049h\x1b[H\x1b[2Jtitle\r\nfirst line here\r\nab\r\nthird line here\r\n~\r\n~\x1b[2;9H")
	h.key("\x1b[B") // Down: first prediction is tentative
	if h.real.CursorPosition().Y != 1 {
		t.Fatal("tentative cursor move shown")
	}
	h.srv("\x1b[3;2H") // vim normal mode: short line, cursor on its last char
	h.key("\x1b[B")    // Down again: back to the wanted column 8
	if c := h.real.CursorPosition(); c.Y != 3 || c.X != 8 {
		t.Fatalf("Down with wanted column: %v", c)
	}
	h.key("\x1bOA") // application-mode Up: short line again
	if c := h.real.CursorPosition(); c.Y != 2 || c.X != 1 {
		t.Fatalf("Up onto short line: %v", c)
	}
	h.srv("\x1b[4;9H\x1b[3;2H")
	h.sameAsServer()
	h.key("\x1b[B\x1b[B") // second Down would land on a ~ row: not predicted
	if c := h.real.CursorPosition(); c.Y != 3 {
		t.Fatalf("predicted onto a ~ row: %v", c)
	}
	h.srv("\x1b[4;9H")
	h.sameAsServer()
}

func TestUpInShellIsHistory(t *testing.T) {
	h := newHarness(t, 200*time.Millisecond)
	h.srv("first output\r\nsome output here\r\n$ ")
	h.confirm("a", "a")
	h.key("\x1b[A") // history: unpredictable
	if c := h.real.CursorPosition(); c.Y != 2 || c.X != 3 {
		t.Fatalf("Up predicted in a shell: cursor %v", c)
	}
	h.srv("\r\x1b[K$ previous command")
	h.sameAsServer()
}

// At the bottom of the screen Enter scrolls: the new prompt is on the same
// row index, so the input start must come from the fresh-prompt signal.
func TestInputStartRelearnedOnSameRow(t *testing.T) {
	h := newHarnessSize(t, 200*time.Millisecond, 40, 3)
	h.srv("\r\n\r\n$ ")
	h.confirm("a", "a")
	h.key("\r")
	h.srv("\r\nlong-prompt> ") // scrolls; prompt again on row 2
	h.confirm("x", "x")
	h.key("yz\x1b[H")
	if h.realCursorX() != len("long-prompt> ") {
		t.Fatalf("Home after a scrolled prompt: cursor %d", h.realCursorX())
	}
	h.srv("yz\x1b[3D")
	h.sameAsServer()
}

// vim normal mode: the cursor stays on the last character of a short line;
// the first line of the file is row 0 (no title bar).
func TestUpDownVimNormalModeClamp(t *testing.T) {
	h := newHarnessSize(t, 200*time.Millisecond, 40, 8)
	h.srv("\x1b[?1049h\x1b[H\x1b[2Jline one here\r\nab\r\nline three here\r\n~\r\n~\x1b[1;9H")
	h.key("\x1b[B")
	h.srv("\x1b[2;2H") // normal mode: on the 'b'
	h.key("\x1b[A")    // back to row 0, wanted column 8
	if c := h.real.CursorPosition(); c.Y != 0 || c.X != 8 {
		t.Fatalf("Up to the first line: %v", c)
	}
	h.key("\x1b[B") // short line again: clamped onto the last character
	if c := h.real.CursorPosition(); c.Y != 1 || c.X != 1 {
		t.Fatalf("normal-mode clamp: %v", c)
	}
	h.srv("\x1b[1;9H\x1b[2;2H")
	h.sameAsServer()
	if h.e.cursorEpoch < h.e.epoch {
		t.Fatal("correct cursor predictions should keep the epoch confirmed")
	}
}
