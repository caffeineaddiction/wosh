// Package predict implements mosh-style speculative local echo.
//
// A terminal emulator mirrors what the server has drawn, so we know the
// cursor position and the contents of every cell. When the user types a
// printable character we draw it at the predicted position right away
// (underlined when latency is high) instead of waiting a round trip for
// the remote echo. When server output arrives, predictions are first
// erased (cells restored from the mirror), the output is passed through
// untouched, and predictions that are still outstanding are drawn again.
// A prediction is confirmed when the mirrored cell shows the predicted
// character; if the server acknowledged the keystroke but the cell does
// not match within a grace period, it was a misprediction: everything is
// withdrawn and prediction restarts tentatively.
//
// Like mosh, each "epoch" (a stretch of typing on one line) starts
// tentative: nothing is shown until one of its predictions is confirmed by
// the server. That keeps passwords (no echo) and full-screen apps that do
// not echo from ever showing predicted text.
//
// Unlike mosh, the real terminal still gets the raw output stream (so
// scrollback works); predictions are drawn with relative cursor movement
// only, so the mirror needs no knowledge of absolute screen position.
package predict

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hinshun/vt10x"
)

// Mode selects when predictions are displayed.
type Mode int

const (
	Adaptive Mode = iota // when the link is slow (srtt > ShowAbove) or down
	Always
	Never
)

// ParseMode parses --predict values.
func ParseMode(s string) (Mode, error) {
	switch s {
	case "", "adaptive":
		return Adaptive, nil
	case "always":
		return Always, nil
	case "never":
		return Never, nil
	}
	return Adaptive, fmt.Errorf("--predict wants adaptive, always or never")
}

var (
	ShowAbove      = 30 * time.Millisecond // adaptive: predict above this srtt
	UnderlineAbove = 80 * time.Millisecond // flag predictions above this srtt
)

type pred struct {
	col     int
	r       rune
	off     uint64 // input stream offset just past this keystroke
	ackedAt time.Time
}

// Engine is the prediction state for the session on screen.
type Engine struct {
	mu   sync.Mutex
	mode Mode
	vt   vt10x.Terminal
	cols int

	srtt      time.Duration
	connected bool

	preds     []pred
	row       int  // row of the current epoch
	confirmed bool // epoch has had a prediction confirmed by the server
	shown     int  // number of leading preds currently drawn
	shownFrom int  // column of the first drawn prediction
	ackedOff  uint64
}

// New creates an engine for a cols x rows terminal.
func New(mode Mode, cols, rows int) *Engine {
	e := &Engine{mode: mode, connected: true}
	e.reset(cols, rows)
	return e
}

func (e *Engine) reset(cols, rows int) {
	if cols <= 0 || rows <= 0 {
		cols, rows = 80, 24
	}
	e.vt = vt10x.New(vt10x.WithSize(cols, rows), vt10x.WithWriter(io.Discard))
	e.cols = cols
	e.preds, e.shown, e.confirmed = nil, 0, false
}

// Reset forgets the screen (a different session is now shown); returns
// bytes that erase any drawn predictions.
func (e *Engine) Reset(cols, rows int) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	var b strings.Builder
	e.undraw(&b)
	e.reset(cols, rows)
	return []byte(b.String())
}

// Resize follows the real terminal size.
func (e *Engine) Resize(cols, rows int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if cols > 0 && rows > 0 {
		e.vt.Resize(cols, rows)
		e.cols = cols
	}
}

// SetRTT updates the smoothed round trip time.
func (e *Engine) SetRTT(d time.Duration) {
	e.mu.Lock()
	if e.srtt == 0 {
		e.srtt = d
	} else {
		e.srtt = (7*e.srtt + d) / 8
	}
	e.mu.Unlock()
}

// SRTT returns the smoothed round trip time.
func (e *Engine) SRTT() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.srtt
}

// SetConnected tells whether the link is up (predictions show while down).
func (e *Engine) SetConnected(up bool) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.connected = up
	var b strings.Builder
	e.undraw(&b)
	e.draw(&b)
	return []byte(b.String())
}

// Acked records that the server received input up to off.
func (e *Engine) Acked(off uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if off <= e.ackedOff {
		return
	}
	e.ackedOff = off
	now := time.Now()
	for i := range e.preds {
		if e.preds[i].off <= off && e.preds[i].ackedAt.IsZero() {
			e.preds[i].ackedAt = now
		}
	}
}

// Output processes server output for the screen; returns what to write to
// the real terminal (prediction erase + output + prediction redraw).
func (e *Engine) Output(p []byte) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mode == Never {
		_, _ = e.vt.Write(p)
		return p
	}
	var b strings.Builder
	e.undraw(&b)
	b.Write(p)
	_, _ = e.vt.Write(p)
	e.settle()
	e.draw(&b)
	return []byte(b.String())
}

// Tick re-evaluates predictions (expiry); returns bytes to write.
func (e *Engine) Tick() []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.preds) == 0 {
		return nil
	}
	before := len(e.preds)
	var b strings.Builder
	e.undraw(&b)
	e.settle()
	e.draw(&b)
	if len(e.preds) == before && e.shown > 0 && b.Len() > 0 {
		// nothing changed: avoid needless redraws
	}
	return []byte(b.String())
}

// Input processes keystrokes (off: input stream offset of p[0]); returns
// bytes to write (predicted echo).
func (e *Engine) Input(p []byte, off uint64) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mode == Never {
		return nil
	}
	var b strings.Builder
	e.undraw(&b)
	cur := e.vt.Cursor()
	altScreen := e.vt.Mode()&vt10x.ModeAltScreen != 0
	for len(p) > 0 {
		r, n := utf8.DecodeRune(p)
		p = p[n:]
		off += uint64(n)
		switch {
		case r == 0x7f || r == 0x08: // backspace: take back our own last prediction
			if len(e.preds) > 0 {
				e.preds = e.preds[:len(e.preds)-1]
			} else {
				e.endEpoch()
			}
		case r >= 0x20 && r != utf8.RuneError && unicode.IsPrint(r) && !isWide(r) && !altScreen:
			col := cur.X
			if len(e.preds) == 0 {
				e.row = cur.Y
			} else {
				col = e.preds[len(e.preds)-1].col + 1
			}
			if e.row != cur.Y || col >= e.cols-1 {
				e.endEpoch() // would wrap / cursor moved: stop guessing
				continue
			}
			e.preds = append(e.preds, pred{col: col, r: r, off: off})
		default: // Enter, escape sequences, control keys: new epoch
			e.endEpoch()
			if r == 0x1b { // rest of an escape sequence: not printable input
				p = nil
			}
		}
	}
	e.draw(&b)
	return []byte(b.String())
}

func isWide(r rune) bool {
	return r >= 0x1100 && (r <= 0x115f || r == 0x2329 || r == 0x232a ||
		(r >= 0x2e80 && r <= 0xa4cf && r != 0x303f) || (r >= 0xac00 && r <= 0xd7a3) ||
		(r >= 0xf900 && r <= 0xfaff) || (r >= 0xfe30 && r <= 0xfe6f) || (r >= 0xff00 && r <= 0xff60) ||
		(r >= 0xffe0 && r <= 0xffe6) || r >= 0x1f300)
}

func (e *Engine) endEpoch() {
	e.preds = nil
	e.confirmed = false
}

// settle confirms or withdraws predictions against the mirrored screen.
func (e *Engine) settle() {
	cur := e.vt.Cursor()
	grace := 250*time.Millisecond + 2*e.srtt
	for len(e.preds) > 0 {
		p := e.preds[0]
		cell := e.vt.Cell(p.col, e.row)
		if cell.Char == p.r {
			e.preds = e.preds[1:] // the server drew it: confirmed
			e.confirmed = true
			continue
		}
		if !p.ackedAt.IsZero() && time.Since(p.ackedAt) > grace {
			e.endEpoch() // server has the key but drew something else
			return
		}
		break
	}
	if len(e.preds) > 0 && cur.Y != e.row {
		e.endEpoch() // the server moved to another line
	}
}

func (e *Engine) visible() bool {
	switch e.mode {
	case Never:
		return false
	case Always:
		return e.confirmed
	}
	return e.confirmed && (e.srtt > ShowAbove || !e.connected)
}

// draw shows outstanding predictions; the real cursor ends after them.
func (e *Engine) draw(b *strings.Builder) {
	e.shown = 0
	if len(e.preds) == 0 || !e.visible() {
		return
	}
	cur := e.vt.Cursor()
	if cur.Y != e.row {
		return
	}
	first := e.preds[0].col
	moveCols(b, first-cur.X)
	if e.srtt > UnderlineAbove || !e.connected {
		b.WriteString("\x1b[0;4m")
	} else {
		b.WriteString("\x1b[0m")
	}
	for _, p := range e.preds {
		b.WriteRune(p.r)
	}
	b.WriteString("\x1b[0m")
	e.shown, e.shownFrom = len(e.preds), first
}

// undraw restores the cells under drawn predictions and puts the cursor
// and pen back where the server left them.
func (e *Engine) undraw(b *strings.Builder) {
	if e.shown == 0 {
		return
	}
	cur := e.vt.Cursor()
	end := e.shownFrom + e.shown // real cursor column now
	moveCols(b, e.shownFrom-end)
	for c := e.shownFrom; c < end; c++ {
		g := e.vt.Cell(c, e.row)
		b.WriteString(sgr(g))
		ch := g.Char
		if ch == 0 || !unicode.IsPrint(ch) {
			ch = ' '
		}
		b.WriteRune(ch)
	}
	moveCols(b, cur.X-end)
	b.WriteString(sgr(cur.Attr))
	e.shown = 0
}

func moveCols(b *strings.Builder, n int) {
	switch {
	case n > 0:
		fmt.Fprintf(b, "\x1b[%dC", n)
	case n < 0:
		fmt.Fprintf(b, "\x1b[%dD", -n)
	}
}

// vt10x attribute bits (unexported upstream).
const (
	attrReverse = 1 << iota
	attrUnderline
	attrBold
	attrGfx
	attrItalic
	attrBlink
)

// sgr renders a glyph's attributes as an SGR sequence.
func sgr(g vt10x.Glyph) string {
	parts := []string{"0"}
	if g.Mode&attrBold != 0 {
		parts = append(parts, "1")
	}
	if g.Mode&attrItalic != 0 {
		parts = append(parts, "3")
	}
	if g.Mode&attrUnderline != 0 {
		parts = append(parts, "4")
	}
	if g.Mode&attrBlink != 0 {
		parts = append(parts, "5")
	}
	if g.Mode&attrReverse != 0 {
		parts = append(parts, "7")
	}
	parts = append(parts, color(g.FG, 30, 90, 38, vt10x.DefaultFG)...)
	parts = append(parts, color(g.BG, 40, 100, 48, vt10x.DefaultBG)...)
	return "\x1b[" + strings.Join(parts, ";") + "m"
}

func color(c vt10x.Color, base, bright, ext int, def vt10x.Color) []string {
	switch {
	case c == def || c >= 1<<24:
		return nil
	case c < 8:
		return []string{fmt.Sprint(base + int(c))}
	case c < 16:
		return []string{fmt.Sprint(bright + int(c) - 8)}
	case c < 256:
		return []string{fmt.Sprint(ext), "5", fmt.Sprint(int(c))}
	default:
		return []string{fmt.Sprint(ext), "2", fmt.Sprint(int(c>>16) & 0xff), fmt.Sprint(int(c>>8) & 0xff), fmt.Sprint(int(c) & 0xff)}
	}
}
