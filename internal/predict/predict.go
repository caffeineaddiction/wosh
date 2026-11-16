// Package predict implements mosh-style speculative local echo.
//
// A terminal emulator mirrors what the server has drawn, so we know the
// cursor position and the contents (and width) of every cell. When the user
// types a printable character we draw it at the predicted position right
// away (underlined when latency is high) instead of waiting a round trip
// for the remote echo. When server output arrives, predictions are first
// erased (cells restored from the mirror), the output is passed through
// untouched, and predictions that are still outstanding are drawn again.
// A prediction is confirmed when the mirrored cell shows the predicted
// character; if the server acknowledged the keystroke but the cell does not
// match within a grace period, it was a misprediction: everything is
// withdrawn and prediction restarts tentatively.
//
// Like mosh, each "epoch" (a stretch of typing without cursor-moving keys)
// starts tentative: nothing is shown until one of its predictions is
// confirmed by the server. That keeps passwords and editor commands (vim
// normal mode) from ever showing up as predicted text, while insert-mode
// typing in vim/nano and shells is predicted after the first echo.
//
// Typing in the middle of a line is predicted as an insertion (the rest of
// the line shifts right, as shells, vim and nano do). Predictions wrap to
// the next row at the right margin (except on the bottom row, which would
// scroll) and handle double-width characters.
//
// Unlike mosh, the real terminal still gets the raw output stream (so
// scrollback works); predictions are drawn with relative cursor movement
// only, so the mirror needs no knowledge of absolute screen position.
package predict

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
	"unsafe"

	uv "github.com/charmbracelet/ultraviolet"
	"github.com/charmbracelet/x/vt"
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

type pos struct{ x, y int }

type pred struct {
	pos
	s       string // grapheme
	w       int    // cell width
	off     uint64 // input stream offset just past this keystroke
	ackedAt time.Time
}

// drawn remembers what is on the real terminal on top of the mirror.
type drawn struct {
	from   pos // first cell we overwrote
	end    pos // cell after the last one we overwrote (may be x == cols)
	cursor pos // where we left the real cursor
}

// Engine is the prediction state for the session on screen.
type Engine struct {
	mu   sync.Mutex
	mode Mode
	vt   *vt.Emulator
	cols int
	rows int

	srtt      time.Duration
	connected bool

	preds     []pred
	confirmed bool // epoch has had a prediction confirmed by the server
	shown     *drawn
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
	if e.vt != nil {
		_ = e.vt.Close()
	}
	e.vt = vt.NewEmulator(cols, rows)
	go func(v *vt.Emulator) { _, _ = io.Copy(io.Discard, v) }(e.vt) // replies to queries
	e.cols, e.rows = cols, rows
	e.preds, e.shown, e.confirmed = nil, nil, false
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
	if cols > 0 && rows > 0 && (cols != e.cols || rows != e.rows) {
		e.vt.Resize(cols, rows)
		e.cols, e.rows = cols, rows
		e.preds, e.shown, e.confirmed = nil, nil, false
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
	n := len(e.preds)
	e.settle()
	if len(e.preds) == n {
		return nil // nothing changed: no redraw
	}
	var b strings.Builder
	e.undrawStale(&b)
	e.draw(&b)
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
	cur := e.cursor()
	width := e.vt.WidthMethod()
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
		case r >= 0x20 && r != utf8.RuneError && unicode.IsPrint(r):
			s := string(r)
			w := width.StringWidth(s)
			if w < 1 || w > 2 {
				e.endEpoch()
				continue
			}
			at := cur.pos()
			if len(e.preds) > 0 {
				last := e.preds[len(e.preds)-1]
				at = pos{last.x + last.w, last.y}
			}
			if at.x+w > e.cols { // wraps to the next row
				if at.y >= e.rows-1 {
					e.endEpoch() // bottom row would scroll: stop guessing
					continue
				}
				at = pos{0, at.y + 1}
			}
			e.preds = append(e.preds, pred{pos: at, s: s, w: w, off: off})
		default: // Enter, Tab, Esc, arrows, control keys: new epoch
			e.endEpoch()
			if r == 0x1b { // rest of an escape sequence is not text
				p = nil
			}
		}
	}
	e.draw(&b)
	return []byte(b.String())
}

func (e *Engine) endEpoch() {
	e.preds = nil
	e.confirmed = false
}

type cursorPos struct{ x, y int }

func (c cursorPos) pos() pos { return pos{c.x, c.y} }

func (e *Engine) cursor() cursorPos {
	p := e.vt.CursorPosition()
	return cursorPos{p.X, p.Y}
}

func (e *Engine) cell(x, y int) (content string, width int, style uv.Style) {
	c := e.vt.CellAt(x, y)
	if c == nil {
		return " ", 1, uv.Style{}
	}
	if c.Width == 0 && c.Content == "" {
		return "", 0, c.Style // right half of a wide character
	}
	content = c.Content
	if content == "" {
		content = " "
	}
	return content, max(c.Width, 1), c.Style
}

// settle confirms or withdraws predictions against the mirrored screen.
func (e *Engine) settle() {
	grace := 250*time.Millisecond + 2*e.srtt
	for len(e.preds) > 0 {
		p := e.preds[0]
		// only judge keystrokes the server has received: text it drew on
		// its own (fish autosuggestions, vim completion) must not count
		if s, _, _ := e.cell(p.x, p.y); s == p.s && p.off <= e.ackedOff {
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
	if len(e.preds) > 0 {
		cur := e.cursor()
		first := e.preds[0]
		if cur.y != first.y || cur.x > first.x {
			// the server's cursor is somewhere our next prediction is not:
			// it did something else with the keys (moved, scrolled, redrew)
			if cur.y != first.y || !first.ackedAt.IsZero() {
				e.endEpoch()
			}
		}
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
	e.shown = nil
	if len(e.preds) == 0 || !e.visible() {
		return
	}
	cur := e.cursor()
	first, last := e.preds[0], e.preds[len(e.preds)-1]
	if cur.y != first.y || cur.x > first.x {
		return
	}
	at := cur.pos()
	at = e.move(b, at, first.pos)
	style := "\x1b[0m"
	if e.srtt > UnderlineAbove || !e.connected {
		style = "\x1b[0;4m"
	}
	b.WriteString(style)
	for _, p := range e.preds {
		if p.y != at.y { // wrapped: the terminal moves to the next row itself
			at = pos{0, p.y}
		}
		b.WriteString(p.s)
		at.x = p.x + p.w
	}
	b.WriteString("\x1b[0m")
	end := pos{last.x + last.w, last.y}
	d := &drawn{from: first.pos, end: end}
	// insertion: if the predictions are on one row, shift the rest of the
	// row right like the application will (never touching the last column,
	// so the cursor never enters the terminal's pending-wrap state)
	if s, _, _ := e.cell(first.x, first.y); s == first.s {
		// the screen already shows what is being typed there (an
		// autosuggestion, or retyping existing text): overwrite, no shift
	} else if first.y == last.y && end.x < e.cols {
		shift := end.x - first.x
		var tail strings.Builder
		x, tx := first.x, end.x
		for x < e.cols && tx < e.cols-1 {
			s, w, st := e.cell(x, first.y)
			if w == 0 {
				x++
				continue
			}
			if tx+w > e.cols-1 {
				break
			}
			tail.WriteString(sgr(st))
			tail.WriteString(s)
			x += w
			tx += w
		}
		if tx > end.x && !e.blankFrom(first.x, first.y, e.cols-1-shift) {
			b.WriteString(tail.String())
			b.WriteString("\x1b[0m")
			at = pos{tx, first.y}
			d.end = at
		}
	}
	d.cursor = e.move(b, at, end)
	if end.x >= e.cols { // filled the row: cursor is parked at the margin
		d.cursor = pos{e.cols - 1, end.y}
	}
	e.shown = d
}

// blankFrom reports whether row y holds only blanks from x up to limit.
func (e *Engine) blankFrom(x, y, limit int) bool {
	for ; x < e.cols && x <= limit; x++ {
		if s, w, st := e.cell(x, y); w != 0 && (s != " " || !st.IsZero()) {
			return false
		}
	}
	return true
}

// undraw restores the cells under drawn predictions and puts the cursor
// and pen back where the server left them.
func (e *Engine) undraw(b *strings.Builder) {
	d := e.shown
	if d == nil {
		return
	}
	e.shown = nil
	at := e.move(b, d.cursor, d.from)
	// rewrite the mirror's cells over the span we painted
	y, x := d.from.y, d.from.x
	if s, w, _ := e.cell(x, y); w == 0 && s == "" && x > 0 {
		at = e.move(b, at, pos{x - 1, y}) // started on the right half of a wide char
		x--
	}
	for y < d.end.y || (y == d.end.y && x < d.end.x) {
		if x >= e.cols {
			y, x = y+1, 0
			at = e.move(b, pos{e.cols - 1, y - 1}, pos{0, y})
			continue
		}
		s, w, st := e.cell(x, y)
		if w == 0 {
			x++
			continue
		}
		b.WriteString(sgr(st))
		b.WriteString(s)
		x += w
		at = pos{x, y}
	}
	b.WriteString("\x1b[0m")
	if at.x >= e.cols {
		at.x = e.cols - 1
	}
	e.move(b, at, e.cursor().pos())
	b.WriteString(sgr(e.pen()))
}

// undrawStale: Tick path (no new output) - same as undraw.
func (e *Engine) undrawStale(b *strings.Builder) { e.undraw(b) }

// move emits relative cursor movement from 'from' to 'to'; returns 'to'.
func (e *Engine) move(b *strings.Builder, from, to pos) pos {
	if from.x >= e.cols {
		from.x = e.cols - 1
	}
	switch dy := to.y - from.y; {
	case dy > 0:
		fmt.Fprintf(b, "\x1b[%dB", dy)
	case dy < 0:
		fmt.Fprintf(b, "\x1b[%dA", -dy)
	}
	switch dx := to.x - from.x; {
	case dx > 0:
		fmt.Fprintf(b, "\x1b[%dC", dx)
	case dx < 0:
		fmt.Fprintf(b, "\x1b[%dD", -dx)
	}
	return to
}

// pen returns the style the server would draw the next character with.
// The emulator keeps it unexported (Emulator.scr.cur.Pen); read it via
// reflection. TestPenAccess guards this against library changes.
func (e *Engine) pen() uv.Style {
	defer func() { _ = recover() }()
	scr := reflect.ValueOf(e.vt).Elem().FieldByName("scr")
	if !scr.IsValid() || scr.IsNil() {
		return uv.Style{}
	}
	f := scr.Elem().FieldByName("cur").FieldByName("Pen")
	if !f.IsValid() {
		return uv.Style{}
	}
	return *(*uv.Style)(unsafe.Pointer(f.UnsafeAddr()))
}

func sgr(s uv.Style) string {
	if s.IsZero() {
		return "\x1b[0m"
	}
	return "\x1b[0m" + s.String()
}
