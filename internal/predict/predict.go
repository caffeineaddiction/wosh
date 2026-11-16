// Package predict implements mosh-style speculative local echo.
//
// A terminal emulator mirrors what the server has drawn (cells, widths,
// cursor, pen). Keystrokes are applied to a predicted copy of the cursor's
// line - printable keys insert at the cursor, Backspace/Delete remove and
// shift the rest of the line left, Left/Right move the cursor - and the
// cells that differ from the mirror are drawn right away (typed text is
// underlined when latency is high), together with the predicted cursor.
//
// The server tells us (echo ack) which input the application has had time
// to answer; only then is a prediction judged: typed cells must show the
// typed character and the cursor must be where we put it. Anything else is
// a misprediction: everything is withdrawn and prediction restarts
// tentatively. Text the application draws by itself (fish
// autosuggestions) can therefore never confirm a keystroke.
//
// Like mosh, each "epoch" starts tentative: nothing is shown until a typed
// character of it has been confirmed. Cursor keys, Backspace and Delete
// stay in the current epoch; Enter, Up/Down, Tab and control keys start a
// new one, and since their effect cannot be predicted, nothing is
// predicted until the server has processed them. That keeps passwords and
// editor commands (vim normal mode) from ever showing as text.
//
// Unlike mosh, the real terminal still gets the raw output stream (so
// scrollback works); predictions are drawn with relative cursor movement
// only, so the mirror needs no knowledge of absolute screen position.
package predict

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"sort"
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

// cell is one terminal cell: s is a grapheme (w 1 or 2) or "" for the
// right half of a wide character (w 0).
type cell struct {
	s     string
	w     int
	style uv.Style
}

func (c cell) same(o cell) bool { return c.s == o.s && c.w == o.w && c.style.Equal(&o.style) }

func blank() cell { return cell{s: " ", w: 1} }

// ocell is the predicted content of one screen cell.
type ocell struct {
	cell
	typed bool   // shows a typed character (judged); else shifted/erased text
	off   uint64 // input offset of the keystroke that produced it
	epoch int
}

type cursorPred struct {
	pos
	off   uint64
	epoch int
}

type ackAt struct {
	off uint64
	at  time.Time
}

type drawn struct {
	rows   map[int][2]int // y -> [minX, maxX] we painted
	cursor pos            // where we left the real cursor
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

	overlay        map[pos]ocell
	cursors        []cursorPred // predicted cursor after each modelled key
	epoch          int
	confirmedEpoch int    // typed text of this epoch has been confirmed
	cursorEpoch    int    // a predicted cursor position of this epoch was right
	wantX          int    // column Up/Down aim for (vim curswant), -1 unknown
	blindUntil     uint64 // don't model keys until the server processed this much
	lastBlind      uint64 // offset of the last key we could not model
	inputEnd       uint64

	// where the editable input starts on the shell's line (Home, Ctrl-W,
	// word moves stop there); unknown until we see a fresh prompt
	inputStart    pos
	inputStartOK  bool
	freshAt       uint64 // offset of the last Enter/Ctrl-C/Ctrl-U/Ctrl-L
	inputStartFor uint64 // freshAt value inputStart was learned for

	echoOff  uint64
	echoSeen bool
	acks     []ackAt // fallback when the server sends no echo acks

	shown *drawn
}

// New creates an engine for a cols x rows terminal.
func New(mode Mode, cols, rows int) *Engine {
	e := &Engine{mode: mode, connected: true, epoch: 1, wantX: -1}
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
	e.clear()
	e.shown = nil
	e.confirmedEpoch = e.epoch - 1
}

func (e *Engine) clear() {
	e.overlay = map[pos]ocell{}
	e.cursors = nil
}

// Reset forgets the screen (a different session is now shown); returns
// bytes that erase any drawn predictions.
func (e *Engine) Reset(cols, rows int) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	var b strings.Builder
	e.undraw(&b)
	e.epoch++
	e.reset(cols, rows)
	e.blindUntil, e.lastBlind, e.echoOff, e.echoSeen, e.acks, e.inputEnd = 0, 0, 0, false, nil, 0
	return []byte(b.String())
}

// Resize follows the real terminal size.
func (e *Engine) Resize(cols, rows int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if cols > 0 && rows > 0 && (cols != e.cols || rows != e.rows) {
		e.vt.Resize(cols, rows)
		e.cols, e.rows = cols, rows
		e.shown = nil
		e.tentative(e.inputEnd)
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
	return e.redraw()
}

// Acked records that the server received input up to off (fallback timing
// when the server does not send echo acks).
func (e *Engine) Acked(off uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.acks) == 0 || off > e.acks[len(e.acks)-1].off {
		e.acks = append(e.acks, ackAt{off, time.Now()})
		if len(e.acks) > 256 {
			e.acks = e.acks[len(e.acks)-128:]
		}
	}
}

// EchoAck: the application has had time to answer input up to off, and its
// output is already in the stream before this. Returns bytes to write.
func (e *Engine) EchoAck(off uint64) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.echoSeen = true
	if off <= e.echoOff {
		return nil
	}
	e.echoOff = off
	if !e.settle() {
		return nil
	}
	return e.redraw()
}

// processed: input the server's screen state reflects.
func (e *Engine) processed() uint64 {
	if e.echoSeen {
		return e.echoOff
	}
	cut := time.Now().Add(-(250*time.Millisecond + e.srtt))
	var p uint64
	for _, a := range e.acks {
		if a.at.Before(cut) {
			p = a.off
		}
	}
	return p
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

// Tick re-evaluates predictions (fallback expiry); returns bytes to write.
func (e *Engine) Tick() []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.overlay) == 0 && len(e.cursors) == 0 {
		return nil
	}
	if !e.settle() {
		return nil
	}
	return e.redraw()
}

func (e *Engine) redraw() []byte {
	var b strings.Builder
	e.undraw(&b)
	e.draw(&b)
	return []byte(b.String())
}

// --- input ------------------------------------------------------------------

type keyKind int

const (
	kText keyKind = iota
	kBackspace
	kDelete
	kLeft
	kRight
	kHome
	kEnd
	kWordLeft
	kWordRight
	kKillWord  // Ctrl-W: back to whitespace (shells only)
	kKillAlnum // Alt-Backspace: previous word (shells only)
	kKillEnd   // Ctrl-K: cursor to end of text (shells only)
	kKillStart // Ctrl-U: start of input to cursor (shells only)
	kUp        // full-screen apps only (shells: history)
	kDown
	kOther
)

var cursorKeys = []struct {
	seq  string
	kind keyKind
}{
	{"\x1b[D", kLeft}, {"\x1bOD", kLeft}, {"\x1b[C", kRight}, {"\x1bOC", kRight}, {"\x1b[3~", kDelete},
	{"\x1b[H", kHome}, {"\x1bOH", kHome}, {"\x1b[1~", kHome}, {"\x1b[7~", kHome},
	{"\x1b[F", kEnd}, {"\x1bOF", kEnd}, {"\x1b[4~", kEnd}, {"\x1b[8~", kEnd},
	{"\x1b[1;5D", kWordLeft}, {"\x1b[1;3D", kWordLeft}, {"\x1bb", kWordLeft},
	{"\x1b[1;5C", kWordRight}, {"\x1b[1;3C", kWordRight}, {"\x1bf", kWordRight},
	{"\x1b\x7f", kKillAlnum},
	{"\x1b[A", kUp}, {"\x1bOA", kUp}, {"\x1b[B", kDown}, {"\x1bOB", kDown},
}

// nextKey splits one key off the input. altScreen: a full-screen app
// (vim, nano), where Ctrl-A/E/W mean something else than in shells.
func nextKey(p []byte, altScreen bool) (kind keyKind, text string, n int) {
	switch {
	case p[0] == 0x7f || p[0] == 0x08:
		return kBackspace, "", 1
	case p[0] == 0x01 && !altScreen: // Ctrl-A
		return kHome, "", 1
	case p[0] == 0x05 && !altScreen: // Ctrl-E
		return kEnd, "", 1
	case p[0] == 0x17 && !altScreen: // Ctrl-W
		return kKillWord, "", 1
	case p[0] == 0x0b && !altScreen: // Ctrl-K
		return kKillEnd, "", 1
	case p[0] == 0x15 && !altScreen: // Ctrl-U
		return kKillStart, "", 1
	case p[0] == 0x1b:
		for _, k := range cursorKeys {
			if bytes.HasPrefix(p, []byte(k.seq)) {
				return k.kind, "", len(k.seq)
			}
		}
		return kOther, "", len(p) // other escape sequence (or bare Esc): rest of the chunk
	}
	r, size := utf8.DecodeRune(p)
	if r >= 0x20 && r != utf8.RuneError && unicode.IsPrint(r) {
		return kText, string(r), size
	}
	return kOther, "", size
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
	for len(p) > 0 {
		kind, text, n := nextKey(p, e.vt.IsAltScreen())
		alt := e.vt.IsAltScreen()
		if (kind == kKillAlnum && alt) || ((kind == kUp || kind == kDown) && !alt) {
			kind = kOther // shells: Up/Down walk history - unpredictable
		}
		key := p[0]
		p = p[n:]
		off += uint64(n)
		e.inputEnd = off
		if kind == kOther {
			e.blind(off) // Enter, Tab, Up/Down, Ctrl-..: unpredictable
			e.epoch++
			switch key {
			case '\r', '\n', 0x03, 0x15, 0x0c: // Enter, Ctrl-C, Ctrl-U (full-screen apps), Ctrl-L
				e.freshAt = off // the next prompt / empty input starts after this
			}
			continue
		}
		if e.processed() < e.blindUntil {
			e.blind(off) // still waiting for the server to catch up
		} else if !e.apply(kind, text, off) {
			e.blind(off)
			e.epoch++
		} else {
			continue
		}
		if kind == kKillStart {
			e.freshAt = off // not modelled: relearn the input start from the server
		}
	}
	e.draw(&b)
	return []byte(b.String())
}

func (e *Engine) blind(off uint64) {
	e.blindUntil, e.lastBlind = off, off
}

// tentative: withdraw everything; nothing is predicted until the server
// has processed input up to off, and the next epoch starts unconfirmed.
func (e *Engine) tentative(off uint64) {
	e.clear()
	e.epoch++
	e.confirmedEpoch = min(e.confirmedEpoch, e.epoch-1)
	e.cursorEpoch = min(e.cursorEpoch, e.epoch-1)
	e.wantX = -1
	if off > e.blindUntil {
		e.blind(off)
	}
}

// --- model --------------------------------------------------------------------

func (e *Engine) mirrorCell(x, y int) cell {
	c := e.vt.CellAt(x, y)
	if c == nil {
		return blank()
	}
	if c.Width == 0 && c.Content == "" {
		return cell{s: "", w: 0, style: c.Style}
	}
	s := c.Content
	if s == "" {
		s = " "
	}
	return cell{s: s, w: max(c.Width, 1), style: c.Style}
}

// displayed: what the predicted screen shows (mirror + every prediction,
// shown or tentative).
func (e *Engine) displayed(x, y int) cell {
	if oc, ok := e.overlay[pos{x, y}]; ok {
		return oc.cell
	}
	return e.mirrorCell(x, y)
}

func (e *Engine) predictedCursor() pos {
	if n := len(e.cursors); n > 0 {
		return e.cursors[n-1].pos
	}
	c := e.vt.CursorPosition()
	return pos{c.X, c.Y}
}

type glyph struct {
	cell
	typed bool
}

// rowGlyphs returns the displayed row as glyphs, the glyph index at cell x
// and the index just past the last non-blank glyph.
func (e *Engine) rowGlyphs(y, x int) (gs []glyph, idx, textEnd int) {
	idx = -1
	for cx := 0; cx < e.cols; {
		c := e.displayed(cx, y)
		oc, isOverlay := e.overlay[pos{cx, y}]
		if c.w == 0 { // stray right half: treat as blank
			c = blank()
		}
		if idx < 0 && cx >= x {
			idx = len(gs)
		}
		gs = append(gs, glyph{cell: c, typed: isOverlay && oc.typed})
		if c.s != " " {
			textEnd = len(gs)
		}
		cx += c.w
	}
	if idx < 0 {
		idx = len(gs)
	}
	return gs, idx, textEnd
}

// layout turns glyphs back into cols cells; ok is false if non-blank text
// would not fit.
func (e *Engine) layout(gs []glyph) (cells []glyph, ok bool) {
	for _, g := range gs {
		if len(cells)+g.w > e.cols {
			if g.s != " " {
				return nil, false
			}
			continue
		}
		cells = append(cells, g)
		if g.w == 2 {
			cells = append(cells, glyph{cell: cell{s: "", w: 0, style: g.style}, typed: g.typed})
		}
	}
	for len(cells) < e.cols {
		cells = append(cells, glyph{cell: blank()})
	}
	return cells, true
}

func startX(gs []glyph, idx int) int {
	x := 0
	for i := 0; i < idx && i < len(gs); i++ {
		x += gs[i].w
	}
	return x
}

// apply models one key on the predicted screen; false if it cannot be
// predicted (the caller then stops predicting until the server catches up).
func (e *Engine) apply(kind keyKind, text string, off uint64) bool {
	cur := e.predictedCursor()
	gs, idx, textEnd := e.rowGlyphs(cur.y, cur.x)
	alt := e.vt.IsAltScreen()
	lineStart, startOK := 0, alt
	if !alt {
		e.learnInputStart(cur, gs, idx, textEnd)
		if e.inputStartOK && e.inputStart.y == cur.y {
			lineStart, startOK = e.inputStart.x, true
		}
	}
	startIdx := 0
	for startIdx < len(gs) && startX(gs, startIdx) < lineStart {
		startIdx++
	}
	beforeCells, ok := e.layout(gs)
	if !ok {
		return false
	}
	gs = append([]glyph(nil), gs...)
	nx, ny := cur.x, cur.y
	switch kind {
	case kText:
		w := e.vt.WidthMethod().StringWidth(text)
		if w < 1 || w > 2 {
			return false
		}
		g := glyph{cell: cell{s: text, w: w, style: e.pen()}, typed: true}
		if idx >= textEnd && cur.x+w > e.cols {
			// appending past the right margin: continue on the next row
			if cur.y >= e.rows-1 {
				return false // would scroll
			}
			e.set(pos{0, cur.y + 1}, g, off)
			if w == 2 {
				e.set(pos{1, cur.y + 1}, glyph{cell: cell{s: "", w: 0, style: g.style}, typed: true}, off)
			}
			e.cursors = append(e.cursors, cursorPred{pos{w, cur.y + 1}, off, e.epoch})
			return true
		}
		if idx < len(gs) && gs[idx].s == text && gs[idx].w == w && !gs[idx].typed {
			// the screen already shows it (an autosuggestion, or retyping
			// existing text): overwrite instead of inserting
			gs[idx] = g
		} else {
			gs = append(gs[:idx], append([]glyph{g}, gs[idx:]...)...)
		}
		nx = startX(gs, idx+1)
	case kBackspace:
		if idx == 0 {
			return false // would join with the previous line
		}
		nx = startX(gs, idx-1)
		gs = append(gs[:idx-1], gs[idx:]...)
	case kDelete:
		if idx < textEnd {
			gs = append(gs[:idx], gs[idx+1:]...)
		}
	case kLeft:
		if idx == 0 {
			return false
		}
		nx = startX(gs, idx-1)
	case kRight:
		if idx < textEnd {
			nx = startX(gs, idx+1)
		}
	case kHome:
		if !startOK {
			return false
		}
		nx = lineStart
	case kEnd:
		nx = startX(gs, max(textEnd, idx))
	case kWordLeft:
		if !startOK || idx <= startIdx {
			return false
		}
		nx = startX(gs, wordLeft(gs, idx, startIdx, isWordChar(alt)))
	case kWordRight:
		if idx >= textEnd {
			break
		}
		if alt { // vim/nano: start of the next word
			nx = startX(gs, wordStartRight(gs, idx, textEnd, isWordChar(alt)))
		} else { // readline/fish: end of the next word
			nx = startX(gs, wordEndRight(gs, idx, textEnd, isWordChar(alt)))
		}
	case kUp, kDown:
		ty := cur.y - 1
		if kind == kDown {
			ty = cur.y + 1
		}
		if ty < 0 || ty > e.rows-2 { // status / help bars, or scrolling
			return false
		}
		tg, _, tEnd := e.rowGlyphs(ty, 0)
		if len(tg) > 0 && tg[0].s == "~" { // vim: past the end of the file
			return false
		}
		want := e.wantX
		if want < 0 {
			want = cur.x
		}
		// vim's normal mode keeps the cursor on the last character; insert
		// mode and nano allow it just after the text
		limit := tEnd
		if !e.cursorMayPassEnd() && limit > 0 {
			limit--
		}
		tx, i := 0, 0
		for i < limit && tx+tg[i].w <= want {
			tx += tg[i].w
			i++
		}
		e.wantX = want
		e.cursors = append(e.cursors, cursorPred{pos{tx, ty}, off, e.epoch})
		return true
	case kKillWord, kKillAlnum:
		if !startOK || idx <= startIdx {
			return false
		}
		to := wordLeft(gs, idx, startIdx, isWordChar(false))
		if kind == kKillWord { // unix-word-rubout: back to whitespace
			to = wordLeft(gs, idx, startIdx, func(s string) bool { return s != " " })
		}
		nx = startX(gs, to)
		gs = append(gs[:to], gs[idx:]...)
	case kKillEnd: // readline kill-line
		if startX(gs, textEnd) >= e.cols {
			return false // text reaches the margin: may continue on the next row
		}
		if idx < textEnd {
			gs = gs[:idx]
		}
	case kKillStart: // readline unix-line-discard (zsh kills the whole line: withdrawn)
		if !startOK || idx < startIdx || startX(gs, textEnd) >= e.cols {
			return false
		}
		nx = lineStart
		gs = append(gs[:startIdx], gs[idx:]...)
	}
	after, ok := e.layout(gs)
	if !ok || nx > e.cols || (nx == e.cols && kind != kText) {
		return false // (nx == cols: wrote the last column, cursor waits to wrap)
	}
	for x := 0; x < e.cols; x++ {
		if !after[x].same(beforeCells[x].cell) || after[x].typed != beforeCells[x].typed {
			e.set(pos{x, ny}, after[x], off)
		}
	}
	e.cursors = append(e.cursors, cursorPred{pos{nx, ny}, off, e.epoch})
	e.wantX = nx
	return true
}

func (e *Engine) set(p pos, g glyph, off uint64) {
	if !g.typed && g.same(e.mirrorCell(p.x, p.y)) {
		delete(e.overlay, p) // back to what the server shows
		return
	}
	e.overlay[p] = ocell{cell: g.cell, typed: g.typed, off: off, epoch: e.epoch}
}

// settle judges predictions the server has processed; returns true if the
// overlay changed.
func (e *Engine) settle() bool {
	done := e.processed()
	changed, wrong := false, false
	for p, oc := range e.overlay {
		if oc.off > done {
			continue
		}
		changed = true
		delete(e.overlay, p)
		if !oc.typed || oc.w == 0 {
			continue // shifted or erased text: the app may redraw it its own way
		}
		if e.mirrorCell(p.x, p.y).s == oc.s {
			e.confirmedEpoch = max(e.confirmedEpoch, oc.epoch)
		} else {
			wrong = true
		}
	}
	i := 0
	for i < len(e.cursors) && e.cursors[i].off <= done {
		i++
	}
	if i > 0 {
		last := e.cursors[i-1]
		e.cursors = e.cursors[i:]
		changed = true
		// keys we did not model after it may have moved the cursor
		unmodelled := e.lastBlind > last.off && e.lastBlind <= done
		c := e.vt.CursorPosition()
		if !unmodelled && (c.X != min(last.x, e.cols-1) || c.Y != last.y) {
			wrong = true
		} else if !unmodelled {
			e.cursorEpoch = max(e.cursorEpoch, last.epoch)
		}
	}
	if wrong {
		e.tentative(e.inputEnd)
	}
	return changed || wrong
}

// --- drawing ----------------------------------------------------------------

func (e *Engine) visible() bool {
	switch e.mode {
	case Never:
		return false
	case Always:
		return true
	}
	return e.srtt > ShowAbove || !e.connected
}

func (e *Engine) shownCell(p pos) (ocell, bool) {
	oc, ok := e.overlay[p]
	return oc, ok && oc.epoch <= e.confirmedEpoch
}

// draw paints visible predictions and parks the cursor where predicted.
func (e *Engine) draw(b *strings.Builder) {
	e.shown = nil
	if !e.visible() {
		return
	}
	rows := map[int][2]int{}
	for p := range e.overlay {
		if _, ok := e.shownCell(p); !ok {
			continue
		}
		r, ok := rows[p.y]
		if !ok {
			r = [2]int{p.x, p.x}
		}
		rows[p.y] = [2]int{min(r[0], p.x), max(r[1], p.x)}
	}
	// the predicted cursor is shown once this epoch's cursor or text was
	// confirmed - but never ahead of typed text we are not showing yet
	var curTarget *pos
	if n := len(e.cursors); n > 0 && e.cursors[n-1].epoch <= max(e.confirmedEpoch, e.cursorEpoch) {
		hidden := false
		for p := range e.overlay {
			if _, ok := e.shownCell(p); !ok {
				hidden = true
				break
			}
		}
		if !hidden {
			c := e.cursors[n-1].pos
			curTarget = &c
		}
	}
	if len(rows) == 0 && curTarget == nil {
		return
	}
	mc := e.vt.CursorPosition()
	at := pos{mc.X, mc.Y}
	flag := e.srtt > UnderlineAbove || !e.connected
	for _, y := range sortedKeys(rows) {
		r := rows[y]
		x := r[0]
		if e.displayed(x, y).w == 0 && x > 0 {
			x-- // start on the left half of a wide character
		}
		at = e.move(b, at, pos{x, y})
		for ; x <= r[1] && x < e.cols; x++ {
			c := e.displayed(x, y)
			oc, shown := e.shownCell(pos{x, y})
			if !shown {
				c = e.mirrorCell(x, y)
			}
			if c.w == 0 {
				continue
			}
			b.WriteString(sgr(c.style, shown && oc.typed && flag))
			b.WriteString(c.s)
			x += c.w - 1
			at = pos{x + 1, y}
		}
		b.WriteString("\x1b[0m")
	}
	target := pos{mc.X, mc.Y}
	if curTarget != nil {
		target = *curTarget
	}
	at = e.move(b, at, target)
	e.shown = &drawn{rows: rows, cursor: at}
}

// undraw restores the cells under drawn predictions and puts the cursor
// and pen back where the server left them.
func (e *Engine) undraw(b *strings.Builder) {
	d := e.shown
	if d == nil {
		return
	}
	e.shown = nil
	at := d.cursor
	for _, y := range sortedKeys(d.rows) {
		r := d.rows[y]
		x := r[0]
		if e.mirrorCell(x, y).w == 0 && x > 0 {
			x--
		}
		at = e.move(b, at, pos{x, y})
		for ; x <= r[1] && x < e.cols; x++ {
			c := e.mirrorCell(x, y)
			if c.w == 0 {
				continue
			}
			b.WriteString(sgr(c.style, false))
			b.WriteString(c.s)
			x += c.w - 1
			at = pos{x + 1, y}
		}
	}
	b.WriteString("\x1b[0m")
	mc := e.vt.CursorPosition()
	e.move(b, at, pos{mc.X, mc.Y})
	b.WriteString(sgr(e.pen(), false))
}

func sortedKeys(m map[int][2]int) []int {
	ks := make([]int, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Ints(ks)
	return ks
}

// move emits relative cursor movement from 'from' to 'to'; returns 'to'.
func (e *Engine) move(b *strings.Builder, from, to pos) pos {
	if from.x >= e.cols {
		from.x = e.cols - 1 // pending wrap after writing the last column
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

func sgr(s uv.Style, underline bool) string {
	if underline {
		s.Underline = uv.UnderlineSingle
	}
	if s.IsZero() {
		return "\x1b[0m"
	}
	return "\x1b[0m" + s.String()
}

// learnInputStart remembers where the shell's input begins: the cursor at
// the first key typed on a fresh prompt (after Enter/Ctrl-C/Ctrl-U/Ctrl-L
// has been processed), or on the first prompt we see with nothing typed
// to the right of the cursor yet.
func (e *Engine) learnInputStart(cur pos, gs []glyph, idx, textEnd int) {
	if len(e.cursors) > 0 {
		return // mid-prediction: the cursor is ours, not the server's
	}
	fresh := e.freshAt > e.inputStartFor && e.processed() >= e.freshAt
	first := !e.inputStartOK && idx >= textEnd
	if e.inputStartOK && e.inputStart.y != cur.y {
		e.inputStartOK = false
		first = idx >= textEnd
	}
	if fresh || first {
		e.inputStart, e.inputStartOK, e.inputStartFor = cur, true, e.freshAt
	}
}

func isWordChar(alt bool) func(string) bool {
	return func(s string) bool {
		r, _ := utf8.DecodeRuneInString(s)
		return unicode.IsLetter(r) || unicode.IsDigit(r) || (alt && r == '_')
	}
}

// wordLeft: start of the word before idx (skip non-word, then word chars).
func wordLeft(gs []glyph, idx, lo int, word func(string) bool) int {
	i := idx
	for i > lo && !word(gs[i-1].s) {
		i--
	}
	for i > lo && word(gs[i-1].s) {
		i--
	}
	return i
}

// wordEndRight: end of the next word (readline forward-word).
func wordEndRight(gs []glyph, idx, hi int, word func(string) bool) int {
	i := idx
	for i < hi && !word(gs[i].s) {
		i++
	}
	for i < hi && word(gs[i].s) {
		i++
	}
	return i
}

// wordStartRight: start of the next word (vim/nano Ctrl-Right).
func wordStartRight(gs []glyph, idx, hi int, word func(string) bool) int {
	i := idx
	for i < hi && word(gs[i].s) {
		i++
	}
	for i < hi && !word(gs[i].s) {
		i++
	}
	return i
}

// cursorMayPassEnd: in a full-screen app, can the cursor sit after the last
// character of a line? True in vim's insert/replace mode (shown on the
// bottom row) and in nano (help bar with ^X); false in vim's normal mode.
func (e *Engine) cursorMayPassEnd() bool {
	for y := max(e.rows-3, 0); y < e.rows; y++ {
		var b strings.Builder
		for x := 0; x < e.cols; x++ {
			if c := e.mirrorCell(x, y); c.w > 0 {
				b.WriteString(c.s)
			}
		}
		row := b.String()
		if strings.Contains(row, "-- INSERT --") || strings.Contains(row, "-- REPLACE --") || strings.Contains(row, "^X") {
			return true
		}
	}
	return false
}
