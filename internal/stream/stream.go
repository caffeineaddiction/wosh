// Package stream carries byte streams (port forwards, file transfers) over
// extra WebRTC data channels, and over the daemon's unix socket to local
// helper processes (ssh/scp shims) with the same message types.
package stream

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"wosh/internal/wire"
)

// Message types (first byte of every data channel message / frame).
const (
	Data   = 'd' // payload bytes (stdout for exec)
	Fin    = 'f' // no more Data from this side (half close)
	Stderr = 'e' // exec: stderr bytes
	Exit   = 'x' // exec: 4-byte exit status
	Err    = '!' // refused / failed: message text
)

const labelPrefix = "s:"

const (
	chunk       = 16 * 1024
	maxBuffered = 1 << 20
)

// Label encodes a spec as a data channel label.
func Label(s wire.StreamSpec) string {
	b, _ := json.Marshal(s)
	return labelPrefix + string(b)
}

// ParseLabel decodes a data channel label; ok is false for non-stream channels.
func ParseLabel(l string) (wire.StreamSpec, bool) {
	var s wire.StreamSpec
	if !strings.HasPrefix(l, labelPrefix) {
		return s, false
	}
	return s, json.Unmarshal([]byte(l[len(labelPrefix):]), &s) == nil
}

// Conn is a message stream: a data channel or a framed local socket.
type Conn interface {
	Recv() (typ byte, payload []byte, err error)
	Send(typ byte, p []byte) error
	Close() error
}

// --- data channel -----------------------------------------------------------

// Chan adapts a data channel to Conn.
type Chan struct {
	dc     *webrtc.DataChannel
	opened chan struct{}
	low    chan struct{}

	mu     sync.Mutex
	cond   *sync.Cond
	queue  [][]byte
	closed bool
	dead   atomic.Bool // replaced by a newer channel: stop sending
}

// kill abandons the channel (a resumed stream moved to a new one).
func (c *Chan) kill() {
	c.dead.Store(true)
	_ = c.dc.Close()
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.cond.Broadcast()
}

// Wrap attaches to a data channel (call before it opens, e.g. in
// OnDataChannel or right after CreateDataChannel).
func Wrap(dc *webrtc.DataChannel) *Chan {
	c := &Chan{dc: dc, opened: make(chan struct{}), low: make(chan struct{}, 1)}
	c.cond = sync.NewCond(&c.mu)
	dc.SetBufferedAmountLowThreshold(maxBuffered / 2)
	dc.OnBufferedAmountLow(func() {
		select {
		case c.low <- struct{}{}:
		default:
		}
	})
	var once sync.Once
	markOpen := func() { once.Do(func() { close(c.opened) }) }
	dc.OnOpen(markOpen)
	if dc.ReadyState() == webrtc.DataChannelStateOpen {
		markOpen()
	}
	dc.OnMessage(func(m webrtc.DataChannelMessage) {
		c.mu.Lock()
		c.queue = append(c.queue, m.Data)
		c.mu.Unlock()
		c.cond.Broadcast()
	})
	dc.OnClose(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.cond.Broadcast()
	})
	return c
}

// Open creates a stream channel on pc and waits until it is open.
func Open(pc *webrtc.PeerConnection, spec wire.StreamSpec) (*Chan, error) {
	dc, err := pc.CreateDataChannel(Label(spec), nil)
	if err != nil {
		return nil, err
	}
	c := Wrap(dc)
	select {
	case <-c.opened:
		return c, nil
	case <-time.After(15 * time.Second):
		_ = dc.Close()
		return nil, errors.New("stream open timeout")
	}
}

func (c *Chan) Recv() (byte, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.queue) == 0 && !c.closed {
		c.cond.Wait()
	}
	if len(c.queue) == 0 {
		return 0, nil, io.EOF
	}
	m := c.queue[0]
	c.queue = c.queue[1:]
	if len(m) == 0 {
		return 0, nil, errors.New("empty message")
	}
	return m[0], m[1:], nil
}

func (c *Chan) Send(typ byte, p []byte) error {
	select {
	case <-c.opened:
	case <-time.After(15 * time.Second):
		return errors.New("stream not open")
	}
	for {
		n := min(len(p), chunk)
		for c.dc.BufferedAmount() > maxBuffered {
			if c.dead.Load() || c.dc.ReadyState() != webrtc.DataChannelStateOpen {
				return io.ErrClosedPipe
			}
			select {
			case <-c.low:
			case <-time.After(200 * time.Millisecond):
			}
		}
		if c.dead.Load() {
			return io.ErrClosedPipe
		}
		msg := make([]byte, 1+n)
		msg[0] = typ
		copy(msg[1:], p[:n])
		if err := c.dc.Send(msg); err != nil {
			return err
		}
		p = p[n:]
		if len(p) == 0 {
			return nil
		}
	}
}

func (c *Chan) Close() error {
	// let queued data drain before closing
	deadline := time.Now().Add(5 * time.Second)
	for c.dc.BufferedAmount() > 0 && time.Now().Before(deadline) && c.dc.ReadyState() == webrtc.DataChannelStateOpen {
		time.Sleep(10 * time.Millisecond)
	}
	return c.dc.Close()
}

// --- framed socket ------------------------------------------------------------

// Framed adapts a byte stream (unix socket) to Conn: [type][u32 len][payload].
type Framed struct {
	r  io.Reader
	w  io.Writer
	c  io.Closer
	mu sync.Mutex
}

// NewFramed wraps r/w; c (optional) is closed by Close.
func NewFramed(r io.Reader, w io.Writer, c io.Closer) *Framed { return &Framed{r: r, w: w, c: c} }

func (f *Framed) Recv() (byte, []byte, error) {
	var h [5]byte
	if _, err := io.ReadFull(f.r, h[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(h[1:])
	if n > 16<<20 {
		return 0, nil, errors.New("frame too large")
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(f.r, p); err != nil {
		return 0, nil, err
	}
	return h[0], p, nil
}

func (f *Framed) Send(typ byte, p []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var h [5]byte
	h[0] = typ
	binary.BigEndian.PutUint32(h[1:], uint32(len(p)))
	if _, err := f.w.Write(h[:]); err != nil {
		return err
	}
	_, err := f.w.Write(p)
	return err
}

func (f *Framed) Close() error {
	if f.c != nil {
		return f.c.Close()
	}
	return nil
}

// --- plumbing -------------------------------------------------------------------

// Relay copies messages both ways between a and b until either ends.
func Relay(a, b Conn) {
	done := make(chan struct{}, 2)
	cp := func(dst, src Conn) {
		for {
			t, p, err := src.Recv()
			if err != nil || dst.Send(t, p) != nil {
				break
			}
		}
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	<-done
	_ = a.Close()
	_ = b.Close()
	<-done
}

// Splice connects a message stream to a TCP connection: Data<->bytes, with
// half-close propagated both ways.
func Splice(s Conn, c net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // tcp -> stream
		defer wg.Done()
		buf := make([]byte, chunk)
		for {
			n, err := c.Read(buf)
			if n > 0 && s.Send(Data, buf[:n]) != nil {
				break
			}
			if err != nil {
				_ = s.Send(Fin, nil)
				break
			}
		}
	}()
	go func() { // stream -> tcp
		defer wg.Done()
		for {
			t, p, err := s.Recv()
			if err != nil {
				_ = c.Close()
				return
			}
			switch t {
			case Data:
				if _, err := c.Write(p); err != nil {
					_ = c.Close()
					return
				}
			case Fin:
				if hc, ok := c.(interface{ CloseWrite() error }); ok {
					_ = hc.CloseWrite()
				} else {
					_ = c.Close()
				}
				return
			case Err:
				_ = c.Close()
				return
			}
		}
	}()
	wg.Wait()
	_ = c.Close()
	_ = s.Close()
}

// ExitFrame payload helpers.
func ExitPayload(code int) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(int32(code)))
	return b
}

func ParseExit(p []byte) int {
	if len(p) < 4 {
		return 255
	}
	return int(int32(binary.BigEndian.Uint32(p)))
}
