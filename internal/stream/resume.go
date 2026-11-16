package stream

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"

	"wosh/internal/wire"
)

// Resumable streams survive reconnects between the same two endpoints.
//
// Every message carries a sequence number; the receiver acknowledges what it
// has. When the peer connection is replaced (roaming), the data channel dies
// but the stream does not: the side that opened it reopens a channel on the
// new connection (label carries SID and the next sequence it expects), the
// other side re-attaches by SID, and both resend whatever was not
// acknowledged. A stream that stays detached for SuspendFor is closed.
//
// On the data channel: [type][u64 seq][payload] for stream messages,
// [ack][u64 next expected] for acknowledgements, [closeMsg][u64 seq] for an
// orderly close.

const (
	ackMsg    = 'K'
	closeMsg  = 'C'
	refuseMsg = 'Z'

	maxUnacked = 8 << 20
	ackEvery   = 256 << 10
)

// SuspendFor is how long a detached stream waits to be resumed.
var SuspendFor = 2 * time.Minute

type seqMsg struct {
	seq     uint64
	typ     byte
	payload []byte
}

// Resumable implements Conn over a sequence of data channels.
type Resumable struct {
	ID     string
	Spec   wire.StreamSpec
	opener bool

	mu       sync.Mutex
	cond     *sync.Cond
	ch       *Chan
	gen      int
	sendMu   sync.Mutex // orders transmissions
	nextSeq  uint64
	unacked  []seqMsg
	unackedN int
	rxNext   uint64
	rxBytes  int // since last ack
	inbox    []seqMsg
	peerDone bool // peer closed (orderly)
	done     bool // fully closed / dead
	resend   bool // opener: resend once the peer tells us where it is
	timer    *time.Timer
	onDone   func()
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func newResumable(spec wire.StreamSpec, opener bool) *Resumable {
	r := &Resumable{ID: spec.SID, Spec: spec, opener: opener}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// OpenResumable opens a new resumable stream on pc.
func OpenResumable(pc *webrtc.PeerConnection, spec wire.StreamSpec) (*Resumable, error) {
	spec.SID, spec.RX, spec.Resume = newID(), 0, false
	ch, err := Open(pc, spec)
	if err != nil {
		return nil, err
	}
	r := newResumable(spec, true)
	r.attach(ch, 0, false)
	return r, nil
}

// Reopen re-attaches an opener's stream over a new peer connection.
func (r *Resumable) Reopen(pc *webrtc.PeerConnection) error {
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return nil
	}
	spec := r.Spec
	spec.RX, spec.Resume = r.rxNext, true
	r.mu.Unlock()
	ch, err := Open(pc, spec)
	if err != nil {
		return err
	}
	r.attach(ch, 0, true)
	return nil
}

// Accept handles an incoming stream channel: a new stream (returned with
// isNew) or a resumption of a known one (found via lookup).
func Accept(dc *webrtc.DataChannel, spec wire.StreamSpec, lookup func(sid string) *Resumable) (r *Resumable, isNew bool) {
	ch := Wrap(dc)
	if spec.Resume {
		if r = lookup(spec.SID); r == nil {
			_ = ch.Send(refuseMsg, []byte("unknown stream"))
			_ = ch.Close()
			return nil, false
		}
		// not inside the data channel callback: sends wait for "open",
		// which is only signalled after the callback returns
		go func() {
			r.attach(ch, spec.RX, false)
			r.sendAck()
		}()
		return r, false
	}
	if spec.SID == "" {
		spec.SID = newID()
	}
	r = newResumable(spec, false)
	r.attach(ch, 0, false)
	return r, true
}

// OnDone registers a callback for when the stream is finished (registry cleanup).
func (r *Resumable) OnDone(f func()) {
	r.mu.Lock()
	r.onDone = f
	r.mu.Unlock()
}

// Detached reports whether the stream currently has no transport.
func (r *Resumable) Detached() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ch == nil && !r.done
}

// attach makes ch the transport. peerRX: next seq the peer expects (if
// known); waitAck: resend after the peer's first ack instead.
func (r *Resumable) attach(ch *Chan, peerRX uint64, waitAck bool) {
	r.mu.Lock()
	old := r.ch
	r.ch = ch
	r.gen++
	gen := r.gen
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	r.resend = waitAck
	r.mu.Unlock()
	if old != nil && old != ch {
		old.kill()
	}
	if !waitAck {
		r.ackUpTo(peerRX)
		r.resendUnacked()
	}
	go r.reader(ch, gen)
}

func (r *Resumable) reader(ch *Chan, gen int) {
	for {
		t, p, err := ch.Recv()
		if err != nil {
			r.detach(gen)
			return
		}
		switch t {
		case ackMsg:
			if len(p) >= 8 {
				r.ackUpTo(binary.BigEndian.Uint64(p))
			}
			r.mu.Lock()
			again := r.resend && r.gen == gen
			r.resend = false
			r.mu.Unlock()
			if again {
				r.resendUnacked()
			}
		case refuseMsg: // transport-level refusal (unknown stream on resume)
			r.deliverLocal(seqMsg{typ: Err, payload: p})
			r.finish()
			return
		default:
			if len(p) < 8 {
				continue
			}
			seq := binary.BigEndian.Uint64(p)
			r.mu.Lock()
			if seq != r.rxNext {
				r.mu.Unlock()
				continue // duplicate after a resume
			}
			r.rxNext++
			r.rxBytes += len(p)
			ackNow := r.rxBytes >= ackEvery || t == closeMsg
			if t == closeMsg {
				r.peerDone = true
			} else {
				r.inbox = append(r.inbox, seqMsg{seq: seq, typ: t, payload: p[8:]})
			}
			r.mu.Unlock()
			r.cond.Broadcast()
			if ackNow {
				r.sendAck()
			}
		}
	}
}

func (r *Resumable) deliverLocal(m seqMsg) {
	r.mu.Lock()
	r.inbox = append(r.inbox, m)
	r.mu.Unlock()
	r.cond.Broadcast()
}

// detach: the transport died. Wait to be resumed (the opener reopens).
func (r *Resumable) detach(gen int) {
	r.mu.Lock()
	if r.gen != gen || r.done {
		r.mu.Unlock()
		return
	}
	r.ch = nil
	if r.peerDone && len(r.unacked) == 0 {
		r.mu.Unlock()
		r.finish()
		return
	}
	r.timer = time.AfterFunc(SuspendFor, func() {
		log.Printf("stream %s: not resumed, closing", r.ID)
		r.finish()
	})
	r.mu.Unlock()
}

func (r *Resumable) ackUpTo(n uint64) {
	r.mu.Lock()
	i := 0
	for i < len(r.unacked) && r.unacked[i].seq < n {
		r.unackedN -= len(r.unacked[i].payload)
		i++
	}
	r.unacked = r.unacked[i:]
	r.mu.Unlock()
	r.cond.Broadcast()
}

func (r *Resumable) sendAck() {
	r.mu.Lock()
	ch, n := r.ch, r.rxNext
	r.rxBytes = 0
	r.mu.Unlock()
	if ch == nil {
		return
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	_ = ch.Send(ackMsg, b) // unsequenced: needs no ordering with data
}

func (r *Resumable) resendUnacked() {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	r.mu.Lock()
	ch := r.ch
	msgs := append([]seqMsg(nil), r.unacked...)
	r.mu.Unlock()
	if ch == nil {
		return
	}
	for _, m := range msgs {
		if ch.Send(m.typ, frame(m.seq, m.payload)) != nil {
			return
		}
	}
}

func frame(seq uint64, p []byte) []byte {
	b := make([]byte, 8+len(p))
	binary.BigEndian.PutUint64(b, seq)
	copy(b[8:], p)
	return b
}

// Send queues a message; it is retransmitted after a resume if needed.
func (r *Resumable) Send(typ byte, p []byte) error {
	for {
		n := min(len(p), chunk-8) // + 8-byte seq header = one transport message
		if err := r.sendOne(typ, p[:n]); err != nil {
			return err
		}
		p = p[n:]
		if len(p) == 0 {
			return nil
		}
	}
}

func (r *Resumable) sendOne(typ byte, p []byte) error {
	// wait for window space first (not holding sendMu, so acks still flow)
	r.mu.Lock()
	for r.unackedN > maxUnacked && !r.done {
		r.cond.Wait()
	}
	r.mu.Unlock()
	// sequence assignment and transmission must happen in the same order
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return io.ErrClosedPipe
	}
	m := seqMsg{seq: r.nextSeq, typ: typ, payload: append([]byte(nil), p...)}
	r.nextSeq++
	r.unacked = append(r.unacked, m)
	r.unackedN += len(p)
	ch := r.ch
	r.mu.Unlock()
	if ch != nil {
		_ = ch.Send(typ, frame(m.seq, m.payload)) // failure = detach; resent on resume
	}
	return nil
}

// Recv returns the next message; io.EOF after an orderly close or when the
// stream could not be resumed.
func (r *Resumable) Recv() (byte, []byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.inbox) == 0 && !r.peerDone && !r.done {
		r.cond.Wait()
	}
	if len(r.inbox) == 0 {
		return 0, nil, io.EOF
	}
	m := r.inbox[0]
	r.inbox = r.inbox[1:]
	if r.rxBytes > 0 && len(r.inbox) == 0 {
		go r.sendAck()
	}
	return m.typ, m.payload, nil
}

// Close sends an orderly close and waits (briefly) for it to be delivered.
func (r *Resumable) Close() error {
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()
	_ = r.sendOne(closeMsg, nil)
	deadline := time.Now().Add(10 * time.Second)
	r.mu.Lock()
	for len(r.unacked) > 0 && !r.done && time.Now().Before(deadline) {
		r.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		r.mu.Lock()
	}
	r.mu.Unlock()
	r.finish()
	return nil
}

// Kill drops the stream without waiting (endpoint going away).
func (r *Resumable) Kill() { r.finish() }

func (r *Resumable) finish() {
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return
	}
	r.done = true
	ch, f := r.ch, r.onDone
	r.ch = nil
	if r.timer != nil {
		r.timer.Stop()
	}
	r.mu.Unlock()
	r.cond.Broadcast()
	if ch != nil {
		go ch.Close()
	}
	if f != nil {
		f()
	}
}

// Registry tracks an endpoint's resumable streams.
type Registry struct {
	mu sync.Mutex
	m  map[string]*Resumable
}

func NewRegistry() *Registry { return &Registry{m: map[string]*Resumable{}} }

func (g *Registry) Add(r *Resumable) {
	g.mu.Lock()
	g.m[r.ID] = r
	g.mu.Unlock()
	r.OnDone(func() { g.mu.Lock(); delete(g.m, r.ID); g.mu.Unlock() })
}

func (g *Registry) Get(id string) *Resumable {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.m[id]
}

// ReopenAll re-attaches every stream this side opened over a new connection.
func (g *Registry) ReopenAll(pc *webrtc.PeerConnection) {
	g.mu.Lock()
	var rs []*Resumable
	for _, r := range g.m {
		if r.opener {
			rs = append(rs, r)
		}
	}
	g.mu.Unlock()
	for _, r := range rs {
		go func(r *Resumable) {
			if err := r.Reopen(pc); err != nil {
				log.Printf("stream %s: reopen: %v", r.ID, err)
			}
		}(r)
	}
}

// KillAll drops every stream (endpoint gone).
func (g *Registry) KillAll() {
	g.mu.Lock()
	var rs []*Resumable
	for _, r := range g.m {
		rs = append(rs, r)
	}
	g.mu.Unlock()
	for _, r := range rs {
		r.Kill()
	}
}

// ErrUnknown is returned for refused streams.
var ErrUnknown = errors.New("stream refused")
