package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"

	"wosh/internal/rtc"
	"wosh/internal/wire"
)

const noSession = "no such session" // keep in sync with server.ErrNoSession

const (
	pingEvery   = time.Second
	deadAfter   = 6 * time.Second
	openTimeout = 25 * time.Second
	sigTimeout  = 45 * time.Second
)

// Session is one wosh connection to one remote daemon.
type Session struct {
	c      *Client
	id     string
	key    string
	host   string
	user   string
	parent *Session  // nil: reachable by ssh (root); else signaled through parent's terminal
	sock   string    // daemon control socket on the remote host
	path   string    // selected ICE candidate pair, for ~s
	anchor *pipeWire // bootstrap ssh kept open (macOS LAN permission), closed on finish

	mu       sync.Mutex
	gen      int
	pc       *webrtc.PeerConnection
	dc       *webrtc.DataChannel
	alive    bool
	aliveCh  chan struct{} // closed while alive
	lastRecv time.Time
	rxOut    uint64
	inBase   uint64 // offset of inBuf[0]
	inBuf    []byte // input not yet acknowledged by the daemon
	waiters  map[string]chan wire.Msg

	outMu  sync.Mutex // serializes output processing
	filter wire.OSCFilter
	shown  bool   // on the stack; before that, output is kept in early
	early  []byte // output received before the session was shown

	deadCh   chan struct{}
	done     chan struct{}
	doneOnce sync.Once
	exitCode int
}

func newSession(c *Client, id, host, user string, parent *Session) *Session {
	k := make([]byte, 24)
	_, _ = rand.Read(k)
	return &Session{
		c: c, id: id, host: host, user: user, parent: parent,
		key:     hex.EncodeToString(k),
		aliveCh: make(chan struct{}),
		waiters: map[string]chan wire.Msg{},
		deadCh:  make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
}

func (s *Session) name() string {
	if s.user != "" {
		return s.user + "@" + s.host
	}
	return s.host
}

func (s *Session) addWaiter(id string, ch chan wire.Msg) {
	s.mu.Lock()
	s.waiters[id] = ch
	s.mu.Unlock()
}

func (s *Session) removeWaiter(id string) {
	s.mu.Lock()
	delete(s.waiters, id)
	s.mu.Unlock()
}

func (s *Session) isAlive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alive
}

// waitAlive blocks until the session is connected, ended, or ctx expires.
func (s *Session) waitAlive(ctx context.Context) error {
	s.mu.Lock()
	ch := s.aliveCh
	s.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-s.done:
		return errors.New("session ended")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// negotiate builds a fresh peer connection and signals it over w.
func (s *Session) negotiate(w sigChan) error {
	s.mu.Lock()
	s.gen++
	gen := s.gen
	rx := s.rxOut
	s.mu.Unlock()

	pc, err := s.c.api.NewPeerConnection(rtc.Config(s.c.o.ICE, s.c.o.RelayOnly))
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = pc.Close()
		}
	}()
	dc, err := pc.CreateDataChannel("wosh", nil)
	if err != nil {
		return err
	}
	opened := make(chan struct{})
	dc.OnOpen(func() { close(opened) })
	dc.OnMessage(func(m webrtc.DataChannelMessage) { s.onFrame(gen, m.Data) })
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			log.Printf("[%s] gen %d local candidate %s", s.id, gen, c.String())
		}
	})
	pc.SCTP().Transport().ICETransport().OnSelectedCandidatePairChange(func(p *webrtc.ICECandidatePair) {
		log.Printf("[%s] gen %d selected pair %s", s.id, gen, p.String())
		s.mu.Lock()
		s.path = p.Local.Typ.String() + " " + p.Local.Address + " <-> " + p.Remote.Typ.String() + " " + p.Remote.Address
		s.mu.Unlock()
	})
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		log.Printf("[%s] gen %d state %s", s.id, gen, st)
		if st == webrtc.PeerConnectionStateFailed || st == webrtc.PeerConnectionStateClosed {
			s.markDead(gen, st.String())
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), sigTimeout)
	defer cancel()
	gctx, gcancel := context.WithTimeout(ctx, 10*time.Second)
	_ = rtc.WaitGathered(gctx, pc)
	gcancel()

	cols, rows := s.c.size()
	err = w.send(wire.Msg{
		T: wire.TOffer, ID: s.id, Key: s.key, SDP: pc.LocalDescription().SDP,
		ICE: s.c.o.ICE, Relay: s.c.o.RelayOnly, Term: s.c.o.Term,
		Cols: cols, Rows: rows, RxOut: rx,
	})
	if err != nil {
		return err
	}
	ans, err := w.recv(ctx, s.id)
	if err != nil {
		return err
	}
	if ans.T == wire.TError {
		return fmt.Errorf("remote: %s", ans.Err)
	}
	if ans.T != wire.TAnswer {
		return fmt.Errorf("unexpected %q message", ans.T)
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: ans.SDP}); err != nil {
		return err
	}
	select {
	case <-opened:
	case <-time.After(openTimeout):
		return errors.New("peer connection timed out (NAT? try --turn)")
	case <-s.done:
		return errors.New("session ended")
	}

	ok = true
	s.mu.Lock()
	old := s.pc
	s.pc, s.dc = pc, dc
	s.alive = true
	s.lastRecv = time.Now()
	select {
	case <-s.aliveCh:
	default:
		close(s.aliveCh)
	}
	// resend everything the daemon has not acknowledged; it dedupes by offset
	pending := append([]byte(nil), s.inBuf...)
	base := s.inBase
	s.mu.Unlock()
	if old != nil && old != pc {
		go old.Close()
	}
	for len(pending) > 0 {
		n := min(len(pending), wire.MaxChunk)
		_ = dc.Send(wire.OffsetFrame(wire.FInput, base, pending[:n]))
		base += uint64(n)
		pending = pending[n:]
	}
	_ = dc.Send(wire.ResizeFrame(cols, rows))
	log.Printf("[%s] connected gen %d", s.id, gen)
	return nil
}

// markDead drops the current connection so the maintainer reconnects.
func (s *Session) markDead(gen int, why string) {
	s.mu.Lock()
	if (gen != 0 && gen != s.gen) || !s.alive {
		s.mu.Unlock()
		return
	}
	s.alive = false
	s.aliveCh = make(chan struct{})
	pc := s.pc
	s.mu.Unlock()
	log.Printf("[%s] dead: %s", s.id, why)
	if pc != nil {
		go pc.Close()
	}
	select {
	case s.deadCh <- struct{}{}:
	default:
	}
	s.c.updateStatus()
}

func (s *Session) send(b []byte) {
	s.mu.Lock()
	dc, alive := s.dc, s.alive
	s.mu.Unlock()
	if alive && dc != nil {
		_ = dc.Send(b)
	}
}

// sendInput queues keystrokes; they are retained until acknowledged.
func (s *Session) sendInput(p []byte) {
	s.mu.Lock()
	off := s.inBase + uint64(len(s.inBuf))
	s.inBuf = append(s.inBuf, p...)
	dc, alive := s.dc, s.alive
	s.mu.Unlock()
	if !alive || dc == nil {
		return
	}
	for len(p) > 0 {
		n := min(len(p), wire.MaxChunk)
		_ = dc.Send(wire.OffsetFrame(wire.FInput, off, p[:n]))
		off += uint64(n)
		p = p[n:]
	}
}

func (s *Session) onFrame(gen int, b []byte) {
	if len(b) == 0 {
		return
	}
	s.mu.Lock()
	if gen != s.gen {
		s.mu.Unlock()
		return
	}
	s.lastRecv = time.Now()
	s.mu.Unlock()

	switch b[0] {
	case wire.FOutput:
		off, data, ok := wire.ParseOffset(b)
		if !ok {
			return
		}
		s.outMu.Lock()
		defer s.outMu.Unlock()
		s.mu.Lock()
		rx := s.rxOut
		if off+uint64(len(data)) <= rx {
			s.mu.Unlock()
			return
		}
		if off < rx {
			data = data[rx-off:]
		} else if off > rx {
			log.Printf("[%s] output gap %d bytes (ring overflow)", s.id, off-rx)
		}
		s.rxOut = off + uint64(len(b)-9)
		s.mu.Unlock()
		out, msgs := s.filter.Write(data)
		if len(out) > 0 {
			if !s.shown {
				if len(s.early) < 1<<20 {
					s.early = append(s.early, out...)
				}
			} else if s.c.isTop(s) {
				s.c.writeOut(out)
			}
		}
		for _, m := range msgs {
			s.onSignal(m)
		}
	case wire.FAck:
		off, _, ok := wire.ParseOffset(b)
		if !ok {
			return
		}
		s.mu.Lock()
		if off > s.inBase {
			drop := min(off-s.inBase, uint64(len(s.inBuf)))
			s.inBuf = s.inBuf[drop:]
			s.inBase += drop
		}
		s.mu.Unlock()
	case wire.FPong:
	case wire.FExit:
		code := wire.ParseExit(b)
		s.send([]byte{wire.FExitAck})
		go func() {
			// let the ack leave before we (possibly) exit the whole client
			s.mu.Lock()
			dc := s.dc
			s.mu.Unlock()
			deadline := time.Now().Add(time.Second)
			for dc != nil && dc.BufferedAmount() > 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(50 * time.Millisecond)
			s.finish(code)
		}()
	}
}

// onSignal handles a wosh message that arrived inside this session's output.
func (s *Session) onSignal(m wire.Msg) {
	switch m.T {
	case wire.THello:
		s.c.onHello(s, m)
	case wire.TAnswer, wire.TError:
		s.mu.Lock()
		ch := s.waiters[m.ID]
		s.mu.Unlock()
		if ch != nil {
			select {
			case ch <- m:
			default:
			}
		}
	}
}

func (s *Session) finish(code int) {
	s.doneOnce.Do(func() {
		s.mu.Lock()
		s.exitCode = code
		pc := s.pc
		s.alive = false
		s.mu.Unlock()
		close(s.done)
		log.Printf("[%s] exited %d", s.id, code)
		if s.anchor != nil {
			go s.anchor.close()
		}
		go func() {
			time.Sleep(300 * time.Millisecond)
			if pc != nil {
				_ = pc.Close()
			}
		}()
		s.c.remove(s)
	})
}

// heartbeat pings the daemon and declares the link dead on silence.
func (s *Session) heartbeat() {
	t := time.NewTicker(pingEvery)
	defer t.Stop()
	var n uint64
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		s.mu.Lock()
		alive, gen, last := s.alive, s.gen, s.lastRecv
		s.mu.Unlock()
		if !alive {
			continue
		}
		if time.Since(last) > deadAfter {
			s.markDead(gen, "ping timeout")
			continue
		}
		n++
		s.send(wire.U64Frame(wire.FPing, n))
	}
}

// maintain reconnects the session whenever it drops.
func (s *Session) maintain() {
	for {
		select {
		case <-s.done:
			return
		case <-s.deadCh:
		}
		backoff := 500 * time.Millisecond
		for !s.isAlive() {
			select {
			case <-s.done:
				return
			default:
			}
			err := s.reconnect()
			if err == nil {
				s.c.updateStatus()
				if s.c.isTop(s) && s.c.statusShown() {
					s.c.clearStatus()
					s.send([]byte{wire.FRedraw})
				}
				break
			}
			log.Printf("[%s] reconnect failed: %v", s.id, err)
			s.c.setStatusErr(s, err)
			select {
			case <-s.done:
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 10*time.Second)
		}
	}
}

func (s *Session) reconnect() error {
	if s.parent != nil {
		// signal through the parent's terminal, where `wosh up` is parked
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err := s.parent.waitAlive(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("parent %s: %w", s.parent.name(), err)
		}
		w := newParentWire(s.parent, s.id)
		defer w.close()
		return s.negotiate(w)
	}
	pc := s.c.woshCmd(false, "attach", s.id)
	if s.sock != "" && s.c.o.Push {
		// pushed binary: reattach with a perl relay, no upload needed
		pc = pipeCmd{remote: perlCommand(pushAttachStub, s.sock, s.id)}
	}
	w, err := s.c.startPipe(pc)
	if err != nil {
		return err
	}
	defer w.close()
	ctx, cancel := context.WithTimeout(context.Background(), sigTimeout)
	hello, err := w.recv(ctx, s.id)
	cancel()
	if err != nil {
		return err
	}
	if hello.T == wire.TError {
		if hello.Err == noSession {
			s.c.notice("session on %s is gone", s.name())
			go s.finish(255)
		}
		return fmt.Errorf("remote: %s", hello.Err)
	}
	if hello.T != wire.THello || hello.ID != s.id {
		return fmt.Errorf("unexpected %q from remote", hello.T)
	}
	return s.negotiate(w)
}
