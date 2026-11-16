package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/term"

	"wosh/internal/wire"
)

// Delegate implements `wosh host` run inside an existing wosh session
// (client A -wosh-> B, then on B: `wosh C`).
//
// Instead of becoming a second client on B, it starts the remote side on C
// over ssh from B (C may only be reachable from B), then hands C's hello to
// the client on A through B's terminal - tagged with B's session token so A
// accepts without asking. A connects to C directly; B's session idles in the
// background and this process stays parked in B's terminal as the signaling
// path for C's reconnects. When C's session ends this process exits and you
// are back on B.
func Delegate(o Options) (int, error) {
	o.setDefaults()
	c := &Client{o: o, errOut: o.Err, finished: make(chan struct{}), ttyFd: -1}
	// Raw (no echo) from the start: keystrokes typed while we set up are
	// not echoed here but forwarded to the client as typeahead for the new
	// session. ssh still prompts fine (it drives /dev/tty itself); we do
	// not read stdin until it is done.
	if f, ok := o.In.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		if st, err := term.MakeRaw(int(f.Fd())); err == nil {
			defer term.Restore(int(f.Fd()), st)
		}
	}
	here, _ := os.Hostname()
	fmt.Fprintf(o.Err, "wosh: connecting your client directly to %s (via %s)...\r\n", strings.Join(o.SSHArgs, " "), here)
	w, hello, err := c.startRemote([]string{"server"})
	if err != nil {
		return 255, err
	}
	id := hello.ID

	hello.Token = os.Getenv("WOSH_TOKEN")
	hello.Via = here
	hello.Hop = o.SSHArgs // so the client can reach the target without us
	if _, err := o.Out.Write(wire.EncodeOSC(hello)); err != nil {
		return 255, err
	}

	type inEv struct {
		msgs  []wire.Msg
		other []byte
		err   error
	}
	inCh := make(chan inEv, 16)
	var sp wire.InputSplitter
	var spMu sync.Mutex
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := o.In.Read(buf)
			spMu.Lock()
			msgs, other := sp.Feed(buf[:n])
			spMu.Unlock()
			if len(msgs) > 0 || len(other) > 0 {
				inCh <- inEv{msgs: msgs, other: other}
			}
			if err != nil {
				inCh <- inEv{err: io.EOF}
				return
			}
		}
	}()
	typeahead := func(b []byte) {
		if len(b) > 0 {
			_, _ = o.Out.Write(wire.EncodeOSC(wire.Msg{T: wire.TTypeahead, ID: id, Text: base64.StdEncoding.EncodeToString(b)}))
		}
	}

	connected := false
	firstWait := time.After(30 * time.Second)
	msgs := w.msgs
	for {
		select {
		case <-firstWait:
			if !connected {
				_ = w.send(wire.Msg{T: wire.TBye})
				w.close()
				fmt.Fprint(o.Out, "\r\nwosh: no wosh client answered (is this terminal inside wosh?)\r\n")
				return 1, nil
			}
		case ev := <-inCh:
			if ev.err != nil {
				if !connected && w != nil {
					_ = w.send(wire.Msg{T: wire.TBye})
				}
				if w != nil {
					w.close()
				}
				return 0, nil
			}
			if !connected && bytes.IndexByte(ev.other, 0x03) >= 0 { // ^C before the hop is up: abort
				if w != nil {
					_ = w.send(wire.Msg{T: wire.TBye})
					w.close()
				}
				return 130, nil
			}
			// anything typed here belongs to the new session
			typeahead(ev.other)
			for _, m := range ev.msgs {
				if m.ID != "" && m.ID != id {
					continue
				}
				switch m.T {
				case wire.TBye:
					if w != nil {
						if !connected {
							_ = w.send(wire.Msg{T: wire.TBye})
						}
						w.close()
					}
					return 0, nil
				case wire.TOffer:
					ans := c.relayOffer(&w, hello, m)
					msgs = nil
					if w != nil {
						msgs = w.msgs
					}
					_, _ = o.Out.Write(wire.EncodeOSC(ans))
					if ans.T == wire.TAnswer {
						connected = true
					}
				}
			}
		case <-time.After(150 * time.Millisecond):
			// a lone "W", "WO"... typed last is held as a possible prefix
			spMu.Lock()
			held := sp.FlushPartial()
			spMu.Unlock()
			typeahead(held)
		case _, ok := <-msgs:
			if !ok { // ssh to the target dropped; reattach on the next offer
				w.close()
				w, msgs = nil, nil
				if !connected {
					return 1, errors.New("lost connection to " + hello.Host)
				}
			}
			// the client tells us when the target session is over (TBye on stdin)
		}
	}
}

// relayOffer forwards an offer to the target and returns its answer,
// reattaching over ssh if the original connection is gone.
func (c *Client) relayOffer(w **pipeWire, hello wire.Msg, offer wire.Msg) wire.Msg {
	fail := func(err error) wire.Msg { return wire.Msg{T: wire.TError, ID: hello.ID, Err: err.Error()} }
	if *w == nil {
		pc := c.woshCmd(false, "attach", hello.ID)
		if c.o.Push && hello.Sock != "" {
			pc = pipeCmd{remote: perlCommand(pushAttachStub, hello.Sock, hello.ID)}
		}
		nw, err := c.startPipe(pc)
		if err != nil {
			return fail(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), sigTimeout)
		h, err := nw.recv(ctx, hello.ID)
		cancel()
		if err != nil || h.T != wire.THello {
			nw.close()
			if err == nil {
				err = fmt.Errorf("remote: %s", h.Err)
			}
			return fail(err)
		}
		*w = nw
	}
	if err := (*w).send(offer); err != nil {
		(*w).close()
		*w = nil
		return fail(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), sigTimeout)
	defer cancel()
	for {
		ans, err := (*w).recv(ctx, hello.ID)
		if err != nil {
			(*w).close()
			*w = nil
			return fail(err)
		}
		if ans.T == wire.TAnswer || ans.T == wire.TError {
			return ans
		}
	}
}
