package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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
	here, _ := os.Hostname()
	fmt.Fprintf(o.Err, "wosh: connecting your client directly to %s (via %s)...\n", strings.Join(o.SSHArgs, " "), here)
	w, hello, err := c.startRemote([]string{"server"})
	if err != nil {
		return 255, err
	}
	id := hello.ID

	if f, ok := o.In.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		if st, err := term.MakeRaw(int(f.Fd())); err == nil {
			defer term.Restore(int(f.Fd()), st)
		}
	}
	hello.Token = os.Getenv("WOSH_TOKEN")
	hello.Via, _ = os.Hostname()
	hello.Hop = o.SSHArgs // so the client can reach the target without us
	if _, err := o.Out.Write(wire.EncodeOSC(hello)); err != nil {
		return 255, err
	}

	type inEv struct {
		m   wire.Msg
		err error
	}
	inCh := make(chan inEv)
	go func() {
		var lr wire.LineReader
		buf := make([]byte, 4096)
		for {
			n, err := o.In.Read(buf)
			lines, lerr := lr.Feed(buf[:n])
			for _, l := range lines {
				if m, ok, _ := wire.ParseInputLine(l); ok {
					inCh <- inEv{m: m}
				}
			}
			if lerr != nil || err != nil {
				inCh <- inEv{err: errors.Join(lerr, err, io.EOF)}
				return
			}
		}
	}()

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
			if ev.m.ID != "" && ev.m.ID != id {
				continue
			}
			switch ev.m.T {
			case wire.TBye:
				if w != nil {
					if !connected {
						_ = w.send(wire.Msg{T: wire.TBye})
					}
					w.close()
				}
				return 0, nil
			case wire.TOffer:
				ans := c.relayOffer(&w, hello, ev.m)
				msgs = nil
				if w != nil {
					msgs = w.msgs
				}
				_, _ = o.Out.Write(wire.EncodeOSC(ans))
				if ans.T == wire.TAnswer {
					connected = true
				}
			}
		case m, ok := <-msgs:
			if !ok { // ssh to the target dropped; reattach on the next offer
				w.close()
				w, msgs = nil, nil
				if !connected {
					return 1, errors.New("lost connection to " + hello.Host)
				}
				continue
			}
			_ = m // the client tells us when the target session is over (TBye on stdin)
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
