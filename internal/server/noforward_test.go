package server

import (
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"wosh/internal/stream"
	"wosh/internal/wire"
)

// TestNoForward: with --no-forward a peer's -L stream is refused and never
// dialed, -R listeners are not opened, and local helpers cannot open
// streams to the client. Without the flag the same requests work, which
// shows the test exercises the real paths.
func TestNoForward(t *testing.T) {
	for _, noFwd := range []bool{true, false} {
		t.Run(fmt.Sprintf("no-forward=%v", noFwd), func(t *testing.T) {
			var args []string
			if noFwd {
				args = []string{"--no-forward"}
			}
			d := startDaemon(t, args...)
			a := d.connect("aaaa", false)

			// -L: the daemon would dial 127.0.0.1:<port>
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			var dialed atomic.Int32
			go func() {
				for {
					c, err := l.Accept()
					if err != nil {
						return
					}
					dialed.Add(1)
					_ = c.Close()
				}
			}()
			s, err := stream.OpenResumable(a.pc, wire.StreamSpec{K: "tcp", Host: "127.0.0.1", Port: l.Addr().(*net.TCPAddr).Port})
			if err != nil {
				t.Fatal(err)
			}
			typ, msg, _ := s.Recv()
			time.Sleep(300 * time.Millisecond)
			if noFwd {
				if typ != stream.Err || !strings.Contains(string(msg), "disabled") || dialed.Load() != 0 {
					t.Fatalf("-L not refused: %q %q, dialed %d", typ, msg, dialed.Load())
				}
			} else if dialed.Load() == 0 {
				t.Fatal("-L: daemon did not dial (test setup broken)")
			}

			// -R: the controller asks the daemon to listen
			pl, _ := net.Listen("tcp", "127.0.0.1:0")
			port := pl.Addr().(*net.TCPAddr).Port
			_ = pl.Close()
			_ = a.dc.Send(wire.ControlFrame(wire.Control{Op: "rfwd", Rfwd: []wire.RemoteFwd{{ID: 1, Bind: "127.0.0.1", Port: port}}}))
			listening := func() bool {
				c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
				if err == nil {
					_ = c.Close()
				}
				return err == nil
			}
			if noFwd {
				time.Sleep(time.Second)
				if listening() {
					t.Fatal("-R listener opened with --no-forward")
				}
			} else {
				waitUntil(t, "-R listener", 10*time.Second, listening)
			}

			// local helper (ssh/scp shim) asking for a stream to the client
			if noFwd {
				r := d.ctl(wire.Msg{T: wire.TOpen, Stream: &wire.StreamSpec{K: "agent"}})
				if r.T != wire.TError || !strings.Contains(r.Err, "disabled") {
					t.Fatalf("open: %+v", r)
				}
			}

			// the terminal itself still works
			a.typ("echo term-$((5*5))\r")
			a.waitOutput("term-25", 10*time.Second)
		})
	}
}
