package server

import (
	"net"
	"runtime"
	"testing"
	"time"
)

// A closed control connection must not leave a goroutine behind until the
// daemon exits: callers that poll the socket (status, tail) would grow it
// without bound in a long-lived daemon.
func TestControlConnNoGoroutineLeak(t *testing.T) {
	d := &Daemon{conns: map[net.Conn]chan struct{}{}, quit: make(chan struct{})}
	defer d.stop()

	settle := func() int {
		var n int
		for i := 0; i < 50; i++ {
			runtime.GC()
			time.Sleep(10 * time.Millisecond)
			n = runtime.NumGoroutine()
			d.mu.Lock()
			open := len(d.conns)
			d.mu.Unlock()
			if open == 0 {
				break
			}
		}
		return n
	}

	before := settle()
	const conns = 100
	for i := 0; i < conns; i++ {
		srv, cli := net.Pipe()
		done := make(chan struct{})
		go func() { d.serveConn(srv); close(done) }()
		cli.Close()
		<-done
	}
	after := settle()
	if after-before > conns/10 {
		t.Fatalf("goroutines grew from %d to %d after %d closed control connections", before, after, conns)
	}
}
