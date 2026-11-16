package client

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"wosh/internal/wire"
)

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func waitFor(t *testing.T, buf *syncBuf, want string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %q; output:\n%s", want, buf.String())
}

func waitCond(t *testing.T, what string, d time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// TestEndToEnd drives a real client against real daemons on this machine:
// bootstrap, typing, in-band nested upgrade (`wosh up`), forced reconnects
// of root (via "ssh" attach) and child (via the parent's parked relay),
// and unwinding the session stack on exit.
func TestEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "wosh")
	build := exec.Command("go", "build", "-o", bin, "wosh")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	rt := filepath.Join(tmp, "rt")
	t.Setenv("WOSH_RUNTIME_DIR", rt)
	t.Setenv("WOSH_KEEP_LOGS", "1")
	t.Setenv("SHELL", "/bin/sh")
	t.Setenv("ENV", "")
	t.Setenv("PS1", "$ ")
	t.Setenv("PATH", tmp+":"+os.Getenv("PATH"))

	log.SetFlags(log.Lmicroseconds)
	inR, inW := io.Pipe()
	out := &syncBuf{}
	c, err := Start(Options{
		Local: bin, In: inR, Out: out, Err: out, Auto: true,
		ICE: []wire.ICEServer{}, Term: "dumb",
	})
	if err != nil {
		t.Fatal(err)
	}
	c.begin()
	defer func() {
		for _, f := range []string{"*.log"} {
			logs, _ := filepath.Glob(filepath.Join(rt, f))
			for _, l := range logs {
				if t.Failed() {
					b, _ := os.ReadFile(l)
					t.Logf("%s:\n%s", l, b)
				}
			}
		}
	}()
	typ := func(s string) { _, _ = inW.Write([]byte(s)) }

	root := c.top()
	typ("echo root-$((40+2))\r")
	waitFor(t, out, "root-42", 20*time.Second)

	// nested upgrade: run `wosh up` inside the root session
	typ("wosh up\r")
	waitCond(t, "child session", 30*time.Second, func() bool { return c.top() != root })
	child := c.top()
	if child.parent != root {
		t.Fatal("child parent mismatch")
	}
	typ("echo child-$WOSH_SESSION\r")
	waitFor(t, out, "child-"+child.id, 20*time.Second)

	// root drops: reconnect by re-running the bootstrap command ("attach")
	root.markDead(0, "test")
	waitCond(t, "root reconnect", 30*time.Second, root.isAlive)

	// child drops: reconnect through the parent's terminal
	child.markDead(0, "test")
	typ("echo typed-while-down\r") // must be retransmitted, not lost
	waitCond(t, "child reconnect", 30*time.Second, child.isAlive)
	waitFor(t, out, "typed-while-down\r\n", 20*time.Second)

	// lots of output survives a drop mid-stream
	typ("i=0; while [ $i -lt 3000 ]; do echo line-$i; i=$((i+1)); done; echo done-$((1+1))\r")
	time.Sleep(100 * time.Millisecond)
	child.markDead(0, "test")
	waitFor(t, out, "done-2", 30*time.Second)
	for _, n := range []int{0, 1500, 2999} {
		if !strings.Contains(out.String(), fmt.Sprintf("line-%d\r\n", n)) {
			t.Fatalf("missing line-%d", n)
		}
	}

	// exit child: back to root, where the parked `wosh up` has returned
	typ("exit\r")
	waitCond(t, "pop to root", 20*time.Second, func() bool { return c.top() == root })
	typ("echo back-$((2*3))\r")
	waitFor(t, out, "back-6", 20*time.Second)

	typ("exit 7\r")
	done := make(chan int)
	go func() { done <- c.Wait() }()
	select {
	case code := <-done:
		if code != 7 {
			t.Fatalf("exit code %d", code)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("client did not finish")
	}
}
