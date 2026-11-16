package client

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/proxy"

	"wosh/internal/wire"
)

// echoServer accepts connections and echoes with a prefix.
func echoServer(t *testing.T, prefix string) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 1024)
				n, _ := c.Read(buf)
				_, _ = c.Write(append([]byte(prefix), buf[:n]...))
			}()
		}
	}()
	return l.Addr().(*net.TCPAddr).Port
}

// echoAll echoes everything until EOF (long-lived connections).
func echoAll(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	return l.Addr().(*net.TCPAddr).Port
}

func echoLine(t *testing.T, c net.Conn, msg string) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != msg {
		t.Fatalf("echo %q: got %q %v", msg, buf, err)
	}
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func roundTrip(c net.Conn, msg string) (string, error) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		return "", err
	}
	b, err := io.ReadAll(c)
	return string(b), err
}

// TestFeatures: env + PATH, -L / -R / -D forwards, src: transfers (rsync,
// scp, sftp) and the chained hop (`wosh host` inside a session).
func TestFeatures(t *testing.T) {
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
	t.Setenv("HOME", tmp) // src: serves the client's home
	t.Setenv("PATH", "/usr/bin:/bin")

	remoteEcho := echoServer(t, "remote:")
	longEcho := echoAll(t)
	// the chained hop reconnects by ssh'ing through the intermediate host;
	// stand in for ssh: run the remote command locally
	fake := filepath.Join(tmp, "fakessh")
	_ = os.WriteFile(fake, []byte("#!/bin/sh\nfor a; do last=$a; done\nexec sh -c \"$last\"\n"), 0o755)
	t.Setenv("WOSH_HOP_SSH", fake)
	clientEcho := echoServer(t, "client:")
	lPort, rPort, dPort, l2Port := freePort(t), freePort(t), freePort(t), freePort(t)
	fl, _ := ParseForward('L', fmt.Sprintf("127.0.0.1:%d:127.0.0.1:%d", lPort, remoteEcho))
	fl2, _ := ParseForward('L', fmt.Sprintf("127.0.0.1:%d:127.0.0.1:%d", l2Port, longEcho))
	fr, _ := ParseForward('R', fmt.Sprintf("127.0.0.1:%d:127.0.0.1:%d", rPort, clientEcho))
	fd, _ := ParseForward('D', fmt.Sprintf("127.0.0.1:%d", dPort))

	// throwaway ssh-agent for -A
	agentSock := filepath.Join(tmp, "agent.sock")
	agent := exec.Command("ssh-agent", "-D", "-a", agentSock)
	if err := agent.Start(); err == nil {
		t.Cleanup(func() { _ = agent.Process.Kill() })
		waitCond(t, "ssh-agent", 5*time.Second, func() bool { _, err := os.Stat(agentSock); return err == nil })
		t.Setenv("SSH_AUTH_SOCK", agentSock)
	}

	inR, inW := io.Pipe()
	out := &syncBuf{}
	c, err := Start(Options{
		Agent: true,
		Local: bin, In: inR, Out: out, Err: out, Auto: true, AllowSrc: true,
		ICE: []wire.ICEServer{}, Term: "dumb",
		Env:      map[string]string{"WOSH_TEST_VAR": "hello-env"},
		Forwards: []Forward{fl, fr, fd, fl2},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.begin()
	defer func() {
		if t.Failed() {
			logs, _ := filepath.Glob(filepath.Join(rt, "*.log"))
			for _, l := range logs {
				b, _ := os.ReadFile(l)
				t.Logf("%s:\n%s", l, b)
			}
		}
	}()
	typ := func(s string) { _, _ = inW.Write([]byte(s + "\r")) }
	root := c.top()

	t.Run("env and PATH", func(t *testing.T) {
		typ(`echo "v=[$WOSH_TEST_VAR]"; command -v wosh; command -v ssh; wosh version | cut -c1-4`)
		waitFor(t, out, "v=[hello-env]", 20*time.Second)
		waitFor(t, out, ".d/bin/wosh", 10*time.Second)
		waitFor(t, out, ".d/bin/ssh", 10*time.Second)
		waitFor(t, out, "\r\nwosh\r\n", 10*time.Second)
	})

	t.Run("fix-shell", func(t *testing.T) {
		// rc that rebuilds PATH (like many fish/zsh setups)
		_ = os.WriteFile(filepath.Join(tmp, ".profile"), []byte("PATH=/usr/bin:/bin; export PATH\n"), 0o644)
		typ(`wosh --fix-shell=sh`)
		waitFor(t, out, "reloading your shell", 20*time.Second)
		typ(`echo "first=${PATH%%:*}"`)
		waitFor(t, out, "first="+filepath.Join(rt, root.id+".d", "bin"), 20*time.Second)
		b, _ := os.ReadFile(filepath.Join(tmp, ".profile"))
		if !strings.Contains(string(b), "wosh --fix-shell") {
			t.Fatal("block not written")
		}
		if _, err := os.Stat(filepath.Join(tmp, ".profile.wosh.bak")); err != nil {
			t.Fatal("no backup")
		}
	})

	t.Run("-A agent", func(t *testing.T) {
		if os.Getenv("SSH_AUTH_SOCK") != agentSock {
			t.Skip("no ssh-agent")
		}
		// exit 1 = connected to an agent with no keys (2 = no agent)
		typ(`ssh-add -l >/dev/null 2>&1; echo "AGENT-$?-${SSH_AUTH_SOCK##*/}"`)
		waitFor(t, out, "AGENT-1-agent.sock", 20*time.Second)
	})

	t.Run("-L", func(t *testing.T) {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", lPort))
		if err != nil {
			t.Fatal(err)
		}
		if got, err := roundTrip(conn, "ping-L"); got != "remote:ping-L" {
			t.Fatalf("got %q %v", got, err)
		}
	})

	t.Run("-R", func(t *testing.T) {
		var conn net.Conn
		waitCond(t, "remote listener", 10*time.Second, func() bool {
			conn, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", rPort))
			return err == nil
		})
		if got, err := roundTrip(conn, "ping-R"); got != "client:ping-R" {
			t.Fatalf("got %q %v", got, err)
		}
	})

	t.Run("-D", func(t *testing.T) {
		d, err := proxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", dPort), nil, proxy.Direct)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := d.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", remoteEcho))
		if err != nil {
			t.Fatal(err)
		}
		if got, err := roundTrip(conn, "ping-D"); got != "remote:ping-D" {
			t.Fatalf("got %q %v", got, err)
		}
	})

	t.Run("forwarded connection survives reconnect", func(t *testing.T) {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", l2Port))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		echoLine(t, conn, "before-roam")
		root.markDead(0, "test roam")
		big := strings.Repeat("x", 200*1024) // sent while the link is down
		echoLine(t, conn, big)
		waitCond(t, "root reconnect", 30*time.Second, root.isAlive)
		echoLine(t, conn, "after-roam")
	})

	t.Run("src: rsync scp sftp", func(t *testing.T) {
		// "client" side files live in $HOME (tmp); the session works in $HOME too,
		// so use distinct dirs
		_ = os.MkdirAll(filepath.Join(tmp, "clientdir"), 0o755)
		big := strings.Repeat("0123456789abcdef", 64*1024) // 1 MiB
		_ = os.WriteFile(filepath.Join(tmp, "clientdir", "a.txt"), []byte("from-client\n"), 0o644)
		_ = os.WriteFile(filepath.Join(tmp, "clientdir", "big.bin"), []byte(big), 0o644)
		_ = os.WriteFile(filepath.Join(tmp, "up.txt"), []byte("from-remote\n"), 0o644)
		typ(`rsync -a src:clientdir/ pulled/ && echo RSYNC-PULL-$?`)
		waitFor(t, out, "RSYNC-PULL-0", 30*time.Second)
		if b, _ := os.ReadFile(filepath.Join(tmp, "pulled", "big.bin")); string(b) != big {
			t.Fatal("rsync pull content mismatch")
		}
		typ(`scp up.txt src:clientdir/up-scp.txt && echo SCP-PUSH-$?`)
		waitFor(t, out, "SCP-PUSH-0", 30*time.Second)
		if b, _ := os.ReadFile(filepath.Join(tmp, "clientdir", "up-scp.txt")); string(b) != "from-remote\n" {
			t.Fatal("scp push content mismatch")
		}
		typ(`echo "get clientdir/a.txt sftp-got.txt" | sftp -q -b - src >/dev/null && cat sftp-got.txt && echo SFTP-$?`)
		waitFor(t, out, "SFTP-0", 30*time.Second)
		// not a transfer: never run over the wosh channel; falls back to real
		// ssh into this machine's sshd (strict, throwaway known_hosts: fails at
		// host key verification, which proves the tunnel reached sshd)
		typ(`ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/dev/null src 'echo pwned' ; echo SSH-FALLBACK-$?`)
		waitFor(t, out, "SSH-FALLBACK-", 30*time.Second)
		o := out.String()
		if strings.Contains(o, "\npwned") || strings.Contains(o, "SSH-FALLBACK-0") {
			t.Fatal("non-transfer command ran via src:")
		}
		if _, err := net.DialTimeout("tcp", "127.0.0.1:22", time.Second); err == nil && !strings.Contains(o, "Host key verification failed") {
			t.Fatal("fallback did not reach the local sshd")
		}
	})

	t.Run("chained hop", func(t *testing.T) {
		typ("wosh --local=" + bin + " ignored-host")
		typ(`echo "early-$WOSH_SESSION"`) // typed before the hop exists: must run on the new host
		deadline := time.Now().Add(30 * time.Second)
		for c.top() == root && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
		}
		if c.top() == root {
			o := out.String()
			t.Fatalf("no chained child; output tail:\n%s", o[max(0, len(o)-1500):])
		}
		child := c.top()
		if !child.delegated || len(child.route) != 2 {
			t.Fatalf("child not a routed hop: %+v", child.route)
		}
		waitFor(t, out, "early-"+child.id, 20*time.Second)
		if strings.Contains(out.String(), "early-"+root.id) {
			t.Fatal("typeahead ran on the intermediate host")
		}
		typ(`echo "hop-$WOSH_SESSION"`)
		waitFor(t, out, "hop-"+child.id, 20*time.Second)

		// the intermediate session spins down
		select {
		case <-root.done:
		case <-time.After(15 * time.Second):
			t.Fatal("intermediate session still running")
		}
		// forwards now go to the new destination
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", l2Port))
		if err != nil {
			t.Fatal(err)
		}
		echoLine(t, conn, "via-hop")
		_ = conn.Close()
		var rc net.Conn
		waitCond(t, "-R on new destination", 15*time.Second, func() bool {
			rc, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", rPort))
			return err == nil
		})
		if got, err := roundTrip(rc, "ping-R2"); got != "client:ping-R2" {
			t.Fatalf("-R after hop: %q %v", got, err)
		}

		// reconnect without the intermediate session: ssh through the route
		child.markDead(0, "test")
		waitCond(t, "routed reconnect", 30*time.Second, child.isAlive)
		typ(`echo "still-$WOSH_SESSION"`)
		waitFor(t, out, "still-"+child.id, 20*time.Second)
	})

	typ("exit")
	done := make(chan int)
	go func() { done <- c.Wait() }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("client did not finish")
	}
}
