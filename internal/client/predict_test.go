package client

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charmbracelet/x/vt"

	"wosh/internal/predict"
	"wosh/internal/wire"
)

type vtWriter struct {
	mu sync.Mutex
	vt *vt.Emulator
}

func (w *vtWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.vt.Write(p)
}

func (w *vtWriter) screen() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var b strings.Builder
	for y := 0; y < w.vt.Height(); y++ {
		for x := 0; x < w.vt.Width(); x++ {
			if c := w.vt.CellAt(x, y); c != nil && c.Content != "" {
				b.WriteString(c.Content)
			} else if c != nil && c.Width != 0 {
				b.WriteByte(' ')
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// TestPredictionEndToEnd: with prediction forced on, what the user's
// terminal ends up showing must equal plain output - predictions may never
// leave duplicates or garbage behind.
func TestPredictionEndToEnd(t *testing.T) {
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
	t.Setenv("WOSH_RUNTIME_DIR", filepath.Join(tmp, "rt"))
	t.Setenv("SHELL", "/bin/sh")
	t.Setenv("ENV", "")
	t.Setenv("PS1", "$ ")
	t.Setenv("HOME", tmp)
	_ = os.WriteFile(filepath.Join(tmp, ".profile"), nil, 0o644)

	emu := vt.NewEmulator(80, 24)
	go func() { _, _ = io.Copy(io.Discard, emu) }()
	screen := &vtWriter{vt: emu}
	inR, inW := io.Pipe()
	c, err := Start(Options{Local: bin, In: inR, Out: screen, Err: io.Discard, ICE: []wire.ICEServer{},
		Term: "xterm", Predict: predict.Always})
	if err != nil {
		t.Fatal(err)
	}
	c.begin()
	slowType := func(s string) {
		for _, r := range s {
			_, _ = inW.Write([]byte(string(r)))
			time.Sleep(15 * time.Millisecond)
		}
	}
	waitScreen := func(want string) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if strings.Contains(screen.screen(), want) {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("screen never showed %q:\n%s", want, screen.screen())
	}
	waitScreen("$ ")
	slowType("echo predicted-text\r")
	waitScreen("\npredicted-text")
	slowType("echo abcdef\x7f\x7fXY\r") // with backspaces
	waitScreen("\nabcdXY")
	slowType("echo 日本語 wide\r")
	waitScreen("\n日本語 wide")
	time.Sleep(300 * time.Millisecond)
	var lines []string
	for _, l := range strings.Split(screen.screen(), "\n") {
		lines = append(lines, strings.TrimRight(l, " "))
	}
	got := strings.Join(lines, "\n")
	for _, want := range []string{"$ echo predicted-text\n", "$ echo abcdXY\n", "$ echo 日本語 wide\n"} {
		if strings.Count(got, want) != 1 {
			t.Fatalf("expected exactly one %q on screen:\n%s", want, got)
		}
	}
	_, _ = inW.Write([]byte("exit\r"))
	done := make(chan struct{})
	go func() { c.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("client did not finish")
	}
}
