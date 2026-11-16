package client

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestPushStubCodecs runs the real perl push stub and sends it the perl
// binary as xz, gzip and raw payloads; the stub must decode and exec each.
func TestPushStubCodecs(t *testing.T) {
	perl, err := exec.LookPath("perl")
	if err != nil {
		t.Skip("no perl")
	}
	payloadBin, err := filepath.EvalSymlinks(perl)
	if err != nil {
		t.Fatal(err)
	}
	uname, _ := exec.LookPath("uname")
	xz, xzErr := exec.LookPath("xz")
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(uname, filepath.Join(bin, "uname")); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "payload")
	raw, err := os.ReadFile(payloadBin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, raw, 0o755); err != nil {
		t.Fatal(err)
	}

	run := func(codec string, payload []byte) (caps, out string) {
		t.Helper()
		cmd := exec.Command(perl, "-e", pushStub, "--", "-e", `print "hello wosh\n"`)
		cmd.Env = []string{"PATH=" + bin, "HOME=" + dir, "TMPDIR=" + dir}
		in, _ := cmd.StdinPipe()
		stdout, _ := cmd.StdoutPipe()
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReader(stdout)
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("%s: no handshake: %v %s", codec, err, stderr.String())
		}
		f := strings.Fields(line)
		if len(f) < 4 || f[0] != "WOSHPUSH" {
			t.Fatalf("handshake %q", line)
		}
		fmt.Fprintf(in, "MEM %d %s\n", len(payload), codec)
		_, _ = in.Write(payload)
		_ = in.Close()
		rest, _ := r.ReadString(0)
		if err := cmd.Wait(); err != nil {
			t.Fatalf("%s: %v: %s", codec, err, stderr.String())
		}
		return f[3], strings.TrimSpace(rest)
	}

	gz, err := compressFile(src)
	if err != nil {
		t.Fatal(err)
	}
	caps, out := run("g", gz)
	if caps != "1" || out != "hello wosh" {
		t.Fatalf("gzip without xz on PATH: caps %q out %q", caps, out)
	}
	if _, out = run("r", raw); out != "hello wosh" {
		t.Fatalf("raw: out %q", out)
	}
	if xzErr != nil {
		t.Skip("no xz")
	}
	if err := os.Symlink(xz, filepath.Join(bin, "xz")); err != nil {
		t.Fatal(err)
	}
	z, err := xzFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if caps, out = run("x", z); caps != "1x" || out != "hello wosh" {
		t.Fatalf("xz: caps %q out %q", caps, out)
	}
}

func TestChoosePayload(t *testing.T) {
	b := &localBuild{xzData: []byte("XZ")}
	b.xzOnce.Do(func() {})
	b.once.Do(func() { b.z = []byte("GZ") })
	for caps, want := range map[string]string{"1x": "x", "0x": "x", "1": "g"} {
		if c, _, err := choosePayload(b, caps); err != nil || c != want {
			t.Errorf("caps %q: codec %q err %v, want %q", caps, c, err, want)
		}
	}
	nb := &localBuild{} // xz not ready: gzip fallback
	nb.xzOnce.Do(func() {})
	nb.once.Do(func() { nb.z = []byte("GZ") })
	if c, p, _ := choosePayload(nb, "1x"); c != "g" || string(p) != "GZ" {
		t.Errorf("xz not ready: codec %q", c)
	}
}
