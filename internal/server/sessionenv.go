package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Shims are the names under which the session's bin dir exposes wosh. The
// binary dispatches on argv[0] (see main): `ssh`/`scp`/`sftp` handle the
// client host ("src:path") and pass everything else to the real tools.
var Shims = []string{"ssh", "scp", "sftp"}

// setupBinDir creates <runtime>/<id>.d/bin with wosh and its shims, all
// pointing at the binary this daemon runs from:
//   - Linux: /proc/<pid>/exe, which works even when wosh only exists in a
//     memfd (the runtime dir is normally a tmpfs, so nothing hits disk);
//   - a pushed temp file (macOS): moved here and removed with the session;
//   - otherwise the installed executable.
func (d *Daemon) setupBinDir() error {
	rt, err := RuntimeDir()
	if err != nil {
		return err
	}
	base := filepath.Join(rt, d.opts.ID+".d")
	bin := filepath.Join(base, "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		return err
	}
	d.baseDir, d.binDir = base, bin
	target := ""
	switch {
	case runtime.GOOS == "linux":
		target = fmt.Sprintf("/proc/%d/exe", os.Getpid())
		if d.opts.OwnBinary != "" { // /dev/shm fallback copy: /proc keeps it reachable
			_ = os.Remove(d.opts.OwnBinary)
		}
	case d.opts.OwnBinary != "":
		dst := filepath.Join(bin, "wosh")
		if err := os.Rename(d.opts.OwnBinary, dst); err != nil {
			log.Printf("keep pushed binary: %v", err)
			_ = os.Remove(d.opts.OwnBinary)
		} else {
			target = dst
		}
	default:
		if exe, err := os.Executable(); err == nil {
			target = exe
		}
	}
	if target == "" {
		return fmt.Errorf("no path to own executable")
	}
	for _, name := range append([]string{"wosh"}, Shims...) {
		p := filepath.Join(bin, name)
		if p == target {
			continue
		}
		_ = os.Remove(p)
		if err := os.Symlink(target, p); err != nil {
			return err
		}
	}
	return nil
}

func newToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// keepEnv: WOSH_* variables the session shell inherits. The ICE limits
// carry over to daemons started from inside the session (chained hops).
var keepEnv = map[string]bool{"WOSH_RUNTIME_DIR": true, "WOSH_ICE_IFACES": true, "WOSH_LOOPBACK": true}

// sessionEnv builds the environment of the session shell.
func (d *Daemon) sessionEnv(term string, extra map[string]string) []string {
	skip := map[string]bool{"TERM": true, "WOSH_SESSION": true, "WOSH_UNLINK": true}
	for k := range extra {
		skip[k] = true
	}
	var env []string
	path := os.Getenv("PATH")
	for _, e := range os.Environ() {
		k, _, _ := strings.Cut(e, "=")
		if skip[k] || strings.HasPrefix(k, "WOSH_") && !keepEnv[k] || k == "PATH" {
			continue
		}
		env = append(env, e)
	}
	for k, v := range extra {
		if k == "PATH" {
			path = v
			continue
		}
		env = append(env, k+"="+v)
	}
	sock, _ := SocketPath(d.opts.ID)
	env = append(env, "TERM="+term, "WOSH_SESSION="+d.opts.ID, "WOSH_SOCK="+sock, "WOSH_TOKEN="+d.token)
	if d.src != "" {
		env = append(env, "WOSH_SRC="+d.src)
	}
	if d.binDir != "" {
		path = d.binDir + string(os.PathListSeparator) + path
		env = append(env, "WOSH_BIN="+filepath.Join(d.binDir, "wosh"), "WOSH_BIN_DIR="+d.binDir)
		if os.Getenv("RSYNC_RSH") == "" && extra["RSYNC_RSH"] == "" {
			// rsync finds our ssh shim even if a shell rc rewrites PATH
			env = append(env, "RSYNC_RSH="+filepath.Join(d.binDir, "ssh"))
		}
	}
	return append(env, "PATH="+path)
}
