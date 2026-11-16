// Package build identifies wosh builds so a client can tell whether a wosh
// installed on a remote host is the same build, older, or newer.
package build

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// Stamp is "<unix commit time>-<sha256(binary)[:12]>". The time orders
// builds (uncommitted builds use the last commit's time); the hash makes
// any two different binaries distinct.
type Stamp string

// Time returns the ordering part of the stamp.
func (s Stamp) Time() int64 {
	t, _, _ := strings.Cut(string(s), "-")
	n, _ := strconv.ParseInt(t, 10, 64)
	return n
}

// Self returns the stamp of the running executable.
func Self() Stamp {
	exe := "/proc/self/exe"
	if _, err := os.Stat(exe); err != nil {
		exe, _ = os.Executable()
	}
	var t int64
	if bi, ok := debug.ReadBuildInfo(); ok {
		t = commitTime(bi)
	}
	return Stamp(fmt.Sprintf("%d-%s", t, fileHash(exe)))
}

// Of returns the stamp of a wosh binary file of any platform.
func Of(path string) (Stamp, error) {
	bi, err := buildinfo.ReadFile(path)
	if err != nil {
		return "", err
	}
	return Stamp(fmt.Sprintf("%d-%s", commitTime(bi), fileHash(path))), nil
}

func commitTime(bi *debug.BuildInfo) int64 {
	for _, s := range bi.Settings {
		if s.Key == "vcs.time" {
			if t, err := time.Parse(time.RFC3339, s.Value); err == nil {
				return t.Unix()
			}
		}
	}
	return 0
}

func fileHash(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return "unknown"
	}
	defer f.Close()
	h := sha256.New()
	_, _ = io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil))[:12]
}
