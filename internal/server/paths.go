package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// RuntimeDir is where daemon sockets and logs live (0700).
func RuntimeDir() (string, error) {
	dir := os.Getenv("WOSH_RUNTIME_DIR")
	if dir == "" {
		if x := os.Getenv("XDG_RUNTIME_DIR"); x != "" {
			dir = filepath.Join(x, "wosh")
		} else {
			dir = filepath.Join(os.TempDir(), fmt.Sprintf("wosh-%d", os.Getuid()))
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	st, err := os.Stat(dir)
	if err != nil {
		return "", err
	}
	if st.Mode().Perm()&0o077 != 0 {
		if err := os.Chmod(dir, 0o700); err != nil {
			return "", fmt.Errorf("runtime dir %s has unsafe permissions", dir)
		}
	}
	return dir, nil
}

// SocketPath returns the control socket for a session id.
func SocketPath(id string) (string, error) {
	dir, err := RuntimeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, id+".sock"), nil
}

// NewID returns a random session id.
func NewID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func validID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
