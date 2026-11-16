// Package shellfix implements `wosh --fix-shell`: make the session's bin dir
// ($WOSH_BIN_DIR: wosh, ssh, scp, sftp shims) win in PATH and the forwarded
// ssh-agent ($WOSH_AGENT_SOCK) win over SSH_AUTH_SOCK, even when a shell rc
// sets them, by appending a small managed block to the end of the shell's
// config file(s).
package shellfix

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	begin = "# >>> wosh >>> (managed by `wosh --fix-shell`; keep this block at the end)"
	end   = "# <<< wosh <<<"
)

var snippets = map[string]string{
	"fish": `if set -q WOSH_BIN_DIR
    set -gx PATH $WOSH_BIN_DIR (string match -v -- $WOSH_BIN_DIR $PATH)
end
if set -q WOSH_AGENT_SOCK
    set -gx SSH_AUTH_SOCK $WOSH_AGENT_SOCK
end`,
	"posix": `if [ -n "${WOSH_BIN_DIR:-}" ]; then
    PATH="$WOSH_BIN_DIR:$(printf '%s' "$PATH" | tr ':' '\n' | grep -vxF -- "$WOSH_BIN_DIR" | paste -s -d: -)"
    export PATH
fi
if [ -n "${WOSH_AGENT_SOCK:-}" ]; then
    SSH_AUTH_SOCK=$WOSH_AGENT_SOCK; export SSH_AUTH_SOCK
fi`,
	"csh": `if ( $?WOSH_BIN_DIR ) then
    setenv PATH "${WOSH_BIN_DIR}:${PATH}"
endif
if ( $?WOSH_AGENT_SOCK ) then
    setenv SSH_AUTH_SOCK "${WOSH_AGENT_SOCK}"
endif`,
}

// Target is one config file to patch.
type Target struct {
	File    string
	Snippet string
}

// Targets returns the files to patch for a shell (name or path).
func Targets(shell string) ([]Target, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	name := filepath.Base(shell)
	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }
	switch name {
	case "fish":
		dir := os.Getenv("XDG_CONFIG_HOME")
		if dir == "" {
			dir = filepath.Join(home, ".config")
		}
		return []Target{{filepath.Join(dir, "fish", "config.fish"), snippets["fish"]}}, nil
	case "bash":
		// interactive shells read .bashrc; login shells (wosh starts those)
		// read the first of .bash_profile / .bash_login / .profile
		t := []Target{{filepath.Join(home, ".bashrc"), snippets["posix"]}}
		login := filepath.Join(home, ".bash_profile")
		for _, f := range []string{".bash_profile", ".bash_login", ".profile"} {
			if p := filepath.Join(home, f); exists(p) {
				login = p
				break
			}
		}
		return append(t, Target{login, snippets["posix"]}), nil
	case "zsh":
		dir := os.Getenv("ZDOTDIR")
		if dir == "" {
			dir = home
		}
		return []Target{{filepath.Join(dir, ".zshrc"), snippets["posix"]}}, nil
	case "sh", "dash", "ksh", "mksh", "ash", "busybox":
		return []Target{{filepath.Join(home, ".profile"), snippets["posix"]}}, nil
	case "tcsh", "csh":
		f := filepath.Join(home, ".cshrc")
		if p := filepath.Join(home, ".tcshrc"); name == "tcsh" && exists(p) || !exists(f) && name == "tcsh" {
			f = p
		}
		return []Target{{f, snippets["csh"]}}, nil
	}
	return nil, fmt.Errorf("don't know how to configure %q; add WOSH_BIN_DIR to the front of PATH at the end of its config", name)
}

// Apply writes the managed block at the end of the file. It returns false
// when the file already ends with the current block (nothing to do).
func Apply(t Target) (changed bool, backup string, err error) {
	old, err := os.ReadFile(t.File)
	if err != nil && !os.IsNotExist(err) {
		return false, "", err
	}
	block := begin + "\n" + t.Snippet + "\n" + end + "\n"
	rest := strip(string(old))
	if strings.TrimRight(rest, "\n") != "" {
		rest = strings.TrimRight(rest, "\n") + "\n\n"
	} else {
		rest = ""
	}
	next := rest + block
	if next == string(old) {
		return false, "", nil
	}
	if len(old) > 0 {
		backup = t.File + ".wosh.bak"
		if _, err := os.Stat(backup); os.IsNotExist(err) {
			if err := os.WriteFile(backup, old, 0o600); err != nil {
				return false, "", err
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(t.File), 0o755); err != nil {
		return false, backup, err
	}
	mode := os.FileMode(0o644)
	if st, err := os.Stat(t.File); err == nil {
		mode = st.Mode().Perm()
	}
	tmp := t.File + ".wosh.tmp"
	if err := os.WriteFile(tmp, []byte(next), mode); err != nil {
		return false, backup, err
	}
	return true, backup, os.Rename(tmp, t.File)
}

// Remove deletes the managed block.
func Remove(t Target) (bool, error) {
	old, err := os.ReadFile(t.File)
	if err != nil {
		return false, nil
	}
	next := strip(string(old))
	if next == string(old) {
		return false, nil
	}
	return true, os.WriteFile(t.File, []byte(next), 0o644)
}

// strip removes any existing managed block (wherever it is).
func strip(s string) string {
	for {
		i := strings.Index(s, begin)
		if i < 0 {
			return s
		}
		j := strings.Index(s[i:], end)
		if j < 0 {
			return s
		}
		j = i + j + len(end)
		if j < len(s) && s[j] == '\n' {
			j++
		}
		s = s[:i] + s[j:]
	}
}
