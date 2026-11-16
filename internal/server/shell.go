package server

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// LoginShell returns the user's shell: $SHELL, else the passwd entry,
// else /bin/sh. Any shell works (bash, zsh, fish, tcsh, nushell, ...);
// it is started as a login shell via a leading "-" in argv[0].
func LoginShell() string {
	if sh := os.Getenv("SHELL"); sh != "" {
		if _, err := os.Stat(sh); err == nil {
			return sh
		}
	}
	if sh := passwdShell(os.Getuid()); sh != "" {
		return sh
	}
	return "/bin/sh"
}

func passwdShell(uid int) string {
	f, err := os.Open("/etc/passwd")
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	want := strconv.Itoa(uid)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ":")
		if len(fields) >= 7 && fields[2] == want {
			if _, err := os.Stat(fields[6]); err == nil {
				return fields[6]
			}
		}
	}
	return ""
}
