package client

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The remote command string is interpreted by the user's login shell on the
// remote host. It must behave identically under every common shell.
func TestRemoteCommandAcrossShells(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "my wosh") // space forces quoting
	script := "#!/bin/sh\necho \"ARGS:$*\"\n"
	if err := writeExec(fake, script); err != nil {
		t.Fatal(err)
	}
	cmd := RemoteCommand(fake, "attach", "abc123")
	tested := 0
	for _, sh := range []string{"sh", "dash", "bash", "zsh", "fish", "tcsh", "csh", "ksh", "mksh", "nu"} {
		path, err := exec.LookPath(sh)
		if err != nil {
			continue
		}
		tested++
		out, err := exec.Command(path, "-c", cmd).CombinedOutput()
		if err != nil || strings.TrimSpace(string(out)) != "ARGS:attach abc123" {
			t.Errorf("%s: %q err %v (cmd %s)", sh, out, err, cmd)
		}
	}
	if tested == 0 {
		t.Skip("no shells found")
	}
	t.Logf("verified on %d shells", tested)

	// default binary name is looked up via PATH incl. ~/.local/bin etc.
	if got := RemoteCommand("", "server"); !strings.Contains(got, " wosh server") {
		t.Errorf("default: %s", got)
	}
}

func writeExec(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o755)
}
