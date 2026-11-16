package shellfix

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplyIdempotentAndLast(t *testing.T) {
	f := filepath.Join(t.TempDir(), "rc")
	tg := Target{File: f, Snippet: snippets["posix"]}
	_ = os.WriteFile(f, []byte("export PATH=/usr/bin:/bin\n"), 0o644)
	if ch, bak, err := Apply(tg); !ch || err != nil || bak == "" {
		t.Fatalf("first apply: %v %q %v", ch, bak, err)
	}
	if ch, _, _ := Apply(tg); ch {
		t.Fatal("second apply changed the file")
	}
	// user appends PATH rewrites after our block: we move back to the end
	b, _ := os.ReadFile(f)
	_ = os.WriteFile(f, append(b, []byte("export PATH=/opt/x:$PATH\n")...), 0o644)
	if ch, _, _ := Apply(tg); !ch {
		t.Fatal("block not moved to the end")
	}
	b, _ = os.ReadFile(f)
	s := string(b)
	if strings.Count(s, begin) != 1 || !strings.HasSuffix(s, end+"\n") || !strings.Contains(s, "/opt/x") {
		t.Fatalf("bad result:\n%s", s)
	}
	if ch, _ := Remove(tg); !ch {
		t.Fatal("remove")
	}
	b, _ = os.ReadFile(f)
	if strings.Contains(string(b), "wosh") {
		t.Fatal("block left after remove")
	}
}

// The snippets must actually put WOSH_BIN_DIR first, once.
func TestSnippetsWork(t *testing.T) {
	cases := map[string]string{"sh": "posix", "bash": "posix", "zsh": "posix", "dash": "posix", "fish": "fish"}
	for sh, kind := range cases {
		p, err := exec.LookPath(sh)
		if err != nil {
			continue
		}
		script := "set -gx PATH /usr/bin /bin /wosh/bin\nset -gx SSH_AUTH_SOCK /rc/agent\n" + snippets[kind] + "\necho (string join : $PATH) $SSH_AUTH_SOCK"
		if kind == "posix" {
			script = "PATH=/usr/bin:/bin:/wosh/bin\nSSH_AUTH_SOCK=/rc/agent\n" + snippets[kind] + "\necho \"$PATH\" \"$SSH_AUTH_SOCK\""
		}
		cmd := exec.Command(p, "-c", script)
		cmd.Env = append(os.Environ(), "WOSH_BIN_DIR=/wosh/bin", "WOSH_AGENT_SOCK=/wosh/agent")
		out, err := cmd.CombinedOutput()
		if got := strings.TrimSpace(string(out)); err != nil || got != "/wosh/bin:/usr/bin:/bin /wosh/agent" {
			t.Errorf("%s: %q %v", sh, got, err)
		}
	}
}
