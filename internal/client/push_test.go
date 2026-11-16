package client

import (
	"testing"

	"wosh/internal/build"
	"wosh/internal/wire"
)

func TestDecide(t *testing.T) {
	ours := &localBuild{stamp: build.Stamp("2000-aaaa")}
	same := installed{path: "/h/bin/wosh", writable: true, proto: wire.Version, stamp: "2000-aaaa"}
	older := installed{path: "/h/bin/wosh", writable: true, proto: wire.Version, stamp: "1000-bbbb"}
	olderRO := installed{path: "/usr/bin/wosh", writable: false, proto: wire.Version, stamp: "1000-bbbb"}
	oldProto := installed{path: "/h/bin/wosh", writable: true, proto: 0} // pre-_proto build
	newer := installed{path: "/h/bin/wosh", writable: true, proto: wire.Version, stamp: "3000-cccc"}
	newerIncompat := installed{path: "/h/bin/wosh", writable: true, proto: wire.Version + 1, stamp: "3000-cccc"}
	sameTimeDirty := installed{path: "/h/bin/wosh", writable: true, proto: wire.Version, stamp: "2000-dddd"}

	cases := []struct {
		name  string
		found []installed
		ours  *localBuild
		force bool
		verb  string
		path  string
	}{
		{"nothing installed", nil, ours, false, "MEM", ""},
		{"same build", []installed{same}, ours, false, "USE", "/h/bin/wosh"},
		{"older writable is upgraded", []installed{older}, ours, false, "UPGRADE", "/h/bin/wosh"},
		{"pre-_proto build is upgraded", []installed{oldProto}, ours, false, "UPGRADE", "/h/bin/wosh"},
		{"same commit, different binary: client wins", []installed{sameTimeDirty}, ours, false, "UPGRADE", "/h/bin/wosh"},
		{"older read-only but compatible is used", []installed{olderRO}, ours, false, "USE", "/usr/bin/wosh"},
		{"newer compatible is used, never downgraded", []installed{newer}, ours, false, "USE", "/h/bin/wosh"},
		{"newer incompatible: run ours from memory", []installed{newerIncompat}, ours, false, "MEM", ""},
		{"same build later in PATH wins over upgrade", []installed{older, {path: "/x/wosh", proto: wire.Version, stamp: "2000-aaaa"}}, ours, false, "USE", "/x/wosh"},
		{"force push", []installed{same}, ours, true, "MEM", ""},
		{"no build for platform, compatible installed", []installed{olderRO}, nil, false, "USE", "/usr/bin/wosh"},
	}
	for _, c := range cases {
		d, err := decide(c.found, c.ours, c.force)
		if err != nil || d.verb != c.verb || d.path != c.path {
			t.Errorf("%s: got %+v err %v, want %s %s", c.name, d, err, c.verb, c.path)
		}
	}
	if _, err := decide([]installed{oldProto}, nil, false); err == nil {
		t.Error("no build and nothing compatible should fail")
	}
}

func TestParseInstalled(t *testing.T) {
	got := parseInstalled([]string{"/a/wosh|1|2/123-abc", "/b/wosh|0|-", "junk"})
	if len(got) != 2 || !got[0].writable || got[0].proto != 2 || got[0].stamp != "123-abc" || got[1].proto != 0 {
		t.Fatalf("%+v", got)
	}
}
