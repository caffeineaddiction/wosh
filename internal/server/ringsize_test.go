package server

import (
	"testing"
	"time"
)

func TestParseSize(t *testing.T) {
	good := map[string]int{
		"65536": 64 << 10, "64K": 64 << 10, "64k": 64 << 10, "64KiB": 64 << 10,
		"4M": 4 << 20, "64M": 64 << 20, "64MB": 64 << 20, "64mib": 64 << 20,
		"512M": 512 << 20,
	}
	for s, want := range good {
		if n, err := ParseSize(s); err != nil || n != want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", s, n, err, want)
		}
	}
	for _, s := range []string{"", "M", "-1M", "0", "1K", "63K", "513M", "1G", "64X", "1.5M", "9999999999999G"} {
		if n, err := ParseSize(s); err == nil {
			t.Errorf("ParseSize(%q) = %d, want an error", s, n)
		}
	}
}

// TestRingFlag: a peer that connects late replays at most what --ring keeps
// (the ring holds up to twice its size before trimming); without the flag
// the 4 MiB default keeps everything.
func TestRingFlag(t *testing.T) {
	const produce = 600 << 10
	cmd := "head -c 614400 /dev/zero | tr '\\0' x; echo; echo END-$((40+2))\r"
	for _, tc := range []struct {
		args []string
		min  int
		max  int
	}{
		{[]string{"--ring", "64K"}, 64 << 10, 2*(64<<10) + 32<<10},
		{nil, produce, produce + 32<<10},
	} {
		d := startDaemon(t, tc.args...)
		a := d.connect("aaaa", false)
		a.typ(cmd)
		a.waitOutput("END-42", 20*time.Second)
		b := d.connect("bbbb", true) // replays from offset 0: what the ring kept
		b.waitOutput("END-42", 20*time.Second)
		if n := len(b.output()); n < tc.min || n > tc.max {
			t.Errorf("%v: replayed %d bytes, want %d..%d", tc.args, n, tc.min, tc.max)
		}
	}
}
