package server

import (
	"fmt"
	"strconv"
	"strings"
)

// ring keeps the most recent output bytes addressed by absolute stream
// offset, so a reconnecting client can resume exactly where it left off.
type ring struct {
	data  []byte
	start uint64 // absolute offset of data[0]
	max   int
}

func newRing(max int) *ring { return &ring{max: max} }

// Ring size limits for `_daemon --ring`. The ring holds up to twice its size
// before trimming, so 512M stays clear of a 32-bit int.
const (
	MinRingSize = 64 << 10
	MaxRingSize = 512 << 20
)

// ParseSize parses a ring size: bytes, or a number with a K, M or G suffix
// (binary units; "KiB", "MB" and lowercase forms are accepted too).
func ParseSize(s string) (int, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	t = strings.TrimSuffix(strings.TrimSuffix(t, "B"), "I")
	mult := 1
	switch {
	case strings.HasSuffix(t, "K"):
		mult = 1 << 10
	case strings.HasSuffix(t, "M"):
		mult = 1 << 20
	case strings.HasSuffix(t, "G"):
		mult = 1 << 30
	}
	if mult > 1 {
		t = t[:len(t)-1]
	}
	n, err := strconv.Atoi(t)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("bad size %q", s)
	}
	if n > MaxRingSize/mult {
		return 0, fmt.Errorf("size %q is over %dM", s, MaxRingSize>>20)
	}
	if n*mult < MinRingSize {
		return 0, fmt.Errorf("size %q is under %dK", s, MinRingSize>>10)
	}
	return n * mult, nil
}

func (r *ring) end() uint64 { return r.start + uint64(len(r.data)) }

func (r *ring) append(p []byte) {
	r.data = append(r.data, p...)
	if len(r.data) > 2*r.max {
		drop := len(r.data) - r.max
		r.data = append(r.data[:0:0], r.data[drop:]...)
		r.start += uint64(drop)
	}
}

// slice returns up to n bytes starting at off (clamped to what is retained)
// and the offset actually used.
func (r *ring) slice(off uint64, n int) ([]byte, uint64) {
	if off < r.start {
		off = r.start
	}
	if off >= r.end() {
		return nil, off
	}
	i := int(off - r.start)
	j := i + n
	if j > len(r.data) {
		j = len(r.data)
	}
	return r.data[i:j], off
}
