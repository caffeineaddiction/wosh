package server

import "testing"

func TestRing(t *testing.T) {
	r := newRing(10)
	for i := 0; i < 50; i++ {
		r.append([]byte{byte(i)})
	}
	if r.end() != 50 {
		t.Fatalf("end %d", r.end())
	}
	if r.start < 30 {
		t.Fatalf("ring not trimmed: start %d", r.start)
	}
	b, off := r.slice(0, 4) // too old: clamped
	if off != r.start || b[0] != byte(r.start) {
		t.Fatalf("clamp: off %d b %v", off, b)
	}
	b, off = r.slice(48, 100)
	if off != 48 || len(b) != 2 || b[1] != 49 {
		t.Fatalf("tail: off %d b %v", off, b)
	}
	if b, _ := r.slice(50, 1); b != nil {
		t.Fatal("past end")
	}
}
