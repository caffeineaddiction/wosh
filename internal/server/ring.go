package server

// ring keeps the most recent output bytes addressed by absolute stream
// offset, so a reconnecting client can resume exactly where it left off.
type ring struct {
	data  []byte
	start uint64 // absolute offset of data[0]
	max   int
}

func newRing(max int) *ring { return &ring{max: max} }

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
