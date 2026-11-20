package wire

import (
	"encoding/binary"
	"encoding/json"
)

// Data channel frame types. Every frame is one data channel message whose
// first byte is the type.
const (
	FOutput  = 'o' // S->C  u64 offset + bytes   (PTY output)
	FInput   = 'i' // C->S  u64 offset + bytes   (keystrokes)
	FAck     = 'a' // S->C  u64 input offset received
	FResize  = 'r' // C->S  u16 cols + u16 rows
	FRedraw  = 'R' // C->S  ask for a full-screen redraw
	FPing    = 'p' // C->S  u64 nonce
	FPong    = 'P' // S->C  u64 nonce
	FExit    = 'x' // S->C  i32 exit status
	FExitAck = 'X' // C->S
	FKill    = 'k' // C->S  hang up the session
	FControl = 'c' // both  JSON control message (see Control)
	FEchoAck = 'e' // S->C  u64: input up to here reached the app >= 50ms ago and
	//                       any output it caused was sent before this frame
)

// MaxChunk is the largest payload we put in one frame.
const MaxChunk = 16 * 1024

// OffsetFrame builds an output/input frame.
func OffsetFrame(t byte, off uint64, data []byte) []byte {
	b := make([]byte, 9+len(data))
	b[0] = t
	binary.BigEndian.PutUint64(b[1:], off)
	copy(b[9:], data)
	return b
}

// U64Frame builds a frame holding one u64.
func U64Frame(t byte, v uint64) []byte { return OffsetFrame(t, v, nil) }

// ResizeFrame builds a resize frame.
func ResizeFrame(cols, rows uint16) []byte {
	b := make([]byte, 5)
	b[0] = FResize
	binary.BigEndian.PutUint16(b[1:], cols)
	binary.BigEndian.PutUint16(b[3:], rows)
	return b
}

// ExitFrame builds an exit frame.
func ExitFrame(code int) []byte {
	b := make([]byte, 5)
	b[0] = FExit
	binary.BigEndian.PutUint32(b[1:], uint32(int32(code)))
	return b
}

// ParseOffset parses an offset frame (also usable for u64 frames).
func ParseOffset(b []byte) (off uint64, data []byte, ok bool) {
	if len(b) < 9 {
		return 0, nil, false
	}
	return binary.BigEndian.Uint64(b[1:9]), b[9:], true
}

// ParseResize parses a resize frame.
func ParseResize(b []byte) (cols, rows uint16, ok bool) {
	if len(b) < 5 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint16(b[1:]), binary.BigEndian.Uint16(b[3:]), true
}

// ParseExit parses an exit frame.
func ParseExit(b []byte) int {
	if len(b) < 5 {
		return 255
	}
	return int(int32(binary.BigEndian.Uint32(b[1:])))
}

// Control is a JSON control message on the main channel.
type Control struct {
	Op    string      `json:"op"`             // info, role, size (S->C); rfwd, take (C->S)
	Role  string      `json:"role,omitempty"` // role: "control" or "view" (peers with a CID)
	Cols  uint16      `json:"cols,omitempty"` // size: the session terminal (set by the controller)
	Rows  uint16      `json:"rows,omitempty"`
	Token string      `json:"token,omitempty"` // info: session token for chained hops
	Out   uint64      `json:"out,omitempty"`   // info: output offset at connect; below it is replay (don't answer queries there)
	Rfwd  []RemoteFwd `json:"rfwd,omitempty"`  // rfwd: remote forwards to listen on
}

// RemoteFwd is a -R forward: the daemon listens on Bind:Port.
type RemoteFwd struct {
	ID   int    `json:"id"`
	Bind string `json:"bind"`
	Port int    `json:"port"`
}

// ControlFrame encodes a control message.
func ControlFrame(c Control) []byte {
	b, _ := json.Marshal(c)
	return append([]byte{FControl}, b...)
}

// ParseControl decodes a control frame.
func ParseControl(b []byte) (Control, bool) {
	var c Control
	if len(b) < 2 || json.Unmarshal(b[1:], &c) != nil {
		return c, false
	}
	return c, true
}
