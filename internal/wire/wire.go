// Package wire implements wosh's signaling protocol.
//
// Signaling runs in-band over any byte stream that already reaches the
// target host: an `ssh -T` pipe, a chain of ssh hops, or the PTY of another
// wosh session. That is how a session can be "upgraded" from wherever it
// happens to be reachable.
//
//   - host -> client: messages are wrapped in a private OSC escape sequence
//     (ESC ] 5379 ; wosh ; <base64 json> BEL) so they pass untouched through
//     PTYs and ssh hops and are invisible if they reach a real terminal.
//   - client -> host: messages are sent as a single input line
//     "WOSH:<base64 json>\r", which the remote relay reads from a raw-mode tty
//     (or a plain pipe).
package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

// Version of the signaling protocol. A remote wosh is only reused (instead
// of pushing this build) when its `wosh _proto` prints the same number.
// 2: hello carries sock/os, relays park/anchor.
const Version = 2

const (
	oscPrefix   = "\x1b]5379;wosh;"
	inputPrefix = "WOSH:"
	maxPayload  = 1 << 20
)

// Message types.
const (
	THello     = "hello"     // relay -> client: a wosh daemon is ready to be connected to
	TOffer     = "offer"     // client -> relay: SDP offer
	TAnswer    = "answer"    // relay -> client: SDP answer
	TError     = "err"       // relay -> client: negotiation failed
	TBye       = "bye"       // client -> relay: client declined / is done
	TTypeahead = "typeahead" // relay -> client: keystrokes that reached the relay's tty (Text: base64)
	TPark      = "park"      // relay -> daemon: I stay connected as the client's ssh anchor
	TOpen      = "open"      // shim -> daemon: open a stream to the client, then raw frames
	TInfo      = "info"      // shim -> daemon: who is the client (reply carries User)
	TType      = "type"      // local tool -> daemon: type Text into the session (e.g. reload the shell)
	TDrop      = "drop"      // local tool -> daemon: close the peers with CID (all peers if CID is "")
	TKill      = "kill"      // local tool -> daemon: hang up the session and exit
	TTail      = "tail"      // local tool -> daemon: the last N output bytes (reply: ok with Data, Off)
	TStart     = "start"     // local tool -> daemon: start the session with no peer (Term, Cols, Rows, Env, Key)
	TStatus    = "status"    // local tool -> daemon: session state (reply: ok with Status)
	TOK        = "ok"
)

// ICEServer mirrors webrtc.ICEServer without importing pion here.
type ICEServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

// Msg is a signaling message.
type Msg struct {
	T     string      `json:"t"`
	V     int         `json:"v,omitempty"`
	ID    string      `json:"id,omitempty"`
	Host  string      `json:"host,omitempty"`
	User  string      `json:"user,omitempty"`
	Key   string      `json:"key,omitempty"`
	SDP   string      `json:"sdp,omitempty"`
	ICE   []ICEServer `json:"ice,omitempty"`
	Relay bool        `json:"relay,omitempty"`
	Term  string      `json:"term,omitempty"`
	Cols  uint16      `json:"cols,omitempty"`
	Rows  uint16      `json:"rows,omitempty"`
	RxOut uint64      `json:"rx_out,omitempty"` // client has received session output up to this offset
	Err   string      `json:"err,omitempty"`
	Sock  string      `json:"sock,omitempty"` // hello: daemon control socket (for binary-less reattach)
	OS    string      `json:"os,omitempty"`   // hello: remote GOOS
	// Anchor is set by a relay running in the ssh session that launched the
	// daemon (never trusted from the client: the relay overwrites it).
	Anchor bool `json:"anchor,omitempty"`

	Env    map[string]string `json:"env,omitempty"`    // offer: variables for the session shell
	Src    string            `json:"src,omitempty"`    // offer: host name that means "the client" (src:path)
	Token  string            `json:"token,omitempty"`  // hello: parent session token (chained hop, no prompt)
	Via    string            `json:"via,omitempty"`    // hello: host that relayed a chained hop
	Hop    []string          `json:"hop,omitempty"`    // hello: ssh args the relaying host used to reach this one
	Agent  bool              `json:"agent,omitempty"`  // offer: forward the client's ssh-agent
	Stream *StreamSpec       `json:"stream,omitempty"` // control socket: open a stream to the client
	Text   string            `json:"text,omitempty"`   // control socket: input for TType
	N      int               `json:"n,omitempty"`      // control socket: bytes wanted by TTail
	Data   []byte            `json:"data,omitempty"`   // control socket: TTail reply bytes (base64 in JSON)
	Off    uint64            `json:"off,omitempty"`    // control socket: output offset of Data[0]
	Status *Status           `json:"status,omitempty"` // control socket: TStatus reply

	// Shared sessions. A client that sets CID (stable for one client instance,
	// across its reconnects) supports roles: several such peers may watch one
	// session while one of them controls it (input, resize, kill, forwards).
	// Without Join the offer takes control; with Join it only takes control if
	// nobody else has it, else it watches. A peer without a CID always takes
	// control, and the peer it replaces is dropped (the original behaviour).
	CID  string `json:"cid,omitempty"`  // offer: client instance id
	Join bool   `json:"join,omitempty"` // offer: watch if another client has control
	// Watch (with CID) makes a passive peer, e.g. a recorder: it always
	// watches, is never promoted to control, and cannot take it. A daemon
	// without this field ignores it (the peer then joins as above).
	Watch bool `json:"watch,omitempty"`
}

// Status is a daemon's state, for a local tool managing sessions.
type Status struct {
	Started  bool         `json:"started"`             // the shell runs (or ran)
	Exited   bool         `json:"exited"`              // the shell is gone
	ExitCode int          `json:"exit_code,omitempty"` // when Exited
	EverOpen bool         `json:"ever_open"`           // some peer connected at least once
	Peers    []PeerStatus `json:"peers"`               // connected and connecting peers
}

// PeerStatus describes one peer in Status.
type PeerStatus struct {
	CID     string `json:"cid,omitempty"`
	Role    string `json:"role"` // control, view, or pending (data channel not open yet)
	Passive bool   `json:"passive,omitempty"`
}

// StreamSpec describes an extra data channel (port forward, file transfer).
type StreamSpec struct {
	K    string `json:"k"`              // tcp | rfwd | exec | src-ssh
	Host string `json:"host,omitempty"` // tcp: target host (dialed by the daemon)
	Port int    `json:"port,omitempty"` // tcp: target port
	ID   int    `json:"id,omitempty"`   // rfwd: forward id
	Cmd  string `json:"cmd,omitempty"`  // exec: command line / subsystem name
	Sub  bool   `json:"sub,omitempty"`  // exec: Cmd is an ssh subsystem (sftp)
	// SID identifies a resumable stream; RX (on a reopen) is the next
	// message sequence number the opener expects.
	SID    string `json:"sid,omitempty"`
	RX     uint64 `json:"rx,omitempty"`
	Resume bool   `json:"resume,omitempty"`
}

func encode(m Msg) string {
	if m.V == 0 {
		m.V = Version
	}
	b, _ := json.Marshal(m)
	return base64.StdEncoding.EncodeToString(b)
}

func decode(s string) (Msg, error) {
	var m Msg
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return m, err
	}
	err = json.Unmarshal(b, &m)
	return m, err
}

// EncodeOSC encodes a host->client message.
func EncodeOSC(m Msg) []byte { return []byte(oscPrefix + encode(m) + "\a") }

// EncodeInput encodes a client->host message as an input line.
func EncodeInput(m Msg) []byte { return []byte(inputPrefix + encode(m) + "\r") }

// ParseInputLine extracts a message from one input line. ok is false when the
// line carries no wosh message (e.g. stray keystrokes).
func ParseInputLine(line []byte) (m Msg, ok bool, err error) {
	i := bytes.LastIndex(line, []byte(inputPrefix))
	if i < 0 {
		return m, false, nil
	}
	m, err = decode(string(line[i+len(inputPrefix):]))
	return m, err == nil, err
}

// LineReader splits raw-mode input into lines terminated by CR or LF.
type LineReader struct {
	buf []byte
}

// ErrInterrupt is returned when a Ctrl-C byte is seen on its own line.
var ErrInterrupt = errors.New("interrupted")

// Feed consumes input bytes and returns complete lines.
func (r *LineReader) Feed(p []byte) (lines [][]byte, err error) {
	for _, c := range p {
		switch c {
		case '\r', '\n':
			if len(r.buf) > 0 {
				lines = append(lines, r.buf)
				r.buf = nil
			}
		case 0x03: // ^C
			if !bytes.Contains(r.buf, []byte(inputPrefix)) {
				return lines, ErrInterrupt
			}
		default:
			if len(r.buf) < maxPayload {
				r.buf = append(r.buf, c)
			}
		}
	}
	return lines, nil
}

// OSCFilter scans a host->client output stream, strips wosh OSC messages
// and passes everything else through. It handles messages split across
// arbitrary chunk boundaries.
type OSCFilter struct {
	held    []byte // bytes matching a prefix of oscPrefix
	payload []byte
	inMsg   bool
	sawEsc  bool // inside payload, previous byte was ESC (for ST terminator)
}

// Write processes a chunk; returns the passthrough bytes and any messages.
func (f *OSCFilter) Write(p []byte) (out []byte, msgs []Msg) {
	out = make([]byte, 0, len(p))
	for _, c := range p {
		if f.inMsg {
			if c == '\a' || (f.sawEsc && c == '\\') {
				if m, err := decode(string(f.payload)); err == nil {
					msgs = append(msgs, m)
				}
				f.inMsg, f.sawEsc, f.payload = false, false, f.payload[:0]
				continue
			}
			if c == 0x1b {
				f.sawEsc = true
				continue
			}
			f.sawEsc = false
			if len(f.payload) >= maxPayload { // garbage; give up silently
				f.inMsg, f.payload = false, f.payload[:0]
				continue
			}
			f.payload = append(f.payload, c)
			continue
		}
		if len(f.held) > 0 || c == 0x1b {
			if c == oscPrefix[len(f.held)] {
				f.held = append(f.held, c)
				if len(f.held) == len(oscPrefix) {
					f.held, f.inMsg = f.held[:0], true
				}
				continue
			}
			// mismatch: flush held bytes, then reconsider c
			out = append(out, f.held...)
			f.held = f.held[:0]
			if c == 0x1b {
				f.held = append(f.held, c)
				continue
			}
		}
		out = append(out, c)
	}
	return out, msgs
}

// Pending reports whether the filter is holding back bytes that may turn out
// to be a wosh message prefix.
func (f *OSCFilter) Pending() bool { return len(f.held) > 0 || f.inMsg }

// InputSplitter separates "WOSH:<b64>" lines injected by the client from
// keystrokes the user typed into the same tty (typeahead).
type InputSplitter struct {
	buf []byte
}

// Feed returns messages and any other bytes, in arrival order of the latter.
func (s *InputSplitter) Feed(p []byte) (msgs []Msg, other []byte) {
	s.buf = append(s.buf, p...)
	for len(s.buf) > 0 {
		i := bytes.Index(s.buf, []byte(inputPrefix))
		if i < 0 {
			// keep a tail that may be the start of a prefix
			keep := 0
			for k := min(len(inputPrefix)-1, len(s.buf)); k > 0; k-- {
				if bytes.HasSuffix(s.buf, []byte(inputPrefix[:k])) {
					keep = k
					break
				}
			}
			other = append(other, s.buf[:len(s.buf)-keep]...)
			s.buf = s.buf[len(s.buf)-keep:]
			return msgs, other
		}
		other = append(other, s.buf[:i]...)
		s.buf = s.buf[i:]
		j := bytes.IndexAny(s.buf, "\r\n")
		if j < 0 {
			if len(s.buf) > maxPayload {
				s.buf = nil
			}
			return msgs, other
		}
		if m, err := decode(string(s.buf[len(inputPrefix):j])); err == nil {
			msgs = append(msgs, m)
		}
		s.buf = s.buf[j+1:]
	}
	return msgs, other
}

// FlushPartial returns held bytes that only looked like the start of a
// message prefix (call when input has gone quiet).
func (s *InputSplitter) FlushPartial() []byte {
	if len(s.buf) == 0 || bytes.HasPrefix(s.buf, []byte(inputPrefix)) {
		return nil
	}
	b := s.buf
	s.buf = nil
	return b
}
