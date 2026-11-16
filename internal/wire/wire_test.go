package wire

import (
	"bytes"
	"strings"
	"testing"
)

func TestOSCFilterSplitChunks(t *testing.T) {
	msg := Msg{T: THello, ID: "abc", Host: "h"}
	stream := append([]byte("before\x1b[31mred\x1b]0;title\a"), EncodeOSC(msg)...)
	stream = append(stream, []byte("after\x1b")...)
	stream = append(stream, []byte("]x")...)

	for chunk := 1; chunk <= len(stream); chunk++ {
		var f OSCFilter
		var out []byte
		var got []Msg
		for i := 0; i < len(stream); i += chunk {
			j := min(i+chunk, len(stream))
			o, m := f.Write(stream[i:j])
			out = append(out, o...)
			got = append(got, m...)
		}
		want := "before\x1b[31mred\x1b]0;title\aafter\x1b]x"
		if string(out) != want {
			t.Fatalf("chunk %d: passthrough %q want %q", chunk, out, want)
		}
		if len(got) != 1 || got[0].ID != "abc" || got[0].T != THello {
			t.Fatalf("chunk %d: msgs %+v", chunk, got)
		}
	}
}

func TestInputLine(t *testing.T) {
	var lr LineReader
	in := append([]byte("junk typed\r"), EncodeInput(Msg{T: TOffer, SDP: "v=0"})...)
	lines, err := lr.Feed(in)
	if err != nil || len(lines) != 2 {
		t.Fatalf("lines %q err %v", lines, err)
	}
	if _, ok, _ := ParseInputLine(lines[0]); ok {
		t.Fatal("junk parsed as message")
	}
	m, ok, err := ParseInputLine(lines[1])
	if !ok || err != nil || m.SDP != "v=0" {
		t.Fatalf("got %+v %v %v", m, ok, err)
	}
}

func TestFrames(t *testing.T) {
	off, data, ok := ParseOffset(OffsetFrame(FOutput, 42, []byte("hi")))
	if !ok || off != 42 || !bytes.Equal(data, []byte("hi")) {
		t.Fatal("offset frame")
	}
	c, r, ok := ParseResize(ResizeFrame(120, 40))
	if !ok || c != 120 || r != 40 {
		t.Fatal("resize frame")
	}
	if ParseExit(ExitFrame(-3)) != -3 {
		t.Fatal("exit frame")
	}
}

func TestInputSplitter(t *testing.T) {
	msg := EncodeInput(Msg{T: TOffer, ID: "x"})
	stream := append([]byte("ls -la\rWO"), msg...) // "WO" typed, then a message
	stream = append(stream, []byte("WOSHY not a msg")...)
	for chunk := 1; chunk <= len(stream); chunk++ {
		var sp InputSplitter
		var other []byte
		var msgs []Msg
		for i := 0; i < len(stream); i += chunk {
			m, o := sp.Feed(stream[i:min(i+chunk, len(stream))])
			msgs, other = append(msgs, m...), append(other, o...)
		}
		m, o := sp.Feed(nil)
		msgs, other = append(msgs, m...), append(other, o...)
		if len(msgs) != 1 || msgs[0].ID != "x" {
			t.Fatalf("chunk %d: msgs %+v", chunk, msgs)
		}
		if !strings.HasPrefix(string(other), "ls -la\rWO") || !strings.Contains(string(other), "Y not a msg") {
			t.Fatalf("chunk %d: other %q", chunk, other)
		}
	}
}
