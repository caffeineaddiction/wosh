package stream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"

	"wosh/internal/wire"
)

// InSession reports whether we run inside a wosh session.
func InSession() bool { return os.Getenv("WOSH_SOCK") != "" }

func dialSession() (net.Conn, *bufio.Reader, error) {
	sock := os.Getenv("WOSH_SOCK")
	if sock == "" {
		return nil, nil, fmt.Errorf("not inside a wosh session")
	}
	c, err := net.Dial("unix", sock)
	if err != nil {
		return nil, nil, err
	}
	return c, bufio.NewReader(c), nil
}

func request(c net.Conn, r *bufio.Reader, m wire.Msg) (wire.Msg, error) {
	if err := json.NewEncoder(c).Encode(m); err != nil {
		return wire.Msg{}, err
	}
	line, err := r.ReadBytes('\n')
	if err != nil {
		return wire.Msg{}, err
	}
	var resp wire.Msg
	err = json.Unmarshal(line, &resp)
	if err == nil && resp.T == wire.TError {
		err = fmt.Errorf("%s", resp.Err)
	}
	return resp, err
}

// SessionInfo asks this session's daemon about its client.
func SessionInfo() (wire.Msg, error) {
	c, r, err := dialSession()
	if err != nil {
		return wire.Msg{}, err
	}
	defer c.Close()
	return request(c, r, wire.Msg{T: wire.TInfo})
}

// ToClient opens a stream from this session's host to the wosh client.
func ToClient(spec wire.StreamSpec) (*Framed, error) {
	c, r, err := dialSession()
	if err != nil {
		return nil, err
	}
	if _, err := request(c, r, wire.Msg{T: wire.TOpen, Stream: &spec}); err != nil {
		c.Close()
		return nil, err
	}
	return NewFramed(r, c, c), nil
}

// TypeIntoSession sends text to the session's terminal as if typed (it is
// read by the shell once the current foreground program exits).
func TypeIntoSession(text string) error {
	c, r, err := dialSession()
	if err != nil {
		return err
	}
	defer c.Close()
	_, err = request(c, r, wire.Msg{T: wire.TType, Text: text})
	return err
}
