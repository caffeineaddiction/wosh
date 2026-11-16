// Package rtc wraps the pion WebRTC setup shared by client and daemon.
package rtc

import (
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/logging"
	"github.com/pion/webrtc/v4"

	"wosh/internal/wire"
)

// DefaultSTUN is used when no ICE servers are configured.
var DefaultSTUN = []string{"stun:stun.l.google.com:19302"}

// LogWriter receives pion's log output (discarded by default so it never
// lands on the user's terminal).
var LogWriter io.Writer = io.Discard

// NewAPI returns a pion API tuned for interactive use: fast failure
// detection so roaming is noticed quickly.
// Option tweaks the setting engine.
type Option func(*webrtc.SettingEngine)

func NewAPI(opts ...Option) *webrtc.API {
	se := webrtc.SettingEngine{}
	level := logging.LogLevelWarn
	if os.Getenv("WOSH_DEBUG") != "" {
		level = logging.LogLevelDebug
	}
	se.LoggerFactory = &logging.DefaultLoggerFactory{Writer: LogWriter, DefaultLogLevel: level}
	// WOSH_ICE_NET=udp4,udp6 restricts candidate network types (debugging)
	if v := os.Getenv("WOSH_ICE_NET"); v != "" {
		var nts []webrtc.NetworkType
		for _, n := range strings.Split(v, ",") {
			if nt, err := webrtc.NewNetworkType(strings.TrimSpace(n)); err == nil {
				nts = append(nts, nt)
			}
		}
		se.SetNetworkTypes(nts)
	}
	se.SetICETimeouts(3*time.Second, 8*time.Second, 1*time.Second)
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	if os.Getenv("WOSH_LOOPBACK") != "" {
		se.SetIncludeLoopbackCandidate(true)
	}
	for _, o := range opts {
		o(&se)
	}
	return webrtc.NewAPI(webrtc.WithSettingEngine(se))
}

// Config builds a peer connection configuration.
func Config(servers []wire.ICEServer, relayOnly bool) webrtc.Configuration {
	cfg := webrtc.Configuration{}
	for _, s := range servers {
		cfg.ICEServers = append(cfg.ICEServers, webrtc.ICEServer{
			URLs: s.URLs, Username: s.Username, Credential: s.Credential,
		})
	}
	if relayOnly {
		cfg.ICETransportPolicy = webrtc.ICETransportPolicyRelay
	}
	return cfg
}

// ParseICE parses --stun / --turn style values.
//
//	stun: "stun:host:port"            (comma separated list allowed)
//	turn: "turn:host:port?transport=udp|user|pass"   (';' separated list)
func ParseICE(stun, turn string) []wire.ICEServer {
	var out []wire.ICEServer
	for _, s := range strings.Split(stun, ",") {
		if s = strings.TrimSpace(s); s != "" {
			if !strings.Contains(s, ":") || !strings.HasPrefix(s, "stun") {
				s = "stun:" + s
			}
			out = append(out, wire.ICEServer{URLs: []string{s}})
		}
	}
	for _, t := range strings.Split(turn, ";") {
		if t = strings.TrimSpace(t); t == "" {
			continue
		}
		parts := strings.SplitN(t, "|", 3)
		u := parts[0]
		if !strings.HasPrefix(u, "turn") {
			u = "turn:" + u
		}
		s := wire.ICEServer{URLs: []string{u}}
		if len(parts) > 1 {
			s.Username = parts[1]
		}
		if len(parts) > 2 {
			s.Credential = parts[2]
		}
		out = append(out, s)
	}
	return out
}

// WaitGathered waits for ICE gathering to complete so the SDP can be sent
// in a single message (no trickle; signaling paths may be slow).
func WaitGathered(ctx context.Context, pc *webrtc.PeerConnection) error {
	done := webrtc.GatheringCompletePromise(pc)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
