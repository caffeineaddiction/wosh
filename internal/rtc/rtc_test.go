package rtc

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestICEIfacesEnvEmptyList: WOSH_ICE_IFACES set but naming no interface
// gathers no candidates rather than lifting the limit.
func TestICEIfacesEnvEmptyList(t *testing.T) {
	t.Setenv("WOSH_ICE_IFACES", " , ")
	pc, err := NewAPI().NewPeerConnection(Config(nil, false))
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	if _, err := pc.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = WaitGathered(ctx, pc)
	if sdp := pc.LocalDescription().SDP; strings.Contains(sdp, "a=candidate:") {
		t.Fatalf("candidates gathered with an empty WOSH_ICE_IFACES:\n%s", sdp)
	}
}
