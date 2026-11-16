package rtc

import (
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"
)

// DropUnreachableCandidates removes the peer's host candidates on our own
// subnets that we cannot actually send to, and returns the filtered SDP.
//
// macOS "Local Network" privacy blocks LAN sends (EHOSTUNREACH) from a
// process once the ssh session that launched it is gone, while still
// letting LAN traffic in. ICE can then half-succeed on a LAN pair and die
// right after nomination. Probing and dropping such candidates makes ICE
// settle on a path that works (VPN, server-reflexive, TURN). The policy
// takes effect with a short delay, so each probe keeps sending ~0.5s.
func DropUnreachableCandidates(sdp string) string {
	var local []*net.IPNet
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() && !ipn.IP.IsLinkLocalUnicast() {
				local = append(local, ipn)
			}
		}
	}
	onLink := func(ip net.IP) bool {
		for _, n := range local {
			if n.Contains(ip) && !n.IP.Equal(ip) {
				return true
			}
		}
		return false
	}
	lines := strings.Split(sdp, "\n")
	targets := map[string]bool{}
	for _, line := range lines {
		if ip, ok := hostCandidateIP(line); ok && onLink(ip) {
			targets[ip.String()] = true
		}
	}
	if len(targets) == 0 {
		return sdp
	}
	bad := map[string]bool{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for t := range targets {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			if err := probe(net.ParseIP(t)); err != nil {
				log.Printf("dropping peer candidate %s: cannot send to it (%v)", t, err)
				mu.Lock()
				bad[t] = true
				mu.Unlock()
			}
		}(t)
	}
	wg.Wait()
	if len(bad) == 0 {
		return sdp
	}
	out := lines[:0:0]
	for _, line := range lines {
		if ip, ok := hostCandidateIP(line); ok && bad[ip.String()] {
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// hostCandidateIP parses "a=candidate:<f> <c> udp <prio> <ip> <port> typ host".
func hostCandidateIP(line string) (net.IP, bool) {
	f := strings.Fields(strings.TrimPrefix(strings.TrimSpace(line), "a="))
	if len(f) >= 8 && strings.HasPrefix(f[0], "candidate:") && strings.EqualFold(f[2], "udp") && f[7] == "host" {
		ip := net.ParseIP(f[4])
		return ip, ip != nil
	}
	return nil, false
}

func probe(ip net.IP) error {
	u, err := net.ListenUDP("udp", nil) // unconnected, like ICE sockets
	if err != nil {
		return nil
	}
	defer u.Close()
	dst := &net.UDPAddr{IP: ip, Port: 9} // discard
	for i := 0; i < 6; i++ {
		if i > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		if _, err := u.WriteToUDP([]byte{0}, dst); err != nil && !errors.Is(err, syscall.ECONNREFUSED) {
			return err
		}
	}
	return nil
}
