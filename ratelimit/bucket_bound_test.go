package ratelimit

import (
	"encoding/binary"
	"net"
	"testing"
)

// Regression: the IP and group tiers kept a token bucket for every key they
// ever saw. Their keys come from unauthenticated NACKs (a spoofable source
// address, and a group index the NACK selects), so key diversity alone grew
// both maps without limit for the life of the process. A bucket that has
// refilled is in the state a new key starts in, so it must be swept.
func TestTokenBucketMapsAreBounded(t *testing.T) {
	l := New(Config{
		IPRate: 1e9, IPBurst: 1_000, // refills in a microsecond
		SequenceMax: 1_000_000,
		GroupRate:   1e9, GroupBurst: 1_000,
	})
	const sources = maxWindowKeys + maxWindowKeys/2
	ip := net.ParseIP("2001:db8::").To16()
	for i := 0; i < sources; i++ {
		binary.BigEndian.PutUint32(ip[12:], uint32(i))
		if ok, level := l.Allow(ip, testFlowKey, 1); !ok {
			t.Fatalf("source %d rejected at the %s tier", i, level)
		}
		if !l.AllowGroup(ip, uint32(i%4096)) {
			t.Fatalf("source %d rejected at the group tier", i)
		}
	}
	if n := len(l.ipLimiter.limit); n > maxWindowKeys {
		t.Errorf("IP tier holds %d buckets after %d one-off sources, want at most %d", n, sources, maxWindowKeys)
	}
	if n := len(l.groupLimiter.limit); n > maxWindowKeys {
		t.Errorf("group tier holds %d buckets after %d one-off sources, want at most %d", n, sources, maxWindowKeys)
	}
}
