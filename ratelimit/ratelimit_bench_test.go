package ratelimit

import (
	"net"
	"testing"
	"time"
)

// The benchmarks drive the chain tier with max = 20000: through AllowChain on
// the real clock, or through allowChainAt where a steady per-key rate needs a
// synthetic clock.

const benchMax = 20_000

var benchIP = net.ParseIP("2001:db8::1")

func benchLimiter() *Limiter {
	return New(Config{
		IPRate: 1e9, IPBurst: 1_000_000,
		ChainRate: benchMax, ChainWindow: time.Minute,
		SequenceMax: 1_000_000,
	})
}

// BenchmarkAllowChainSteady feeds one key a steady rate: its window holds
// benchMax/2 live events, and each call admits one and expires one.
func BenchmarkAllowChainSteady(b *testing.B) {
	l := benchLimiter()
	step := int64(time.Minute) / (benchMax / 2)
	var now int64
	for range benchMax / 2 {
		allowChainAt(l, benchIP, 0xfeed, now)
		now += step
	}
	for b.Loop() {
		if !allowChainAt(l, benchIP, 0xfeed, now) {
			b.Fatal("rejected below max")
		}
		now += step
	}
}

// BenchmarkAllowChainSaturated calls AllowChain for one key that already
// holds max events in its one-minute window, so every call is rejected: a
// flow in a NACK storm.
func BenchmarkAllowChainSaturated(b *testing.B) {
	l := benchLimiter()
	for range benchMax {
		l.AllowChain(benchIP, 0xfeed)
	}
	for b.Loop() {
		if l.AllowChain(benchIP, 0xfeed) {
			b.Fatal("admitted past max")
		}
	}
}

// BenchmarkAllowChainNewKey calls AllowChain with a new HashKey every time,
// measuring what a new key costs. A fresh Limiter every 512 keys keeps the
// benchmark's memory bounded.
func BenchmarkAllowChainNewKey(b *testing.B) {
	var l *Limiter
	var h uint64
	for b.Loop() {
		if h%512 == 0 {
			l = benchLimiter()
		}
		h++
		l.AllowChain(benchIP, h)
	}
}
