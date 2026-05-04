package ratelimit

import (
	"net"
	"testing"
	"time"
)

func TestIPRateLimit(t *testing.T) {
	l := New(Config{
		IPRate:       2,
		IPBurst:      2,
		SenderRate:   100,
		SenderWindow: time.Minute,
		SequenceMax:  100,
	})

	ip := net.ParseIP("::1")
	var id uint32

	// First two should pass (burst).
	for i := uint32(0); i < 2; i++ {
		ok, _ := l.Allow(ip, id, i)
		if !ok {
			t.Fatalf("request %d should be allowed within burst", i)
		}
	}

	// Third should be dropped.
	ok, level := l.Allow(ip, id, 99)
	if ok {
		t.Fatal("expected IP rate limit to drop request")
	}
	if level != LevelIP {
		t.Fatalf("expected level %q, got %q", LevelIP, level)
	}
}

func TestSenderRateLimit(t *testing.T) {
	l := New(Config{
		IPRate:       1000,
		IPBurst:      1000,
		SenderRate:   2,
		SenderWindow: time.Minute,
		SequenceMax:  100,
	})

	ip := net.ParseIP("::1")
	var sender uint32 = 0xAB

	// First two requests in window should pass.
	for i := uint32(0); i < 2; i++ {
		ok, _ := l.Allow(ip, sender, i)
		if !ok {
			t.Fatalf("request %d should be allowed within sender window", i)
		}
	}

	// Third should be dropped.
	ok, level := l.Allow(ip, sender, 99)
	if ok {
		t.Fatal("expected sender rate limit to drop request")
	}
	if level != LevelSender {
		t.Fatalf("expected level %q, got %q", LevelSender, level)
	}
}

func TestSequenceRateLimit(t *testing.T) {
	l := New(Config{
		IPRate:       1000,
		IPBurst:      1000,
		SenderRate:   1000,
		SenderWindow: time.Minute,
		SequenceMax:  3,
	})

	ip := net.ParseIP("::1")
	var sender uint32
	const seqID = uint32(42)

	// First three requests for same seqID should pass.
	for i := 0; i < 3; i++ {
		ok, _ := l.Allow(ip, sender, seqID)
		if !ok {
			t.Fatalf("request %d should be allowed within sequence max", i)
		}
	}

	// Fourth should be dropped.
	ok, level := l.Allow(ip, sender, seqID)
	if ok {
		t.Fatal("expected sequence rate limit to drop request")
	}
	if level != LevelSequence {
		t.Fatalf("expected level %q, got %q", LevelSequence, level)
	}
}

func TestDifferentSequencesIndependent(t *testing.T) {
	l := New(Config{
		IPRate:       1000,
		IPBurst:      1000,
		SenderRate:   1000,
		SenderWindow: time.Minute,
		SequenceMax:  1,
	})

	ip := net.ParseIP("::1")
	var sender uint32

	// Each unique seqID gets its own counter.
	for i := uint32(0); i < 5; i++ {
		ok, _ := l.Allow(ip, sender, i)
		if !ok {
			t.Fatalf("seqID %d should be allowed (first request)", i)
		}
	}
}
