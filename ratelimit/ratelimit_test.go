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

// TestSequenceLimiterSlidingWindow guards against regressing to the legacy
// monotonic-counter behaviour: once a SequenceID's window elapses, new
// requests must be admitted again rather than permanently blocked.
func TestSequenceLimiterSlidingWindow(t *testing.T) {
	l := New(Config{
		IPRate:         1000,
		IPBurst:        1000,
		SenderRate:     1000,
		SenderWindow:   time.Minute,
		SequenceMax:    2,
		SequenceWindow: 50 * time.Millisecond,
	})

	ip := net.ParseIP("::1")
	var sender uint32
	const seqID = uint32(7)

	// Fill the window.
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow(ip, sender, seqID); !ok {
			t.Fatalf("request %d should be allowed within window", i)
		}
	}
	// Third in the same window must be dropped.
	if ok, level := l.Allow(ip, sender, seqID); ok || level != LevelSequence {
		t.Fatalf("expected sequence drop, got ok=%v level=%q", ok, level)
	}

	// After the window elapses the limiter must self-heal.
	time.Sleep(75 * time.Millisecond)
	if ok, _ := l.Allow(ip, sender, seqID); !ok {
		t.Fatal("expected sequence limiter to admit requests after window expiry")
	}
}

// TestSequenceLimiterDefaultWindow verifies that a zero-value SequenceWindow
// is replaced with a sane default instead of locking the limiter open/closed.
func TestSequenceLimiterDefaultWindow(t *testing.T) {
	l := New(Config{
		IPRate:       1000,
		IPBurst:      1000,
		SenderRate:   1000,
		SenderWindow: time.Minute,
		SequenceMax:  1,
		// SequenceWindow deliberately left zero.
	})
	ip := net.ParseIP("::1")
	if ok, _ := l.Allow(ip, 0, 1); !ok {
		t.Fatal("first request must pass regardless of window default")
	}
	if ok, level := l.Allow(ip, 0, 1); ok || level != LevelSequence {
		t.Fatalf("second request must be sequence-limited; got ok=%v level=%q", ok, level)
	}
}
