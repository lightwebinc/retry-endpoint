package config

import (
	"strings"
	"testing"
)

func TestLoad_TeeListen(t *testing.T) {
	c, err := loadWithArgs(t, withEgress(t, "-tee-listen=[::1]:9002")...)
	if err != nil {
		t.Fatalf("tee-listen loopback: %v", err)
	}
	if !c.TeeListen.IsValid() || c.TeeListen.String() != "[::1]:9002" {
		t.Errorf("TeeListen = %v, want [::1]:9002", c.TeeListen)
	}
	if !c.MCJoinEnabled {
		t.Error("MCJoinEnabled default must be true")
	}
}

func TestLoad_TeeListenDisabledByDefault(t *testing.T) {
	c, err := loadWithArgs(t, withEgress(t)...)
	if err != nil {
		t.Fatalf("minimal load: %v", err)
	}
	if c.TeeListen.IsValid() {
		t.Errorf("TeeListen default = %v, want invalid (disabled)", c.TeeListen)
	}
}

func TestLoad_TeeListenRejectsNonLoopback(t *testing.T) {
	// The tee envelope asserts frame sources; a non-loopback bind would
	// accept forged attribution off the network. Fail closed.
	for _, addr := range []string{
		"[fd00::1]:9002",          // ULA
		"[2001:db8::1]:9002",      // global
		"127.0.0.1:9002",          // v4 loopback: socket is AF_INET6-only
		"[::ffff:127.0.0.1]:9002", // mapped v4 loopback
		"[::1]:0",                 // no explicit port
		"localhost:9002",          // not a literal
	} {
		if _, err := loadWithArgs(t, withEgress(t, "-tee-listen="+addr)...); err == nil {
			t.Errorf("tee-listen=%s should error", addr)
		}
	}
}

func TestLoad_JoinDisabledRequiresTee(t *testing.T) {
	// A retry with neither multicast join nor tee ingest answers every NACK
	// with MISS while its process looks healthy — refuse to start that way.
	_, err := loadWithArgs(t, withEgress(t, "-mc-join-enabled=false")...)
	if err == nil {
		t.Fatal("mc-join-enabled=false without -tee-listen should error")
	}
	if !strings.Contains(err.Error(), "tee-listen") {
		t.Errorf("error should name -tee-listen, got: %v", err)
	}

	c, err := loadWithArgs(t, withEgress(t, "-mc-join-enabled=false", "-tee-listen=[::1]:9002")...)
	if err != nil {
		t.Fatalf("tee-only load: %v", err)
	}
	if c.MCJoinEnabled {
		t.Error("MCJoinEnabled = true, want false")
	}
}
