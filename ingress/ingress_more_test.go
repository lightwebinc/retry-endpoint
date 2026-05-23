package ingress

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestNew_Construction(t *testing.T) {
	mc := &mockCache{}
	ttls := TTLConfig{Tx: 30 * time.Second, Block: 31 * time.Second, Subtree: 32 * time.Second, Anchor: 33 * time.Second}
	w := New(&net.Interface{Index: 1, Name: "lo"}, 9001, nil, mc, nil, ttls, true)
	if w.port != 9001 {
		t.Errorf("port=%d", w.port)
	}
	if w.ttls != ttls {
		t.Errorf("ttls=%+v, want %+v", w.ttls, ttls)
	}
	if !w.debug {
		t.Error("debug not preserved")
	}
}

type errCache struct{ mockCache }

func (e *errCache) Store(_, _ []byte, _ time.Duration) error {
	return errors.New("synthetic")
}

func TestProcessFrame_PrimaryStoreError(t *testing.T) {
	w := New(&net.Interface{Name: "lo"}, 0, nil, &errCache{}, nil, TTLConfig{Tx: time.Minute, Block: time.Minute, Subtree: time.Minute, Anchor: time.Minute}, false)
	raw := buildRaw(t, 0x1111111111111111, 0x22, nil)
	w.processFrame(raw) // must not panic; just logs
}

func TestRun_CtxCancelExits(t *testing.T) {
	// Open an existing socket on a port so our bind fails predictably... actually
	// the simpler path: bind to a real free port and immediately cancel.
	probe, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("udp6 unavailable: %v", err)
	}
	port := probe.LocalAddr().(*net.UDPAddr).Port
	_ = probe.Close()

	ifs, _ := net.Interfaces()
	var iface *net.Interface
	for i := range ifs {
		if ifs[i].Flags&net.FlagLoopback != 0 {
			iface = &ifs[i]
			break
		}
	}
	if iface == nil {
		t.Skip("no loopback")
	}

	w := New(iface, port, nil, &mockCache{}, nil, TTLConfig{Tx: time.Minute, Block: time.Minute, Subtree: time.Minute, Anchor: time.Minute}, false)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run returned err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit")
	}
}
