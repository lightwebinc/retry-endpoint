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
	w := New(&net.Interface{Index: 1, Name: "lo"}, 9001, nil, mc, nil, 30*time.Second, true)
	if w.port != 9001 {
		t.Errorf("port=%d", w.port)
	}
	if w.ttl != 30*time.Second {
		t.Errorf("ttl=%v", w.ttl)
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
	w := New(&net.Interface{Name: "lo"}, 0, nil, &errCache{}, nil, time.Minute, false)
	raw := buildRaw(t, 0x11, 0x22, nil)
	w.processFrame(raw) // must not panic; just logs
}

// secondaryErrCache returns success on prefix 0x01 but error on 0x00.
type secondaryErrCache struct{ mockCache }

func (e *secondaryErrCache) Store(key, val []byte, ttl time.Duration) error {
	if len(key) > 0 && key[0] == 0x00 {
		return errors.New("secondary fail")
	}
	return e.mockCache.Store(key, val, ttl)
}

func TestProcessFrame_SecondaryStoreError(t *testing.T) {
	w := New(&net.Interface{Name: "lo"}, 0, nil, &secondaryErrCache{}, nil, time.Minute, false)
	raw := buildRaw(t, 0x11, 0x22, nil)
	w.processFrame(raw) // must not panic; primary succeeds, secondary logs
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

	w := New(iface, port, nil, &mockCache{}, nil, time.Minute, false)
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
