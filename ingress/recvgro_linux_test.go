package ingress

import (
	"encoding/binary"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// TestBatchReaderSegSize: a message that came with a UDP_GRO control message
// reports its datagrams' length, and one without reports none.
func TestBatchReaderSegSize(t *testing.T) {
	r := newBatchReader(-1, 2)
	h := (*unix.Cmsghdr)(unsafe.Pointer(&r.ctl[0][0]))
	h.Level, h.Type = unix.IPPROTO_UDP, unix.UDP_GRO
	h.SetLen(unix.SizeofCmsghdr + 4)
	binary.NativeEndian.PutUint32(r.ctl[0][unix.SizeofCmsghdr:], 1000)
	r.hdrs[0].Hdr.SetControllen(unix.CmsgSpace(4))
	r.hdrs[1].Hdr.SetControllen(0)
	if got := r.segSize(0); got != 1000 {
		t.Fatalf("coalesced message: segment size %d, want 1000", got)
	}
	if got := r.segSize(1); got != 0 {
		t.Fatalf("single datagram: segment size %d, want 0", got)
	}
}

// TestRecvGROLoopback sends one GSO message of five 1000-byte datagrams and a
// 300-byte tail over loopback to a socket with receive GRO on and reads it
// through the batch reader: every byte arrives, and a coalesced run carries
// its segment length, however the kernel chose to hand it over.
func TestRecvGROLoopback(t *testing.T) {
	rfd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM, 0)
	if err != nil {
		t.Skip("no IPv6 datagram socket:", err)
	}
	defer func() { _ = unix.Close(rfd) }()
	lo := [16]byte{15: 1}
	if err := unix.Bind(rfd, &unix.SockaddrInet6{Addr: lo}); err != nil {
		t.Skip("cannot bind ::1:", err)
	}
	if err := enableGRO(rfd); err != nil {
		t.Skip("kernel has no UDP_GRO:", err)
	}
	tv := unix.NsecToTimeval((500 * time.Millisecond).Nanoseconds())
	_ = unix.SetsockoptTimeval(rfd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)
	sa, err := unix.Getsockname(rfd)
	if err != nil {
		t.Fatal(err)
	}

	sfd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(sfd) }()
	const seg = 1000
	payload := make([]byte, 5*seg+300)
	for i := range payload {
		payload[i] = byte(i / seg) // each datagram carries its index
	}
	oob := make([]byte, unix.CmsgSpace(2))
	h := (*unix.Cmsghdr)(unsafe.Pointer(&oob[0]))
	h.Level, h.Type = unix.IPPROTO_UDP, unix.UDP_SEGMENT
	h.SetLen(unix.CmsgLen(2))
	binary.NativeEndian.PutUint16(oob[unix.CmsgLen(0):], seg)
	if err := unix.Sendmsg(sfd, payload, oob, sa, 0); err != nil {
		t.Skip("kernel refused UDP_SEGMENT:", err)
	}

	r := newBatchReader(rfd, 8)
	slots := make([]*slot, 8)
	for i := range slots {
		slots[i] = &slot{buf: make([]byte, slotSize)}
	}
	var got [][]byte
	coalesced := false
	for total := 0; total < len(payload); {
		n, err := r.read(slots)
		if err != nil {
			t.Fatalf("read after %d of %d bytes: %v", total, len(payload), err)
		}
		for _, s := range slots[:n] {
			pkt := s.buf[:s.n]
			if s.seg > 0 {
				coalesced = true
				for len(pkt) > s.seg {
					got = append(got, append([]byte(nil), pkt[:s.seg]...))
					pkt = pkt[s.seg:]
				}
			}
			got = append(got, append([]byte(nil), pkt...))
			total += s.n
		}
	}
	t.Logf("kernel coalesced the run: %v", coalesced)
	if len(got) != 6 {
		t.Fatalf("%d datagrams, want 6", len(got))
	}
	for i, d := range got {
		want := seg
		if i == 5 {
			want = 300
		}
		if len(d) != want || d[0] != byte(i) || d[len(d)-1] != byte(i) {
			t.Fatalf("datagram %d: %d bytes, first %d last %d", i, len(d), d[0], d[len(d)-1])
		}
	}
}
