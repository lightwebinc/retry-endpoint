package ingress

import (
	"encoding/binary"
	"unsafe"

	"golang.org/x/sys/unix"
)

// batchReader receives up to len(slots) datagrams per recvmmsg call. The
// headers, vectors and address buffers are allocated once and re-pointed at
// the caller's slots on every call.
type batchReader struct {
	fd    int
	hdrs  []unix.Mmsghdr
	iovs  []unix.Iovec
	names []unix.RawSockaddrInet6
	// ctl holds each message's control data: with receive GRO on, the kernel
	// says there how long the datagrams coalesced into the message are.
	ctl [][groCtlSize]byte
}

func newBatchReader(fd, n int) *batchReader {
	return &batchReader{
		fd:    fd,
		hdrs:  make([]unix.Mmsghdr, n),
		iovs:  make([]unix.Iovec, n),
		names: make([]unix.RawSockaddrInet6, n),
		ctl:   make([][groCtlSize]byte, n),
	}
}

// Receive GRO ([Worker.SetRecvGRO]).
//
// A sender that hands the kernel runs of equal datagrams (UDP GSO, as the
// proxy's group sends are) puts them on the wire back to back, and a receiving
// socket with UDP_GRO set gets such a run back as ONE message: the datagrams'
// payloads concatenated, with their common length in a control message. The
// kernel then queues and hands over one buffer where it handled several, and
// on a host whose cores are slow that is most of what a 10G flow costs it
// before the copy. The slot carries the run and its segment length, and the
// processor splits it.
//
// The option is the receiving socket's own: multicast delivery hands a
// coalesced run to each socket that asked for it and splits it again for one
// that did not.

// groCtlSize is the control buffer per message: room for the one cmsg the
// kernel adds (a cmsghdr and the 4-byte segment size, aligned).
const groCtlSize = 32

// enableGRO asks for coalesced runs on fd. An error means the kernel has no
// UDP_GRO; the socket then delivers single datagrams as before.
func enableGRO(fd int) error {
	return unix.SetsockoptInt(fd, unix.IPPROTO_UDP, unix.UDP_GRO, 1)
}

// segSize is the length of the datagrams coalesced into message i, or 0 when
// no UDP_GRO control message came with it.
func (r *batchReader) segSize(i int) int {
	ctl := r.ctl[i][:r.hdrs[i].Hdr.Controllen]
	for len(ctl) >= unix.SizeofCmsghdr {
		h := (*unix.Cmsghdr)(unsafe.Pointer(&ctl[0]))
		if h.Len < unix.SizeofCmsghdr || int(h.Len) > len(ctl) {
			return 0
		}
		if h.Level == unix.IPPROTO_UDP && h.Type == unix.UDP_GRO && h.Len >= unix.SizeofCmsghdr+4 {
			// The kernel writes an int in host byte order.
			return int(binary.NativeEndian.Uint32(ctl[unix.SizeofCmsghdr:]))
		}
		next := (int(h.Len) + unix.SizeofPtr - 1) &^ (unix.SizeofPtr - 1)
		if next >= len(ctl) {
			return 0
		}
		ctl = ctl[next:]
	}
	return 0
}

// read fills slots[:n] and returns n. MSG_WAITFORONE blocks (up to the
// socket's SO_RCVTIMEO) for the first datagram only, then takes whatever else
// is already queued.
func (r *batchReader) read(slots []*slot) (int, error) {
	for i, s := range slots {
		r.iovs[i].Base = &s.buf[0]
		r.iovs[i].SetLen(len(s.buf))
		h := &r.hdrs[i].Hdr
		h.Name = (*byte)(unsafe.Pointer(&r.names[i]))
		h.Namelen = unix.SizeofSockaddrInet6
		h.Iov = &r.iovs[i]
		h.SetIovlen(1)
		h.Control = &r.ctl[i][0]
		h.SetControllen(groCtlSize)
		h.Flags = 0
		r.hdrs[i].Len = 0
	}
	n, _, e := unix.Syscall6(unix.SYS_RECVMMSG, uintptr(r.fd),
		uintptr(unsafe.Pointer(&r.hdrs[0])), uintptr(len(slots)),
		unix.MSG_WAITFORONE, 0, 0)
	if e != 0 {
		return 0, e
	}
	for i := range int(n) {
		s := slots[i]
		s.n = int(r.hdrs[i].Len)
		if s.seg = r.segSize(i); s.seg >= s.n {
			s.seg = 0 // a run of one
		}
		s.trunc = r.hdrs[i].Hdr.Flags&unix.MSG_TRUNC != 0
		s.v6 = r.names[i].Family == unix.AF_INET6
		s.from = r.names[i].Addr
	}
	return int(n), nil
}
