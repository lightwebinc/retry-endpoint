package ingress

import (
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
}

func newBatchReader(fd, n int) *batchReader {
	return &batchReader{
		fd:    fd,
		hdrs:  make([]unix.Mmsghdr, n),
		iovs:  make([]unix.Iovec, n),
		names: make([]unix.RawSockaddrInet6, n),
	}
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
		h.Control = nil
		h.SetControllen(0)
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
		s.trunc = r.hdrs[i].Hdr.Flags&unix.MSG_TRUNC != 0
		s.v6 = r.names[i].Family == unix.AF_INET6
		s.from = r.names[i].Addr
	}
	return int(n), nil
}
