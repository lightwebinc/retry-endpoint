//go:build !linux

package ingress

import "golang.org/x/sys/unix"

// batchReader receives one datagram per call where recvmmsg is not wired up.
// The pipeline behind it is the same: receive stays decoupled from
// processing.
type batchReader struct {
	fd int
}

func newBatchReader(fd, _ int) *batchReader {
	return &batchReader{fd: fd}
}

func (r *batchReader) read(slots []*slot) (int, error) {
	s := slots[0]
	n, from, err := unix.Recvfrom(r.fd, s.buf, 0)
	if err != nil {
		return 0, err
	}
	s.n, s.trunc, s.v6 = n, false, false
	if sa, ok := from.(*unix.SockaddrInet6); ok {
		s.from, s.v6 = sa.Addr, true
	}
	return 1, nil
}
