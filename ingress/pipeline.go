package ingress

import (
	"context"
	"encoding/binary"
	"net/netip"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/teewire"
)

// Receive pipeline shared by the multicast and tee sockets.
//
// One goroutine drains the socket in batches (recvmmsg on Linux) into a ring
// of reusable slots and hands each datagram to one of a few processing
// goroutines over a bounded queue. The socket is drained while frames are
// decoded and stored, so a slow store, a GC assist or a burst no longer backs
// up into the kernel buffer one datagram at a time.
//
// Ordering: a datagram goes to the processor chosen by its cache key
// (HashKey, SeqNum), so every write of one key, including the duplicate a
// node can receive from both the multicast and the tee feed, is made by one
// goroutine in arrival order. Nothing else needs ordering: the key is unique
// per frame, the cache is safe for concurrent use (the memory backend locks
// per shard and reads its clock under that lock), and several ingress workers
// already store into it concurrently.
//
// Ownership: a slot's bytes are valid only until the processor returns it to
// the free list. The cache does not retain the slice it is given (the memory
// backend stores a clone, the remote backends serialize before returning), the
// same contract the single reused receive buffer relied on before.

const (
	// slotSize holds the largest UDP datagram IPv6 carries without jumbograms,
	// so a datagram is never truncated by the buffer.
	slotSize = 64 << 10
	// recvBatchSize is the most datagrams one recvmmsg call returns.
	recvBatchSize = 64
	// queueFullWait bounds how long the receive loop waits for room in a full
	// queue before it drops the datagram. While it waits the kernel buffer
	// keeps absorbing the burst; past it a processor is stalled and blocking
	// further would only move the loss into the kernel, uncounted by reason.
	queueFullWait = 100 * time.Millisecond

	// DefaultQueueDepth is the datagrams queued between receive and
	// processing per socket when [Worker.SetPipeline] is not called.
	DefaultQueueDepth = 1024
	// DefaultProcs is the processing goroutines per socket when
	// [Worker.SetPipeline] is not called.
	DefaultProcs = 4
)

// slot is one received datagram in a reusable buffer.
type slot struct {
	buf   []byte
	n     int
	from  [16]byte // datagram source when v6
	v6    bool
	trunc bool // the datagram did not fit buf
}

// SetPipeline sets the receive queue depth (datagrams per socket) and the
// processing goroutines per socket. Values below 1 keep the defaults. Must be
// called before [Worker.Run] or [Worker.RunTee].
func (w *Worker) SetPipeline(depth, procs int) {
	w.queueDepth, w.procs = depth, procs
}

func (w *Worker) pipelineSize() (depth, procs int) {
	depth, procs = w.queueDepth, w.procs
	if depth < 1 {
		depth = DefaultQueueDepth
	}
	if procs < 1 {
		procs = DefaultProcs
	}
	return depth, procs
}

func (w *Worker) dropped(reason string) {
	if w.rec != nil {
		w.rec.FrameDropped(reason)
	}
}

// receive drains fd until ctx is canceled or the socket is closed. tee selects
// the tee socket's handling (teewire envelopes accepted, datagrams counted by
// form); otherwise each datagram is a frame labeled by its source address.
// It returns only after every processor has exited.
func (w *Worker) receive(ctx context.Context, fd int, tee bool) {
	depth, procs := w.pipelineSize()
	per := (depth + procs - 1) / procs
	// Every slot is in exactly one place: the free list, the receiver's batch,
	// a queue, or a processor. Sized so the free list can always refill the
	// batch and a processor can always return its slot without blocking.
	total := per*procs + procs + recvBatchSize
	free := make(chan *slot, total)
	for range total {
		free <- &slot{buf: make([]byte, slotSize)}
	}

	queues := make([]chan *slot, procs)
	var wg sync.WaitGroup
	for i := range queues {
		q := make(chan *slot, per)
		queues[i] = q
		wg.Go(func() {
			p := processor{w: w, tee: tee}
			for s := range q {
				p.handle(s)
				free <- s
			}
		})
	}
	defer func() {
		for _, q := range queues {
			close(q)
		}
		wg.Wait()
	}()

	batch := make([]*slot, recvBatchSize)
	for i := range batch {
		batch[i] = <-free
	}
	r := newBatchReader(fd, recvBatchSize)
	wait := time.NewTimer(queueFullWait)
	wait.Stop()
	done := ctx.Done()
	for {
		select {
		case <-done:
			return
		default:
		}
		n, err := r.read(batch)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK || err == unix.EINTR {
				continue
			}
			if err == unix.EBADF || err == unix.EINVAL || ctx.Err() != nil {
				return
			}
			w.log.Error("receive error", "tee", tee, "err", err)
			continue
		}
		for i := range n {
			s := batch[i]
			if s.trunc {
				w.dropped("truncated")
				continue
			}
			if s.n == 0 {
				continue
			}
			q := queues[0]
			if procs > 1 {
				q = queues[shardOf(s.buf[:s.n], tee)%uint64(procs)]
			}
			select {
			case q <- s:
			default:
				wait.Reset(queueFullWait)
				select {
				case q <- s:
					wait.Stop()
				case <-wait.C:
					w.dropped("queue_full")
					continue // the slot stays in the batch
				case <-done:
					wait.Stop()
					return
				}
			}
			batch[i] = <-free
		}
	}
}

// shardOf spreads datagrams over processors by cache key. HashKey and SeqNum
// sit at offsets 40 and 48 in every frame class this cache stores; a datagram
// too short to carry them goes to processor 0 and fails decode there.
func shardOf(pkt []byte, tee bool) uint64 {
	if tee && teewire.IsEncap(pkt) {
		if len(pkt) < teewire.HeaderSize {
			return 0
		}
		pkt = pkt[teewire.HeaderSize:]
	}
	if len(pkt) < 56 {
		return 0
	}
	h := binary.BigEndian.Uint64(pkt[40:48]) ^ binary.BigEndian.Uint64(pkt[48:56])*0x9E3779B97F4A7C15
	return h ^ h>>32
}

// processor is one processing goroutine's state. The source label is formatted
// once per change of source, not once per datagram: a socket sees a handful
// of sources, each in long runs.
type processor struct {
	w       *Worker
	tee     bool
	lastSrc [16]byte
	src     string
}

func (p *processor) label(a [16]byte) string {
	if p.src == "" || a != p.lastSrc {
		p.lastSrc, p.src = a, netip.AddrFrom16(a).String()
	}
	return p.src
}

func (p *processor) handle(s *slot) {
	w := p.w
	pkt := s.buf[:s.n]
	if !p.tee {
		// The datagram's source address IS the fabric source of the frame
		// (SSM: one (S,G) per publisher); it labels the per-source cache
		// counters so a retry starved of ONE source's frames is a zero rate,
		// not an invisible absence.
		if s.v6 {
			w.processFrame(pkt, p.label(s.from))
		} else {
			w.processFrame(pkt, "")
		}
		return
	}
	if teewire.IsEncap(pkt) {
		src, payload, err := teewire.Decap(pkt)
		if err != nil {
			w.dropped("tee_decap_error")
			if w.debug {
				w.log.Debug("tee decap error", "err", err, "len", s.n)
			}
			return
		}
		if w.rec != nil {
			w.rec.TeeDatagram("encap")
		}
		w.processFrame(payload, p.label(src.Addr().As16()))
		return
	}
	// Raw form: the proxy mirrors its own egress verbatim. Label with the
	// datagram source (loopback), the same value a wildcard-bound data socket
	// would have seen for it.
	src := ""
	if s.v6 {
		src = p.label(s.from)
	}
	if w.rec != nil {
		w.rec.TeeDatagram("raw")
	}
	w.processFrame(pkt, src)
}
