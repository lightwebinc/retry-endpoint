package ingress

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	scache "github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/teering"
	"github.com/lightwebinc/shard-common/teewire"

	"github.com/lightwebinc/retry-endpoint/cache"
)

// Shared-memory tee ingest. A co-located proxy whose lanes carry the retry
// tee through a teering ring (shard-proxy -retry-tee-ring) writes every
// mirrored datagram into a file on /dev/shm; RunTeeRing attaches to it and
// feeds the datagrams to the same processors as the tee socket, in the raw
// form (the proxy mirrors its own egress verbatim), labeled as from
// loopback like the socket tee's raw datagrams. The loopback UDP tee costs
// the endpoint a recvmmsg copy per datagram and the kernel the loopback
// delivery and software GSO segmentation of every GSO message; the ring is
// one copy into a slot.
//
// Trust: the ring file is written by whoever can open it. It carries raw
// frames only: a ring datagram in the teewire envelope form (which asserts
// a frame source) is dropped and counted, so a ring writer cannot forge
// per-source attribution. Keep the file on a tmpfs the node's processes own.

const (
	// ringWaitMax bounds the consumer's sleep between polls of an empty ring.
	ringWaitMax = time.Millisecond
	// ringAttachRetry is how often an absent ring file is retried.
	ringAttachRetry = time.Second
)

// RunTeeRing drains the ring at path until ctx is canceled. A file that is
// not there yet (the proxy not started) is waited for; a producer restart
// (the file reset in place) is followed.
func (w *Worker) RunTeeRing(ctx context.Context, path string) error {
	if w.rec != nil {
		w.rec.WorkerReady()
		defer w.rec.WorkerDone()
	}
	for ctx.Err() == nil {
		c, err := teering.Attach(path)
		if err != nil {
			// A producer creates the file and then widens its mode, so a
			// consumer under another uid can be refused in between: retried,
			// at warn level so a refusal that persists stays visible.
			switch {
			case errors.Is(err, os.ErrPermission):
				w.log.Warn("tee ring not readable, retrying", "path", path, "err", err)
			case errors.Is(err, os.ErrNotExist), errors.Is(err, teering.ErrBadHeader):
				w.log.Info("tee ring not ready, waiting", "path", path, "err", err)
			default:
				return fmt.Errorf("ingress tee ring: %w", err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(ringAttachRetry):
			}
			continue
		}
		w.log.Info("tee ring ingest ready", "path", path)
		// receiveRing returns when the file was replaced (a new producer
		// made a new file); attach to the new one.
		w.receiveRing(ctx, c)
		_ = c.Close()
		if ctx.Err() == nil {
			w.log.Warn("tee ring file replaced, re-attaching", "path", path)
			w.dropped("ring_replaced")
		}
	}
	return nil
}

// receiveRing is receive's fan-out over a ring instead of a socket: one
// goroutine copies datagrams out of the ring and hands each to the processor
// its cache key selects.
//
// With a cache that keeps a handed-over value (the in-memory backend), a
// datagram is copied once: out of the ring into memory of its own, which the
// cache then keeps. Otherwise it is copied into a pooled slot, and the cache
// copies it again.
func (w *Worker) receiveRing(ctx context.Context, c *teering.Consumer) {
	depth, procs := w.pipelineSize()
	per := (depth + procs - 1) / procs
	total := per*procs + procs + recvBatchSize
	pw := w
	owning, owned := cache.Owning(w.cache)
	leaser, leased := cache.Leasing(w.cache)
	if owned {
		ow := *w
		ow.cache = owning
		pw = &ow
	}
	free := make(chan *slot, total)
	for range total {
		s := &slot{}
		if !owned && !leased {
			s.buf = make([]byte, slotSize)
		}
		free <- s
	}
	queues := make([]chan *slot, procs)
	var wg sync.WaitGroup
	for i := range queues {
		q := make(chan *slot, per)
		queues[i] = q
		wg.Go(func() {
			p := processor{w: pw, tee: true}
			var view *cache.LeaseView
			if leased {
				// A view of the cache of this goroutine's own: its Store
				// commits the datagram it was handed in place.
				lw := *w
				view = leaser.View()
				lw.cache = view
				p.w = &lw
			}
			for s := range q {
				if view == nil {
					p.handle(s)
				} else {
					view.Handle(s.lease, func() { p.handle(s) })
					s.lease, s.buf = scache.Lease{}, nil
				}
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
	// back returns a slot no processor will see, and its lease with it.
	back := func(s *slot) {
		if s.lease.Buf != nil {
			leaser.Release(s.lease)
			s.lease, s.buf = scache.Lease{}, nil
		}
		free <- s
	}
	var loopback [16]byte
	loopback[15] = 1
	done := ctx.Done()
	wait := time.NewTimer(queueFullWait)
	wait.Stop()
	var lost uint64
	idleSince := time.Time{}
	for {
		select {
		case <-done:
			return
		default:
		}
		s := <-free
		var n int
		if leased {
			// One copy, into the cache's own memory. When the cache cannot
			// lease now (see cache/arena) the datagram takes memory of its
			// own and the cache copies it.
			if v := c.Peek(); v != nil {
				if s.lease = leaser.Lease(v); s.lease.Buf != nil {
					s.buf = s.lease.Buf
				} else {
					s.buf = append([]byte(nil), v...)
				}
				n = len(v)
				c.Advance()
			}
		} else if owned {
			// One copy, into memory the cache will keep.
			if v := c.Peek(); v != nil {
				s.buf = append([]byte(nil), v...)
				n = len(v)
				c.Advance()
			}
		} else {
			n = c.Next(s.buf)
		}
		if n == 0 {
			back(s)
			if l := c.Lost(); l != lost {
				for range l - lost {
					w.dropped("ring_lost")
				}
				lost = l
			}
			select {
			case <-done:
				return
			default:
			}
			// An empty ring for a second: is it still the producer's file?
			now := time.Now()
			if idleSince.IsZero() {
				idleSince = now
			} else if now.Sub(idleSince) > time.Second {
				idleSince = now
				if c.Stale() {
					return
				}
			}
			c.Wait(ringWaitMax)
			continue
		}
		idleSince = time.Time{}
		c.Ready()
		if teewire.IsEncap(s.buf[:n]) {
			// Raw frames only: an envelope would assert a source.
			w.dropped("ring_encap")
			back(s)
			continue
		}
		s.n, s.v6, s.from, s.trunc = n, true, loopback, false
		q := queues[0]
		if procs > 1 {
			q = queues[shardOf(s.buf[:s.n], true)%uint64(procs)]
		}
		select {
		case q <- s:
		default:
			wait.Reset(queueFullWait)
			select {
			case q <- s:
				if !wait.Stop() {
					<-wait.C
				}
			case <-wait.C:
				w.dropped("queue_full")
				back(s)
			case <-done:
				back(s)
				return
			}
		}
	}
}
