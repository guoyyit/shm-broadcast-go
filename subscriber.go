package shmbroadcast

import (
	"sync"
	"sync/atomic"
)

// Subscriber tracks an independent read cursor on a segment. Its progress
// is published to the segment's registry for diagnostics and, on
// PolicyBlock segments, for writer backpressure; it never affects other
// subscribers.
//
// A Subscriber is safe for concurrent use, but reads are serialized.
type Subscriber struct {
	seg    *Segment
	id     uint64
	idx    int    // registry slot
	gen    uint64 // generation this cursor is valid for
	mu     sync.Mutex
	cursor uint64 // next sequence to read
	closed atomic.Bool
}

// Subscribe registers a new subscriber starting at the oldest retained
// event, so it can replay everything still in the ring.
func (s *Segment) Subscribe() (*Subscriber, error) {
	return s.subscribeAt(s.oldestSeq(), s.Generation())
}

// SubscribeAt registers a subscriber resuming from a previously saved
// Position. A position from an older generation fails with
// ErrGenerationMismatch; a sequence that is no longer retained subscribes
// anyway and the first read reports ErrLapped, so gaps stay visible.
func (s *Segment) SubscribeAt(pos Position) (*Subscriber, error) {
	if pos.Generation != s.Generation() {
		return nil, ErrGenerationMismatch
	}
	seq := pos.Seq
	if seq < 1 {
		seq = 1
	}
	if next := s.loadNextSeq(); seq > next {
		seq = next
	}
	return s.subscribeAt(seq, pos.Generation)
}

func (s *Segment) subscribeAt(seq uint64, gen uint64) (*Subscriber, error) {
	if s.isClosed() {
		return nil, ErrClosed
	}
	id, idx, err := s.claimSubscriber(seq)
	if err != nil {
		return nil, err
	}
	return &Subscriber{seg: s, id: id, idx: idx, gen: gen, cursor: seq}, nil
}

// Next returns a copy of the next event. The copy is validated against
// concurrent overwrites, so the returned bytes are always a complete,
// untorn event.
//
// Errors: ErrNoData (caught up), ErrLapped (events were overwritten
// before they could be read; the cursor was advanced to the oldest
// retained event), ErrGenerationMismatch (the segment was recreated;
// close and reopen the subscriber), ErrClosed.
func (sub *Subscriber) Next() ([]byte, error) {
	var out []byte
	err := sub.NextView(func(p []byte) error {
		out = make([]byte, len(p))
		copy(out, p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// NextView passes a zero-copy view of the next event, valid only during
// fn, to fn. The view aliases shared memory directly; it must not be
// retained. After fn returns, the read is validated: if the writer
// concurrently overwrote the slot (only possible with PolicyOverwrite),
// NextView returns ErrLapped and any observation fn made must be
// discarded. If fn returns an error, the cursor is not advanced.
func (sub *Subscriber) NextView(fn func([]byte) error) error {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	if sub.closed.Load() || sub.seg.isClosed() {
		return ErrClosed
	}
	if sub.gen != sub.seg.Generation() {
		return ErrGenerationMismatch
	}
	off := sub.seg.slotOff(sub.cursor)
	raw := atomic.LoadUint64(sub.seg.slotSeqPtr(off))
	seq := raw &^ seqWritingMask
	writing := raw&seqWritingMask != 0
	switch {
	case seq == sub.cursor && writing:
		// The writer is committing this very event; treat as not yet
		// committed and let the caller retry.
		return ErrNoData
	case seq == sub.cursor:
		payload, _ := sub.seg.slotPayload(off)
		if err := fn(payload); err != nil {
			return err
		}
		if atomic.LoadUint64(sub.seg.slotSeqPtr(off)) != sub.cursor {
			// The writer overwrote the slot while fn was running:
			// the event is gone and the view may have been torn.
			sub.resyncLocked()
			return ErrLapped
		}
		sub.advanceLocked(sub.cursor + 1)
		return nil
	case seq > sub.cursor:
		// The slot already holds a newer event: this subscriber was
		// lapped and some events are lost.
		sub.resyncLocked()
		return ErrLapped
	default:
		// seq < cursor: this wrap of the slot is not committed yet.
		return ErrNoData
	}
}

// Position returns the subscriber's current persistable position.
func (sub *Subscriber) Position() Position {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	return Position{Generation: sub.gen, Seq: sub.cursor}
}

// ID returns the subscriber's registry ID, as reported by Stats.
func (sub *Subscriber) ID() uint64 { return sub.id }

// Close deregisters the subscriber. Its registry slot becomes reusable
// and, on PolicyBlock segments, stops holding back the writer. Closing a
// subscriber whose segment is already closed is a no-op.
func (sub *Subscriber) Close() error {
	if sub.closed.Swap(true) {
		return nil
	}
	if !sub.seg.isClosed() {
		sub.seg.releaseSubscriber(sub.idx)
	}
	return nil
}

// advanceLocked publishes progress both locally and to the registry.
func (sub *Subscriber) advanceLocked(seq uint64) {
	sub.cursor = seq
	sub.seg.setSubscriberCursor(sub.idx, seq)
}

// resyncLocked jumps the cursor to the oldest retained event after a gap.
func (sub *Subscriber) resyncLocked() {
	sub.advanceLocked(sub.seg.oldestSeq())
}
