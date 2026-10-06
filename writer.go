package shmbroadcast

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Writer is the single publishing handle on a segment. At most one Writer
// may exist per segment at a time, enforced by an OS file lock that the
// kernel releases automatically if the writer process dies, so a crashed
// writer never blocks a successor.
//
// A Writer is safe for use by multiple goroutines, but events are
// committed strictly in call order under an internal mutex.
type Writer struct {
	seg    *Segment
	next   uint64 // next sequence to write (1-based)
	mu     sync.Mutex
	closed atomic.Bool
}

// Writer acquires the exclusive writer role on the segment. It returns
// ErrWriterExists if another live process holds the role. When reopening
// a segment that already has committed events, the writer resumes at the
// stored write position; partially written slots from a crashed writer
// are invisible to readers and are cleanly overwritten.
func (s *Segment) Writer() (*Writer, error) {
	if s.isClosed() {
		return nil, ErrClosed
	}
	if err := syscall.Flock(int(s.f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrWriterExists
		}
		return nil, err
	}
	return &Writer{seg: s, next: s.loadNextSeq()}, nil
}

// Publish appends one event. With PolicyBlock it waits until the slowest
// active subscriber has freed a slot; with PolicyOverwrite it evicts the
// oldest event, and lagging subscribers later observe ErrLapped.
func (w *Writer) Publish(p []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed.Load() || w.seg.isClosed() {
		return ErrClosed
	}
	if uint64(len(p)) > w.seg.slotSize {
		return ErrEventTooLarge
	}
	if w.seg.policy == PolicyBlock {
		if err := w.waitSpace(); err != nil {
			return err
		}
	}
	w.commit(p)
	return nil
}

// TryPublish is Publish but returns ErrWouldBlock instead of waiting when
// a PolicyBlock segment is full.
func (w *Writer) TryPublish(p []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed.Load() || w.seg.isClosed() {
		return ErrClosed
	}
	if uint64(len(p)) > w.seg.slotSize {
		return ErrEventTooLarge
	}
	if w.seg.policy == PolicyBlock && !w.hasSpace() {
		return ErrWouldBlock
	}
	w.commit(p)
	return nil
}

// commit publishes one event with a three-step protocol:
//  1. store seq|seqWritingMask, invalidating the slot for readers;
//  2. write the payload and length;
//  3. store seq (release), committing the event.
//
// A reader that checks the seq word before and after reading can then
// never accept a partially written payload: any overlap with step 2 is
// bracketed by two different, non-equal seq values.
func (w *Writer) commit(p []byte) {
	s := w.seg
	off := s.slotOff(w.next)
	atomic.StoreUint64(s.slotSeqPtr(off), w.next|seqWritingMask)
	copy(s.data[off+slotHeaderSize:], p)
	s.setSlotLen(off, len(p))
	atomic.StoreUint64(s.slotSeqPtr(off), w.next)
	w.next++
	atomic.StoreUint64(u64ptr(s.data, offNextSeq), w.next)
}

// hasSpace reports whether the ring has a slot free for w.next. A slot is
// free once every active subscriber has consumed past it; with no active
// subscribers the writer never blocks.
func (w *Writer) hasSpace() bool {
	return w.next-w.seg.minActiveCursor(w.next) < w.seg.capacity
}

func (w *Writer) waitSpace() error {
	sleep := time.Millisecond
	for !w.hasSpace() {
		if w.closed.Load() || w.seg.isClosed() {
			return ErrClosed
		}
		time.Sleep(sleep)
		if sleep < 20*time.Millisecond {
			sleep *= 2
		}
	}
	return nil
}

// Close releases the writer role so another process may take it over.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed.Swap(true) {
		return nil
	}
	// Best effort: closing the segment's fd also releases the lock.
	syscall.Flock(int(w.seg.f.Fd()), syscall.LOCK_UN)
	return nil
}

// isClosed reports whether the segment handle has been closed.
func (s *Segment) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}
