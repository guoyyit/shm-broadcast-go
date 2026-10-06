package shmbroadcast

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// TestRemountPersistence: events committed before a clean shutdown must
// still be readable after every handle is closed and the segment is
// reopened, and the writer must resume without duplicating sequences.
func TestRemountPersistence(t *testing.T) {
	path := segPath(t)
	s1 := mustCreate(t, path, testOpts(PolicyOverwrite))
	gen1 := s1.Generation()
	w1 := mustWriter(t, s1)
	publishN(t, w1, 0, 5)
	w1.Close()
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s2.Close()
	if s2.Generation() != gen1 {
		t.Fatalf("plain reopen must keep the generation: %d vs %d", s2.Generation(), gen1)
	}

	sub := mustSubscribe(t, s2)
	for i := 0; i < 5; i++ {
		expectEvent(t, sub, fmt.Sprintf("event-%04d", i))
	}
	if _, err := sub.Next(); !errors.Is(err, ErrNoData) {
		t.Fatalf("want ErrNoData, got %v", err)
	}

	// The writer resumes at the stored position: no duplicated or
	// restarted sequence numbers.
	w2 := mustWriter(t, s2)
	publishN(t, w2, 5, 2)
	expectEvent(t, sub, "event-0005")
	expectEvent(t, sub, "event-0006")
	if st := s2.Stats(); st.NextSeq != 8 {
		t.Fatalf("NextSeq = %d, want 8", st.NextSeq)
	}
}

// TestUncommittedWriteInvisible simulates a writer that crashed in the
// middle of committing an event: the slot carries the writing marker and
// a garbage payload, but the shared write position never advanced.
// After remount, readers must not see the garbage, and a new writer must
// reuse the slot cleanly.
func TestUncommittedWriteInvisible(t *testing.T) {
	path := segPath(t)
	s1 := mustCreate(t, path, testOpts(PolicyOverwrite))
	w1 := mustWriter(t, s1)
	publishN(t, w1, 0, 2) // committed seqs 1, 2

	// Crash mid-commit of seq 3: marker set, garbage payload, len set,
	// but the commit store and the nextSeq advance never happened.
	off := s1.slotOff(3)
	copy(s1.data[off+slotHeaderSize:], []byte("GARBAGE-GARBAGE-GARBAGE-GARBAGE"))
	s1.setSlotLen(off, 32)
	atomic.StoreUint64(s1.slotSeqPtr(off), 3|seqWritingMask)
	w1.Close()
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s2.Close()
	sub := mustSubscribe(t, s2)
	expectEvent(t, sub, "event-0000")
	expectEvent(t, sub, "event-0001")
	// The half-written seq 3 must not be visible.
	if _, err := sub.Next(); !errors.Is(err, ErrNoData) {
		got, _ := sub.Next()
		t.Fatalf("uncommitted write leaked: err=%v then %q", err, got)
	}

	// A new writer reuses the slot for the real seq 3: no misalignment,
	// no duplication, no stale garbage.
	w2 := mustWriter(t, s2)
	publishN(t, w2, 2, 1)
	expectEvent(t, sub, "event-0002")
	if _, err := sub.Next(); !errors.Is(err, ErrNoData) {
		t.Fatalf("want ErrNoData, got %v", err)
	}
}

// TestRemountPreservesSubscriberRegistry: a subscriber whose process
// vanishes without closing leaves a registry entry behind; after remount
// the entry is still there (the registry is part of the segment), and a
// freshly created segment starts with an empty registry.
func TestRegistryLifecycleAcrossRecreate(t *testing.T) {
	path := segPath(t)
	s1 := mustCreate(t, path, testOpts(PolicyBlock))
	sub := mustSubscribe(t, s1)
	_ = sub // never closed: simulates a vanished subscriber process
	if st := s1.Stats(); len(st.Subscribers) != 1 {
		t.Fatalf("subscribers = %d, want 1", len(st.Subscribers))
	}
	s1.Close()

	s2 := mustCreate(t, path, testOpts(PolicyBlock))
	defer s2.Close()
	if st := s2.Stats(); len(st.Subscribers) != 0 {
		t.Fatalf("recreated segment must have an empty registry, got %+v", st.Subscribers)
	}
	// The stale entry must not hold back a writer on the new segment.
	w2 := mustWriter(t, s2)
	publishN(t, w2, 0, 20)
}
