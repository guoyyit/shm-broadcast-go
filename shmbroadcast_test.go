package shmbroadcast

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func segPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.seg")
}

func testOpts(policy Policy) Options {
	return Options{Capacity: 8, MaxEventSize: 64, Policy: policy}
}

func mustCreate(t *testing.T, path string, opts Options) *Segment {
	t.Helper()
	s, err := Create(path, opts)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustWriter(t *testing.T, s *Segment) *Writer {
	t.Helper()
	w, err := s.Writer()
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func mustSubscribe(t *testing.T, s *Segment) *Subscriber {
	t.Helper()
	sub, err := s.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { sub.Close() })
	return sub
}

func publishN(t *testing.T, w *Writer, start, n int) {
	t.Helper()
	for i := start; i < start+n; i++ {
		if err := w.Publish([]byte(fmt.Sprintf("event-%04d", i))); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
}

func expectEvent(t *testing.T, sub *Subscriber, want string) {
	t.Helper()
	got, err := sub.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if string(got) != want {
		t.Fatalf("event = %q, want %q", got, want)
	}
}

func TestCreateOpenStats(t *testing.T) {
	path := segPath(t)
	s := mustCreate(t, path, testOpts(PolicyOverwrite))

	st := s.Stats()
	if st.Generation == 0 {
		t.Fatal("generation must be non-zero")
	}
	if st.Capacity != 8 || st.NextSeq != 1 || st.OldestSeq != 1 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	if len(st.Subscribers) != 0 {
		t.Fatalf("no subscribers expected, got %+v", st.Subscribers)
	}

	// A second handle on the same path sees the same generation.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s2.Close()
	if s2.Generation() != st.Generation {
		t.Fatalf("generation differs across handles: %d vs %d", s2.Generation(), st.Generation)
	}
}

func TestOpenInvalidFile(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "missing.seg")); err == nil {
		t.Fatal("Open of missing file must fail")
	}
}

func TestIndependentSubscriberProgress(t *testing.T) {
	s := mustCreate(t, segPath(t), testOpts(PolicyOverwrite))
	w := mustWriter(t, s)
	subA := mustSubscribe(t, s)
	subB := mustSubscribe(t, s)

	publishN(t, w, 0, 5)

	// A reads everything; B has not started. A's progress must not
	// affect B's stream.
	for i := 0; i < 5; i++ {
		expectEvent(t, subA, fmt.Sprintf("event-%04d", i))
	}
	if _, err := subA.Next(); !errors.Is(err, ErrNoData) {
		t.Fatalf("A: want ErrNoData, got %v", err)
	}
	for i := 0; i < 5; i++ {
		expectEvent(t, subB, fmt.Sprintf("event-%04d", i))
	}

	// Diagnostics reflect both cursors and the write position.
	st := s.Stats()
	if st.NextSeq != 6 || len(st.Subscribers) != 2 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	for _, info := range st.Subscribers {
		if info.Cursor != 6 || info.Backlog != 0 {
			t.Fatalf("subscriber %d cursor=%d backlog=%d, want 6/0",
				info.ID, info.Cursor, info.Backlog)
		}
	}

	// One more event: B reads it, A lags by one and shows backlog.
	publishN(t, w, 5, 1)
	expectEvent(t, subB, "event-0005")
	st = s.Stats()
	var lagging *SubscriberInfo
	for i := range st.Subscribers {
		if st.Subscribers[i].Cursor == 6 {
			lagging = &st.Subscribers[i]
		}
	}
	if lagging == nil || lagging.Backlog != 1 {
		t.Fatalf("expected one lagging subscriber with backlog 1: %+v", st.Subscribers)
	}
}

func TestOverwritePolicyLapsSlowSubscriber(t *testing.T) {
	s := mustCreate(t, segPath(t), testOpts(PolicyOverwrite)) // capacity 8
	w := mustWriter(t, s)
	sub := mustSubscribe(t, s)

	// Writer is never blocked by the idle subscriber.
	publishN(t, w, 0, 20)

	// The subscriber must detect the gap, not read stale data.
	if _, err := sub.Next(); !errors.Is(err, ErrLapped) {
		t.Fatalf("want ErrLapped, got %v", err)
	}
	// After resync the oldest retained events are readable: seqs 13..20
	// (capacity 8, next = 21).
	for i := 12; i < 20; i++ {
		expectEvent(t, sub, fmt.Sprintf("event-%04d", i))
	}
	if _, err := sub.Next(); !errors.Is(err, ErrNoData) {
		t.Fatalf("want ErrNoData, got %v", err)
	}
}

func TestBlockPolicyBackpressure(t *testing.T) {
	s := mustCreate(t, segPath(t), testOpts(PolicyBlock)) // capacity 8
	w := mustWriter(t, s)
	sub := mustSubscribe(t, s)

	// Fill the ring.
	publishN(t, w, 0, 8)

	// Non-blocking write must report full.
	if err := w.TryPublish([]byte("x")); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("want ErrWouldBlock, got %v", err)
	}

	// Blocking write waits until the subscriber frees a slot.
	done := make(chan error, 1)
	go func() { done <- w.Publish([]byte("event-0008")) }()
	select {
	case <-done:
		t.Fatal("Publish returned while the ring was full")
	case <-time.After(100 * time.Millisecond):
	}
	expectEvent(t, sub, "event-0000") // frees one slot
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not unblock after a slot was freed")
	}

	// Closing the lagging subscriber releases its backpressure entirely.
	sub.Close()
	publishN(t, w, 9, 8) // must not block anymore
}

func TestBlockPolicyNoSubscribersNeverBlocks(t *testing.T) {
	s := mustCreate(t, segPath(t), testOpts(PolicyBlock))
	w := mustWriter(t, s)
	publishN(t, w, 0, 100) // far beyond capacity, no subscribers registered
}

func TestWriterExclusion(t *testing.T) {
	path := segPath(t)
	s1 := mustCreate(t, path, testOpts(PolicyOverwrite))
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s2.Close()

	w1 := mustWriter(t, s1)
	if _, err := s2.Writer(); !errors.Is(err, ErrWriterExists) {
		t.Fatalf("want ErrWriterExists, got %v", err)
	}
	w1.Close()
	w2, err := s2.Writer()
	if err != nil {
		t.Fatalf("Writer after close: %v", err)
	}
	w2.Close()
}

func TestEventTooLarge(t *testing.T) {
	s := mustCreate(t, segPath(t), testOpts(PolicyOverwrite))
	w := mustWriter(t, s)
	if err := w.Publish(make([]byte, 65)); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("want ErrEventTooLarge, got %v", err)
	}
	if err := w.Publish(make([]byte, 64)); err != nil {
		t.Fatalf("max-size event must fit: %v", err)
	}
}

func TestSubscribeAtResume(t *testing.T) {
	s := mustCreate(t, segPath(t), testOpts(PolicyOverwrite))
	w := mustWriter(t, s)
	publishN(t, w, 0, 5)

	sub := mustSubscribe(t, s)
	expectEvent(t, sub, "event-0000")
	expectEvent(t, sub, "event-0001")
	pos := sub.Position()
	sub.Close()

	sub2, err := s.SubscribeAt(pos)
	if err != nil {
		t.Fatalf("SubscribeAt: %v", err)
	}
	defer sub2.Close()
	for i := 2; i < 5; i++ {
		expectEvent(t, sub2, fmt.Sprintf("event-%04d", i))
	}
}

func TestSubscribeAtLappedPositionReportsGap(t *testing.T) {
	s := mustCreate(t, segPath(t), testOpts(PolicyOverwrite))
	w := mustWriter(t, s)
	sub := mustSubscribe(t, s)
	pos := sub.Position() // cursor at seq 1
	sub.Close()

	publishN(t, w, 0, 20) // seq 1 is long overwritten

	sub2, err := s.SubscribeAt(pos)
	if err != nil {
		t.Fatalf("SubscribeAt: %v", err)
	}
	defer sub2.Close()
	if _, err := sub2.Next(); !errors.Is(err, ErrLapped) {
		t.Fatalf("want ErrLapped, got %v", err)
	}
}

func TestGenerationMismatchOnRecreate(t *testing.T) {
	path := segPath(t)
	s1 := mustCreate(t, path, testOpts(PolicyOverwrite))
	w1 := mustWriter(t, s1)
	publishN(t, w1, 0, 3)
	sub := mustSubscribe(t, s1)
	stalePos := sub.Position()
	gen1 := s1.Generation()
	w1.Close()
	s1.Close()

	// Recreate the same path: new generation, state reset.
	s2 := mustCreate(t, path, testOpts(PolicyOverwrite))
	if s2.Generation() == gen1 {
		t.Fatal("recreate must bump the generation")
	}
	if st := s2.Stats(); st.NextSeq != 1 {
		t.Fatalf("recreated segment must be empty, got NextSeq=%d", st.NextSeq)
	}

	// A stale position is rejected with a distinguishable error.
	if _, err := s2.SubscribeAt(stalePos); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("want ErrGenerationMismatch, got %v", err)
	}
}

func TestStaleHandleDetectsRecreate(t *testing.T) {
	path := segPath(t)
	s1 := mustCreate(t, path, testOpts(PolicyOverwrite))
	sub := mustSubscribe(t, s1)

	// Recreate in place while s1 is still mapped.
	s2 := mustCreate(t, path, testOpts(PolicyOverwrite))
	defer s2.Close()

	if _, err := sub.Next(); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("want ErrGenerationMismatch, got %v", err)
	}
}

func TestZeroCopyView(t *testing.T) {
	s := mustCreate(t, segPath(t), testOpts(PolicyOverwrite))
	w := mustWriter(t, s)
	sub := mustSubscribe(t, s)

	if err := w.Publish([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	var n int
	err := sub.NextView(func(p []byte) error {
		n = len(p)
		// The view aliases the segment directly.
		if string(p) != "hello" {
			t.Errorf("view = %q", p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("NextView: %v", err)
	}
	if n != 5 {
		t.Fatalf("view length = %d", n)
	}

	// fn errors propagate and the cursor does not advance.
	if err := w.Publish([]byte("world")); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	if err := sub.NextView(func([]byte) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	expectEvent(t, sub, "world")
}
