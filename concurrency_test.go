package shmbroadcast

import (
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"
)

// makePayload builds a 32-byte event: an 8-byte counter plus the counter
// value repeated over the rest, so a torn read is detectable.
func makePayload(i uint64) []byte {
	p := make([]byte, 32)
	binary.LittleEndian.PutUint64(p, i)
	for k := 8; k < len(p); k++ {
		p[k] = byte(i)
	}
	return p
}

// checkPayload verifies the counter and the tear-detection pattern.
func checkPayload(t *testing.T, p []byte, want uint64) {
	t.Helper()
	if len(p) != 32 {
		t.Errorf("payload length = %d, want 32", len(p))
		return
	}
	got := binary.LittleEndian.Uint64(p)
	if got != want {
		t.Errorf("counter = %d, want %d", got, want)
		return
	}
	for k := 8; k < len(p); k++ {
		if p[k] != byte(want) {
			t.Errorf("torn payload at byte %d for event %d", k, want)
			return
		}
	}
}

// TestConcurrentBlockMode hammers a PolicyBlock segment with one writer
// and several subscribers. Block mode guarantees no loss: every
// subscriber must receive every event, in order, untorn. Run with -race.
func TestConcurrentBlockMode(t *testing.T) {
	s := mustCreate(t, segPath(t), Options{Capacity: 32, MaxEventSize: 64, Policy: PolicyBlock})
	w := mustWriter(t, s)

	const n = 5000
	const subs = 4
	var wg sync.WaitGroup
	for k := 0; k < subs; k++ {
		sub, err := s.Subscribe()
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		wg.Add(1)
		go func(sub *Subscriber) {
			defer wg.Done()
			defer sub.Close()
			for i := uint64(0); i < n; {
				p, err := sub.Next()
				if errors.Is(err, ErrNoData) {
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					t.Errorf("Next: %v", err)
					return
				}
				checkPayload(t, p, i)
				i++
			}
		}(sub)
	}
	for i := uint64(0); i < n; i++ {
		if err := w.Publish(makePayload(i)); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	wg.Wait()
}

// TestConcurrentOverwriteSameProcess exercises the overwrite policy with
// a writer and a subscriber taking turns without overlapping in time:
// the subscriber may lag and be lapped, but every event it does receive
// must be complete and in order.
func TestConcurrentOverwriteSequentialInterleave(t *testing.T) {
	s := mustCreate(t, segPath(t), Options{Capacity: 16, MaxEventSize: 64, Policy: PolicyOverwrite})
	w := mustWriter(t, s)
	sub := mustSubscribe(t, s)

	var want uint64 // next counter the subscriber may see (>=, gaps allowed)
	for round := 0; round < 50; round++ {
		for i := uint64(0); i < 7; i++ {
			if err := w.Publish(makePayload(want + i)); err != nil {
				t.Fatal(err)
			}
		}
		for {
			p, err := sub.Next()
			if errors.Is(err, ErrNoData) {
				break
			}
			if errors.Is(err, ErrLapped) {
				continue // gap reported, cursor resynced
			}
			if err != nil {
				t.Fatal(err)
			}
			got := binary.LittleEndian.Uint64(p)
			if got < want {
				t.Fatalf("event regressed: got %d, want >= %d", got, want)
			}
			checkPayload(t, p, got)
			want = got + 1
		}
		want += 7
	}
}
