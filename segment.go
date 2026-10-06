package shmbroadcast

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// Segment file layout (all integers native-endian, 8-byte aligned):
//
//	[0,128)     fixed header fields (see offsets below)
//	[128,4096)  subscriber registry: 124 entries x 32 bytes
//	[4096,...)  event slots: capacity x slotStride bytes
//
// Each slot: [seq uint64][len uint32][reserved uint32][payload...].
// A slot is committed iff its seq equals the sequence number of the event
// it holds; seq is stored with release semantics after the payload, so
// readers never observe a partially written event.
const (
	headerMagic   uint64 = 0x53484D2D42435354 // "SHM-BCST"
	headerVersion uint32 = 1
	headerSize           = 4096

	slotHeaderSize = 16

	// seqWritingMask marks a slot whose payload is being written right
	// now. The writer stores seq|mask before touching the payload and
	// seq alone to commit, so a reader that observes the mask (or any
	// seq change) between its two checks knows the read was unsafe.
	// Valid sequence numbers stay below 1<<63.
	seqWritingMask uint64 = 1 << 63

	offMagic      = 0
	offVersion    = 8
	offPolicy     = 12
	offGeneration = 16
	offCapacity   = 24
	offSlotSize   = 32
	offSlotStride = 40
	offNextSeq    = 64 // atomic: committed write position
	offSubIDSeq   = 72 // atomic: subscriber ID counter
	offSubTable   = 128

	subEntSize      = 32
	subEntID        = 0
	subEntCursor    = 8
	subEntState     = 16
	maxSubscribers  = (headerSize - offSubTable) / subEntSize
	subStateFree    = 0
	subStateClaimed = 1
	subStateActive  = 2
)

// ErrNotSegment means the file is missing, truncated, or not a valid segment.
var ErrNotSegment = errors.New("shmbroadcast: not a valid segment file")

// Segment is a memory-mapped broadcast segment file.
type Segment struct {
	path string
	f    *os.File
	data []byte // mapped bytes: [header][slots...]

	capacity   uint64
	slotSize   uint64
	slotStride uint64
	policy     Policy

	mu     sync.Mutex // guards closed
	closed bool
}

func align8(n int) int { return (n + 7) &^ 7 }

// Create creates or recreates the segment at path. Recreating reuses the
// same file (so existing mappings observe the change), zeroes all state,
// and stamps a fresh generation, invalidating cursors from the previous
// incarnation. Create waits for any current writer to close first.
func Create(path string, opts Options) (*Segment, error) {
	if opts.Capacity <= 0 {
		return nil, fmt.Errorf("shmbroadcast: capacity must be > 0")
	}
	if opts.MaxEventSize <= 0 {
		return nil, fmt.Errorf("shmbroadcast: max event size must be > 0")
	}
	if opts.Policy != PolicyOverwrite && opts.Policy != PolicyBlock {
		return nil, fmt.Errorf("shmbroadcast: unknown policy %d", opts.Policy)
	}
	stride := align8(slotHeaderSize + opts.MaxEventSize)
	total := int64(headerSize + opts.Capacity*stride)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// Serialize with any active writer and other creators. The lock is
	// held only during (re)initialization.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return nil, err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

	// Derive the new generation before truncating: bump the previous one
	// if a valid header is still readable, so generations stay monotonic
	// across in-place recreation.
	var oldGen uint64
	if st, err := f.Stat(); err == nil && st.Size() >= headerSize {
		hdr := make([]byte, slotHeaderSize)
		if _, err := f.ReadAt(hdr, 0); err == nil {
			magic := *(*uint64)(unsafe.Pointer(&hdr[0]))
			if magic == headerMagic {
				genBuf := make([]byte, 8)
				if _, err := f.ReadAt(genBuf, offGeneration); err == nil {
					oldGen = *(*uint64)(unsafe.Pointer(&genBuf[0]))
				}
			}
		}
	}
	gen := oldGen + 1
	if oldGen == 0 {
		// No readable previous header (fresh file, or the file was
		// replaced externally): seed from the clock so a stale position
		// from an earlier deleted incarnation still mismatches.
		gen = uint64(time.Now().UnixNano())
	}

	if err := f.Truncate(total); err != nil {
		return nil, err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(total),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	// Invalidate first, then wipe all residual state (header, registry,
	// slot sequence numbers) so no stale content from a previous
	// incarnation can be mistaken for committed events.
	atomic.StoreUint64(u64ptr(data, offMagic), 0)
	clear(data)
	*(*uint32)(unsafe.Pointer(&data[offVersion])) = headerVersion
	*(*uint32)(unsafe.Pointer(&data[offPolicy])) = uint32(opts.Policy)
	*u64ptr(data, offCapacity) = uint64(opts.Capacity)
	*u64ptr(data, offSlotSize) = uint64(opts.MaxEventSize)
	*u64ptr(data, offSlotStride) = uint64(stride)
	// Sequence numbers are 1-based: a zeroed slot seq word then means
	// "never committed" and can never alias a real event.
	atomic.StoreUint64(u64ptr(data, offNextSeq), 1)
	atomic.StoreUint64(u64ptr(data, offGeneration), gen)
	// Publish: magic is the last field to become visible.
	atomic.StoreUint64(u64ptr(data, offMagic), headerMagic)
	if err := syscall.Munmap(data); err != nil {
		return nil, err
	}
	return Open(path)
}

// Open maps an existing segment without modifying it.
func Open(path string) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() < headerSize {
		f.Close()
		return nil, ErrNotSegment
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(st.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	// A concurrent Create may have the segment briefly invalidated;
	// wait for the magic to be published.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadUint64(u64ptr(data, offMagic)) != headerMagic {
		if time.Now().After(deadline) {
			syscall.Munmap(data)
			f.Close()
			return nil, ErrNotSegment
		}
		time.Sleep(2 * time.Millisecond)
	}
	s := &Segment{path: path, f: f, data: data}
	if v := *(*uint32)(unsafe.Pointer(&data[offVersion])); v != headerVersion {
		s.Close()
		return nil, fmt.Errorf("shmbroadcast: unsupported segment version %d", v)
	}
	s.capacity = atomic.LoadUint64(u64ptr(data, offCapacity))
	s.slotSize = *(*uint64)(unsafe.Pointer(&data[offSlotSize]))
	s.slotStride = *(*uint64)(unsafe.Pointer(&data[offSlotStride]))
	s.policy = Policy(*(*uint32)(unsafe.Pointer(&data[offPolicy])))
	want := int64(headerSize) + int64(s.capacity*s.slotStride)
	if s.capacity == 0 || s.slotStride == 0 || st.Size() != want {
		s.Close()
		return nil, ErrNotSegment
	}
	return s, nil
}

// Close unmaps the segment and releases the file. It is idempotent.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	err1 := syscall.Munmap(s.data)
	err2 := s.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// Stats returns a diagnostic snapshot: write position and per-subscriber
// progress/backlog.
func (s *Segment) Stats() Stats {
	next := s.loadNextSeq()
	st := Stats{
		Generation: s.Generation(),
		Capacity:   s.capacity,
		NextSeq:    next,
		OldestSeq:  s.oldestSeq(),
	}
	for i := 0; i < maxSubscribers; i++ {
		base := offSubTable + i*subEntSize
		if atomic.LoadUint64(u64ptr(s.data, base+subEntState)) != subStateActive {
			continue
		}
		id := atomic.LoadUint64(u64ptr(s.data, base+subEntID))
		cur := atomic.LoadUint64(u64ptr(s.data, base+subEntCursor))
		var backlog uint64
		if next > cur {
			backlog = next - cur
		}
		st.Subscribers = append(st.Subscribers, SubscriberInfo{ID: id, Cursor: cur, Backlog: backlog})
	}
	return st
}

// Generation returns the current generation of the segment, read live
// from the shared header so a recreated segment is detected.
func (s *Segment) Generation() uint64 {
	return atomic.LoadUint64(u64ptr(s.data, offGeneration))
}

// --- shared-header primitives (same package: writer.go, subscriber.go) ---

func u64ptr(b []byte, off int) *uint64 {
	return (*uint64)(unsafe.Pointer(&b[off]))
}

func (s *Segment) loadNextSeq() uint64 {
	return atomic.LoadUint64(u64ptr(s.data, offNextSeq))
}

// oldestSeq is the oldest sequence still retained in the ring.
// Sequences are 1-based, so the floor is 1 even when nothing was written.
func (s *Segment) oldestSeq() uint64 {
	next := s.loadNextSeq()
	if next > s.capacity {
		return next - s.capacity
	}
	return 1
}

// slotOff returns the byte offset of the slot holding sequence seq.
func (s *Segment) slotOff(seq uint64) int {
	return headerSize + int(seq%s.capacity)*int(s.slotStride)
}

// slotSeqPtr returns the pointer to the slot's commit sequence word.
func (s *Segment) slotSeqPtr(slotOff int) *uint64 {
	return u64ptr(s.data, slotOff)
}

// slotPayload returns the slot's payload region and its length field.
// Callers must have validated the slot's seq first.
func (s *Segment) slotPayload(slotOff int) (payload []byte, length uint32) {
	length = *(*uint32)(unsafe.Pointer(&s.data[slotOff+8]))
	start := slotOff + slotHeaderSize
	return s.data[start : start+int(length)], length
}

// setSlotPayload writes the length field of a slot; the payload itself is
// written by the caller before the seq word is published.
func (s *Segment) setSlotLen(slotOff int, n int) {
	*(*uint32)(unsafe.Pointer(&s.data[slotOff+8])) = uint32(n)
}

// claimSubscriber registers a subscriber with the given start cursor and
// returns its ID and table index.
func (s *Segment) claimSubscriber(startSeq uint64) (id uint64, idx int, err error) {
	for i := 0; i < maxSubscribers; i++ {
		base := offSubTable + i*subEntSize
		state := u64ptr(s.data, base+subEntState)
		if !atomic.CompareAndSwapUint64(state, subStateFree, subStateClaimed) {
			continue
		}
		id = atomic.AddUint64(u64ptr(s.data, offSubIDSeq), 1)
		atomic.StoreUint64(u64ptr(s.data, base+subEntID), id)
		atomic.StoreUint64(u64ptr(s.data, base+subEntCursor), startSeq)
		atomic.StoreUint64(state, subStateActive)
		return id, i, nil
	}
	return 0, 0, ErrSubscriberLimit
}

// releaseSubscriber frees the registry entry at idx.
func (s *Segment) releaseSubscriber(idx int) {
	base := offSubTable + idx*subEntSize
	atomic.StoreUint64(u64ptr(s.data, base+subEntState), subStateFree)
}

// setSubscriberCursor publishes the subscriber's read progress.
func (s *Segment) setSubscriberCursor(idx int, seq uint64) {
	base := offSubTable + idx*subEntSize
	atomic.StoreUint64(u64ptr(s.data, base+subEntCursor), seq)
}

// minActiveCursor returns the smallest cursor among active subscribers,
// or fallback when none are registered.
func (s *Segment) minActiveCursor(fallback uint64) uint64 {
	min := fallback
	for i := 0; i < maxSubscribers; i++ {
		base := offSubTable + i*subEntSize
		if atomic.LoadUint64(u64ptr(s.data, base+subEntState)) != subStateActive {
			continue
		}
		cur := atomic.LoadUint64(u64ptr(s.data, base+subEntCursor))
		if cur < min {
			min = cur
		}
	}
	return min
}
