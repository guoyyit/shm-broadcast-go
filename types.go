// Package shmbroadcast implements a single-machine, shared-memory,
// zero-copy broadcast channel: one writer process appends events to a
// memory-mapped segment that many reader processes can map and follow,
// each with an independent read cursor.
package shmbroadcast

import "errors"

// Policy decides what the writer does when the ring is full.
type Policy int

const (
	// PolicyOverwrite lets the writer overwrite the oldest events when the
	// ring is full. Lagging subscribers observe ErrLapped and resync.
	PolicyOverwrite Policy = iota
	// PolicyBlock makes Publish wait until every active subscriber has
	// consumed the oldest slot, applying backpressure instead of loss.
	PolicyBlock
)

// Options configures a segment at Create time.
type Options struct {
	// Capacity is the number of event slots in the ring.
	Capacity int
	// MaxEventSize is the maximum payload bytes per event.
	MaxEventSize int
	// Policy selects the backlog behaviour when the ring is full.
	Policy Policy
}

// Position is a persistable read cursor. Generation ties it to one
// incarnation of the segment file; Seq is the next event to read.
type Position struct {
	Generation uint64
	Seq        uint64
}

// SubscriberInfo reports one registered subscriber for diagnostics.
type SubscriberInfo struct {
	ID      uint64
	Cursor  uint64 // next sequence this subscriber will read
	Backlog uint64 // events not yet consumed by this subscriber
}

// Stats is a point-in-time diagnostic snapshot of a segment.
type Stats struct {
	Generation  uint64
	Capacity    uint64
	NextSeq     uint64 // committed write position (next sequence to be written)
	OldestSeq   uint64 // oldest sequence still retained in the ring
	Subscribers []SubscriberInfo
}

// Distinguishable read/Write outcomes.
var (
	// ErrLapped means the subscriber's cursor fell behind the oldest
	// retained event; some events were lost (overwrite policy) and the
	// cursor was advanced to the oldest available sequence.
	ErrLapped = errors.New("shmbroadcast: subscriber lapped, data gap")
	// ErrGenerationMismatch means the caller's position belongs to an
	// older incarnation of the segment and is invalid on the current one.
	ErrGenerationMismatch = errors.New("shmbroadcast: generation mismatch")
	// ErrNoData means the subscriber is caught up with the writer.
	ErrNoData = errors.New("shmbroadcast: no new events")
	// ErrWouldBlock means PolicyBlock would have to wait for space.
	ErrWouldBlock = errors.New("shmbroadcast: ring full, would block")
	// ErrEventTooLarge means the payload exceeds MaxEventSize.
	ErrEventTooLarge = errors.New("shmbroadcast: event exceeds max event size")
	// ErrWriterExists means another process already holds the writer role.
	ErrWriterExists = errors.New("shmbroadcast: another writer holds the segment lock")
	// ErrSubscriberLimit means the subscriber registry is full.
	ErrSubscriberLimit = errors.New("shmbroadcast: subscriber registry full")
	// ErrClosed is returned by operations on a closed handle.
	ErrClosed = errors.New("shmbroadcast: handle closed")
)
