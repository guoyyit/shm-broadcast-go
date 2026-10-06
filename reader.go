package broadcast

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Subscriber 是段的订阅方，持有相互独立的读取进度，
// 并在段的订阅者槽位中登记进度以供诊断与背压。
// 订阅者只写自己的槽位，不持有任何会阻塞写入方或其他订阅者的锁。
type Subscriber struct {
	mu  sync.Mutex
	seg *segment

	closed  bool
	slotIdx int
	id      uint64

	next    uint64 // 下一条待读记录偏移
	nextSeq uint64 // 下一条待读事件序号
	// 对外发布的读取水位：订阅者仍可能正在阅读的最旧事件。
	// 写入方在 PolicyBlock 下不会覆盖水位及之后的字节，
	// 因此最近一次 Next 返回的零拷贝数据在下一次读取前有效。
	pubOffset uint64
	pubSeq    uint64
}

// Subscribe 挂载 path 上的段并返回一个订阅者，
// 读取起点为段内当前保留的最旧事件。
func Subscribe(path string) (*Subscriber, error) {
	seg, err := openSegment(path, false)
	if err != nil {
		return nil, err
	}
	return newSubscriber(seg, seg.loadHead(), seg.loadHeadSeq())
}

// SubscribeAt 挂载 path 上的段并从指定位置恢复读取。
// 位置的代际与段不一致时返回 ErrGenerationMismatch；
// 位置已被覆盖时返回 *LaggedError；位置越界时返回 ErrPositionInvalid。
func SubscribeAt(path string, pos Position) (*Subscriber, error) {
	seg, err := openSegment(path, false)
	if err != nil {
		return nil, err
	}
	if pos.Generation != seg.generation {
		seg.close()
		return nil, ErrGenerationMismatch
	}
	head, tail := seg.loadHead(), seg.loadTail()
	if pos.Offset < head {
		seg.close()
		return nil, &LaggedError{Offset: pos.Offset, Head: head, Seq: pos.Seq, HeadSeq: seg.loadHeadSeq()}
	}
	if pos.Offset > tail {
		seg.close()
		return nil, ErrPositionInvalid
	}
	return newSubscriber(seg, pos.Offset, pos.Seq)
}

// newSubscriber 注册槽位并构造订阅者。
func newSubscriber(seg *segment, offset, seq uint64) (*Subscriber, error) {
	s := &Subscriber{seg: seg, next: offset, nextSeq: seq}
	if err := s.register(); err != nil {
		seg.close()
		return nil, err
	}
	return s, nil
}

// register 在段内登记一个订阅者槽位，并顺带回收崩溃进程残留的槽位。
func (s *Subscriber) register() error {
	var idBuf [8]byte
	if _, err := rand.Read(idBuf[:]); err != nil {
		return err
	}
	id := binary.LittleEndian.Uint64(idBuf[:])
	if id == 0 {
		id = 1
	}
	for i := 0; i < s.seg.maxSlots; i++ {
		base := slotBase(i)
		cur := s.seg.loadU64(base + slotID)
		if cur != 0 {
			// 回收所属进程已退出的残留槽位。
			pid := int(s.seg.loadU64(base + slotPID))
			if pid > 0 && !pidAlive(pid) {
				s.seg.casU64(base+slotID, cur, 0)
			}
			continue
		}
		// 先写进度与 PID，再发布 ID，保证写入方看到 ID 时进度已就绪。
		s.pubOffset, s.pubSeq = s.next, s.nextSeq
		s.seg.storeU64(base+slotOffset, s.pubOffset)
		s.seg.storeU64(base+slotSeq, s.pubSeq)
		s.seg.storeU64(base+slotPID, uint64(os.Getpid()))
		if s.seg.casU64(base+slotID, 0, id) {
			s.slotIdx = i
			s.id = id
			return nil
		}
	}
	return ErrTooManySubscribers
}

// publishProgress 把读取水位写入槽位。
// 若槽位已不属于自己（段被重建后槽位清零或被新订阅者复用），
// 则跳过，避免污染新代际的槽位。
func (s *Subscriber) publishProgress() {
	base := slotBase(s.slotIdx)
	if s.seg.loadU64(base+slotID) != s.id {
		return
	}
	s.seg.storeU64(base+slotOffset, s.pubOffset)
	s.seg.storeU64(base+slotSeq, s.pubSeq)
}

// Next 读取下一条事件。无新事件时返回 ErrEmpty；
// 进度被覆盖时返回 *LaggedError（调用 Resync 重新对齐）；
// 代际失效时返回 ErrGenerationMismatch。
//
// 返回的 Event.Data 是指向共享内存的零拷贝视图，返回前已经过
// 序号与 CRC 校验，保证不会观察到写入中间状态或撕裂的事件。
// PolicyBlock 下该视图在下一次 Next/Wait/Resync 调用前不会被覆盖；
// PolicyOverwrite 下写入方随时可能覆盖，需要保留请自行拷贝。
func (s *Subscriber) Next() (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Event{}, ErrClosed
	}
	for {
		ev, err := s.readOne()
		if err == errSkipPadding {
			continue
		}
		return ev, err
	}
}

var errSkipPadding = errorString("broadcast: skip padding")

type errorString string

func (e errorString) Error() string { return string(e) }

func (s *Subscriber) readOne() (Event, error) {
	if err := s.seg.checkGeneration(); err != nil {
		return Event{}, err
	}
	tail := s.seg.loadTail()
	if s.next == tail {
		return Event{}, ErrEmpty
	}
	if s.next > tail {
		// tail 只增不减，出现此情况说明段被重建但代际未变（不应发生）。
		return Event{}, ErrInvalidSegment
	}
	head := s.seg.loadHead()
	if s.next < head {
		return Event{}, s.laggedError(head)
	}

	base := s.seg.headerSize + s.next%s.seg.capacity
	cw := s.seg.loadU64(base)
	seqLow, length, padding := decodeCommitWord(cw)
	if padding {
		// 防御性校验：撕裂的填充记录按缺口处理。
		padLen := uint64(length)
		if padLen == 0 || padLen >= s.seg.capacity || s.next+padLen > tail {
			return Event{}, s.laggedError(head)
		}
		s.next += padLen
		return Event{}, errSkipPadding
	}
	if seqLow != uint32(s.nextSeq) {
		// 该位置已被更新的记录覆盖。
		return Event{}, s.laggedError(head)
	}
	recLen := recordLen(uint64(length))
	// 防御性校验：撕裂的提交字按缺口处理，避免越界切片。
	if recLen > s.seg.capacity || s.next+recLen > tail {
		return Event{}, s.laggedError(head)
	}
	crc := uint32(s.seg.loadU64(base + 8))
	raw := s.seg.data[base+recordHeaderSize : base+recordHeaderSize+align8(uint64(length))]
	buf := make([]byte, length)
	atomicLoadBytes(buf, raw)
	if crc32.Checksum(buf, crcTable) != crc {
		return Event{}, s.laggedError(head)
	}
	// 复检：读取期间写入方可能已覆盖该区域。
	if s.seg.loadU64(base) != cw || s.seg.loadHead() > s.next {
		return Event{}, s.laggedError(head)
	}

	evStart := s.next
	s.next += recLen
	seq := s.nextSeq
	s.nextSeq++
	// 水位推进到刚返回的事件起点，保护其零拷贝数据
	// 在下一次读取前不被 PolicyBlock 背压覆盖。
	s.pubOffset, s.pubSeq = evStart, seq
	s.publishProgress()
	// 零拷贝视图：校验通过，直接引用共享内存。
	return Event{Seq: seq, Data: raw[:length]}, nil
}

func (s *Subscriber) laggedError(head uint64) error {
	return &LaggedError{
		Offset:  s.next,
		Head:    head,
		Seq:     s.nextSeq,
		HeadSeq: s.seg.loadHeadSeq(),
	}
}

// Wait 阻塞等待下一条事件，直到有事件可读或 ctx 取消。
// 返回的错误语义与 Next 相同，另可能返回 ctx 的错误。
func (s *Subscriber) Wait(ctx context.Context) (Event, error) {
	backoff := 50 * time.Microsecond
	for {
		ev, err := s.Next()
		if err != ErrEmpty {
			return ev, err
		}
		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 2*time.Millisecond {
			backoff *= 2
		}
	}
}

// Resync 在收到 LaggedError 后将读取进度对齐到段内当前最旧可用事件，
// 调用方借此显式确认数据缺口，随后可继续正常读取。
func (s *Subscriber) Resync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if err := s.seg.checkGeneration(); err != nil {
		return err
	}
	s.next = s.seg.loadHead()
	s.nextSeq = s.seg.loadHeadSeq()
	s.pubOffset, s.pubSeq = s.next, s.nextSeq
	s.publishProgress()
	return nil
}

// ResyncToLatest 丢弃所有积压，把读取进度对齐到当前写入位置，
// 之后只能读到新写入的事件。
func (s *Subscriber) ResyncToLatest() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if err := s.seg.checkGeneration(); err != nil {
		return err
	}
	s.next = s.seg.loadTail()
	s.nextSeq = s.seg.loadCommitted()
	s.pubOffset, s.pubSeq = s.next, s.nextSeq
	s.publishProgress()
	return nil
}

// Position 返回当前读取进度，可用于 SubscribeAt 断点恢复。
// 若段已被重建，返回的位置会因代际不匹配而被新段拒绝。
func (s *Subscriber) Position() Position {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Position{Generation: s.seg.generation, Offset: s.next, Seq: s.nextSeq}
}

// Stats 返回段的诊断信息快照。
func (s *Subscriber) Stats() (Stats, error) {
	return s.seg.stats()
}

// Close 注销订阅者槽位并解除映射。
func (s *Subscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	// 仅当槽位仍属于自己时才注销：段可能已被重建，
	// 槽位或许已分配给新代际的订阅者。
	s.seg.casU64(slotBase(s.slotIdx)+slotID, s.id, 0)
	return s.seg.close()
}

// minSubscriberOffset 返回所有活跃订阅者的最小读取偏移，
// 并顺带回收所属进程已退出的残留槽位。无订阅者时 any 为 false。
func (s *segment) minSubscriberOffset() (min uint64, any bool) {
	min = ^uint64(0)
	for i := 0; i < s.maxSlots; i++ {
		base := slotBase(i)
		id := s.loadU64(base + slotID)
		if id == 0 {
			continue
		}
		pid := int(s.loadU64(base + slotPID))
		if pid > 0 && !pidAlive(pid) {
			s.casU64(base+slotID, id, 0)
			continue
		}
		if off := s.loadU64(base + slotOffset); off < min {
			min = off
			any = true
		}
	}
	return min, any
}

// casU64 是对映射区字长的 CAS 封装。
func (s *segment) casU64(off uint64, old, new uint64) bool {
	return atomic.CompareAndSwapUint64(s.u64ptr(off), old, new)
}
