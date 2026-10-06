package broadcast

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"
	"unsafe"
)

// 段文件布局（同机进程间共享，按本机字节序解释）：
//
//	[0,128)          固定头部
//	[128, hdrSize)   订阅者槽位数组，每槽 32 字节
//	[hdrSize, EOF)   数据区（环形字节缓冲）
//
// 头部字段偏移：
const (
	offMagic      = 0   // u64 魔数，最后写入，作为“初始化完成”标记
	offVersion    = 8   // u64 格式版本
	offGeneration = 16  // u64 代际标识（原子读）
	offHeaderSize = 24  // u64 数据区起始偏移
	offCapacity   = 32  // u64 数据区容量
	offPolicy     = 40  // u64 积压策略
	offMaxSlots   = 48  // u64 订阅者槽位数
	offTail       = 56  // u64 已提交写入位置（原子）
	offHead       = 64  // u64 最旧可用记录偏移（原子）
	offHeadSeq    = 72  // u64 head 处记录的事件序号（原子）
	offCommitted  = 80  // u64 已提交事件总数（原子）
	offCreatedAt  = 88  // u64 创建时间（纳秒）
	headerBase    = 128 // 固定头部长度
)

const (
	magicNumber   uint64 = 0x53484D2D42434E31 // "SHM-BCN1"
	formatVersion uint64 = 1
)

// 订阅者槽位布局（32 字节）：
const (
	slotID     = 0  // u64 订阅者 ID，0 表示空闲（原子）
	slotOffset = 8  // u64 当前读取偏移（原子）
	slotSeq    = 16 // u64 下一个待读序号（原子）
	slotPID    = 24 // u64 所属进程 PID
	slotSize   = 32
)

// segment 持有一个已映射的段文件。
type segment struct {
	f          *os.File
	data       []byte // 整个映射区
	generation uint64 // 映射时缓存的代际
	headerSize uint64
	capacity   uint64
	policy     Policy
	maxSlots   int
}

func (s *segment) u64ptr(off uint64) *uint64 {
	return (*uint64)(unsafe.Pointer(&s.data[off]))
}

func (s *segment) loadU64(off uint64) uint64 { return atomic.LoadUint64(s.u64ptr(off)) }

func (s *segment) storeU64(off uint64, v uint64) { atomic.StoreUint64(s.u64ptr(off), v) }

func (s *segment) loadTail() uint64      { return s.loadU64(offTail) }
func (s *segment) loadHead() uint64      { return s.loadU64(offHead) }
func (s *segment) loadHeadSeq() uint64   { return s.loadU64(offHeadSeq) }
func (s *segment) loadCommitted() uint64 { return s.loadU64(offCommitted) }

// checkGeneration 校验当前映射的段代际是否仍然有效。
func (s *segment) checkGeneration() error {
	if s.loadU64(offGeneration) != s.generation {
		return ErrGenerationMismatch
	}
	return nil
}

// close 解除映射并关闭文件（关闭文件描述符即释放 flock）。
func (s *segment) close() error {
	err1 := unmap(s.data)
	err2 := s.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// stats 读取诊断信息快照。
func (s *segment) stats() (Stats, error) {
	if err := s.checkGeneration(); err != nil {
		return Stats{}, err
	}
	tail := s.loadTail()
	committed := s.loadCommitted()
	st := Stats{
		Generation: s.generation,
		Capacity:   s.capacity,
		Policy:     s.policy,
		Head:       s.loadHead(),
		Tail:       tail,
		Committed:  committed,
		Backlog:    tail - s.loadHead(),
	}
	for i := 0; i < s.maxSlots; i++ {
		base := uint64(headerBase + i*slotSize)
		id := s.loadU64(base + slotID)
		if id == 0 {
			continue
		}
		off := s.loadU64(base + slotOffset)
		seq := s.loadU64(base + slotSeq)
		sub := SubscriberStats{ID: id, Offset: off, Seq: seq}
		if tail > off {
			sub.LagBytes = tail - off
		}
		if committed > seq {
			sub.LagEvents = committed - seq
		}
		st.Subscribers = append(st.Subscribers, sub)
	}
	return st, nil
}

// slotBase 返回第 i 个订阅者槽位的起始偏移。
func slotBase(i int) uint64 { return uint64(headerBase + i*slotSize) }

// randomGeneration 生成一个非零且不同于 old 的代际标识。
func randomGeneration(old uint64) uint64 {
	var buf [8]byte
	for {
		if _, err := rand.Read(buf[:]); err != nil {
			// 熵不可用时退化为时间戳，仍保证与 old 不同。
			g := uint64(time.Now().UnixNano())
			if g != old && g != 0 {
				return g
			}
			return g + 1
		}
		g := binary.LittleEndian.Uint64(buf[:])
		if g != 0 && g != old {
			return g
		}
	}
}

// createSegment 在 path 上创建或原地重建一个段。
// 重建不会 unlink 文件，而是在同一 inode 上重写头部并分配新代际，
// 使已映射旧段的进程能通过代际校验感知重建。
func createSegment(path string, opts Options) (*segment, error) {
	opts.normalize()
	page := uint64(os.Getpagesize())
	capacity := (opts.Capacity + page - 1) &^ (page - 1)
	if capacity > uint64(maxPayloadLen) {
		return nil, fmt.Errorf("broadcast: capacity %d exceeds maximum %d", capacity, uint64(maxPayloadLen))
	}
	headerSize := uint64(headerBase + opts.MaxSubscribers*slotSize)
	total := headerSize + capacity

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := tryLockExclusive(f); err != nil {
		f.Close()
		return nil, ErrWriterActive
	}
	if err := f.Truncate(int64(total)); err != nil {
		f.Close()
		return nil, err
	}
	data, err := mapFile(f, int(total))
	if err != nil {
		f.Close()
		return nil, err
	}

	s := &segment{
		f:          f,
		data:       data,
		headerSize: headerSize,
		capacity:   capacity,
		policy:     opts.Policy,
		maxSlots:   opts.MaxSubscribers,
	}

	// 读取旧代际（若曾是合法段），保证新代际一定不同。
	var oldGen uint64
	if s.loadU64(offMagic) == magicNumber {
		oldGen = s.loadU64(offGeneration)
	}
	gen := randomGeneration(oldGen)

	// 初始化顺序：先写代际让旧订阅者尽快失效，再清魔数阻止新挂载，
	// 最后写魔数作为初始化完成标记。
	s.storeU64(offGeneration, gen)
	s.storeU64(offMagic, 0)
	// 清空订阅者槽位，避免残留旧进度参与背压。
	for i := headerBase; i < headerBase+opts.MaxSubscribers*slotSize; i += 8 {
		s.storeU64(uint64(i), 0)
	}
	s.storeU64(offVersion, formatVersion)
	s.storeU64(offHeaderSize, headerSize)
	s.storeU64(offCapacity, capacity)
	s.storeU64(offPolicy, uint64(opts.Policy))
	s.storeU64(offMaxSlots, uint64(opts.MaxSubscribers))
	s.storeU64(offTail, 0)
	s.storeU64(offHead, 0)
	s.storeU64(offHeadSeq, 0)
	s.storeU64(offCommitted, 0)
	s.storeU64(offCreatedAt, uint64(time.Now().UnixNano()))
	s.storeU64(offMagic, magicNumber)

	s.generation = gen
	return s, nil
}

// openSegment 挂载 path 上已存在的段。lock 为 true 时获取写锁（写入方）。
func openSegment(path string, lock bool) (*segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrSegmentNotFound
		}
		return nil, err
	}
	if lock {
		if err := tryLockExclusive(f); err != nil {
			f.Close()
			return nil, ErrWriterActive
		}
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() < headerBase {
		f.Close()
		return nil, ErrInvalidSegment
	}
	data, err := mapFile(f, int(fi.Size()))
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &segment{f: f, data: data}
	if s.loadU64(offMagic) != magicNumber || s.loadU64(offVersion) != formatVersion {
		unmap(data)
		f.Close()
		return nil, ErrInvalidSegment
	}
	s.generation = s.loadU64(offGeneration)
	s.headerSize = s.loadU64(offHeaderSize)
	s.capacity = s.loadU64(offCapacity)
	s.policy = Policy(s.loadU64(offPolicy))
	s.maxSlots = int(s.loadU64(offMaxSlots))
	if s.headerSize < headerBase || s.headerSize+s.capacity != uint64(fi.Size()) {
		unmap(data)
		f.Close()
		return nil, ErrInvalidSegment
	}
	return s, nil
}
