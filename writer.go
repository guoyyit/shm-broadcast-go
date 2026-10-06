package broadcast

import (
	"hash/crc32"
	"sync"
	"time"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Writer 是段的写入方。同一段同一时刻只允许一个 Writer 存活
// （由文件排他锁保证，进程崩溃后锁由内核自动释放）。
type Writer struct {
	mu  sync.Mutex
	seg *segment

	closed bool
	// 写入方内存态，与头部原子字段保持一致：
	writeOffset uint64 // == tail
	head        uint64 // == head
	headSeq     uint64 // == headSeq
	nextSeq     uint64 // == committed
}

// Create 在 path 上创建（或原地重建）一个段，并返回其写入方。
// 重建会分配新的代际标识，持有旧代际进度的订阅者将收到
// ErrGenerationMismatch，而不会把旧偏移当作新段进度。
func Create(path string, opts Options) (*Writer, error) {
	seg, err := createSegment(path, opts)
	if err != nil {
		return nil, err
	}
	return &Writer{seg: seg}, nil
}

// OpenWriter 重新挂载 path 上已存在的段并恢复写入方状态。
// 已提交事件保持可读；崩溃时未提交的残留写入不可见也不会错位，
// 新写入会从最后提交位置继续并覆盖残留字节。
func OpenWriter(path string) (*Writer, error) {
	seg, err := openSegment(path, true)
	if err != nil {
		return nil, err
	}
	return &Writer{
		seg:         seg,
		writeOffset: seg.loadTail(),
		head:        seg.loadHead(),
		headSeq:     seg.loadHeadSeq(),
		nextSeq:     seg.loadCommitted(),
	}, nil
}

// Write 追加一条事件。PolicyBlock 下段满时阻塞等待最慢的已注册
// 订阅者腾出空间；PolicyOverwrite 下覆盖最旧事件。返回事件序号。
func (w *Writer) Write(p []byte) (uint64, error) {
	return w.write(p, true)
}

// TryWrite 与 Write 相同，但 PolicyBlock 下段满时返回 ErrWouldBlock
// 而不是阻塞。
func (w *Writer) TryWrite(p []byte) (uint64, error) {
	return w.write(p, false)
}

func (w *Writer) write(p []byte, block bool) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	if uint64(len(p)) > uint64(maxPayloadLen) {
		return 0, ErrEventTooLarge
	}
	recLen := recordLen(uint64(len(p)))
	if recLen > w.seg.capacity {
		return 0, ErrEventTooLarge
	}

	// 第一步：若记录无法放入数据区尾部剩余空间，先提交一段填充记录，
	// 使下一条记录从数据区起点开始。填充作为独立一步提交，
	// 保证每次空间推进量都不超过容量。
	if pos := w.writeOffset % w.seg.capacity; pos+recLen > w.seg.capacity {
		padLen := w.seg.capacity - pos
		if err := w.ensureSpace(padLen, block); err != nil {
			return 0, err
		}
		w.seg.storeU64(w.seg.headerSize+pos, encodeCommitWord(0, uint32(padLen), true))
		w.writeOffset += padLen
		w.seg.storeU64(offTail, w.writeOffset)
	}

	// 第二步：写入正式记录并提交。
	if err := w.ensureSpace(recLen, block); err != nil {
		return 0, err
	}
	base := w.seg.headerSize + w.writeOffset%w.seg.capacity
	payload := w.seg.data[base+recordHeaderSize : base+recordHeaderSize+align8(uint64(len(p)))]
	atomicStoreBytes(payload, p)
	crc := crc32.Checksum(p, crcTable)
	w.seg.storeU64(base+8, uint64(crc))
	w.seg.storeU64(base, encodeCommitWord(w.nextSeq, uint32(len(p)), false))
	// 发布：先更新已提交计数，再推进 tail（读者以 tail 为可见性门槛）。
	w.seg.storeU64(offCommitted, w.nextSeq+1)
	w.writeOffset += recLen
	w.seg.storeU64(offTail, w.writeOffset)
	seq := w.nextSeq
	w.nextSeq++
	return seq, nil
}

// ensureSpace 保证再写 n 字节不违反容量约束：
// PolicyBlock 下等待最慢的已注册订阅者；两种策略都会推进 head
// 丢弃将被覆盖的记录。调用前须保证 n <= capacity。
func (w *Writer) ensureSpace(n uint64, block bool) error {
	if w.seg.policy == PolicyBlock {
		backoff := time.Microsecond * 50
		for {
			minSub, any := w.seg.minSubscriberOffset()
			if !any || w.writeOffset+n <= minSub+w.seg.capacity {
				break
			}
			if !block {
				return ErrWouldBlock
			}
			time.Sleep(backoff)
			if backoff < 2*time.Millisecond {
				backoff *= 2
			}
		}
	}
	if limit := w.writeOffset + n - w.seg.capacity; limit <= w.writeOffset { // 即 writeOffset+n >= capacity（无符号回绕时远大于 writeOffset）
		w.advanceHead(limit)
	}
	return nil
}

// advanceHead 把 head 推进到不小于 limit 的下一条记录起点，
// 丢弃所有起点小于 limit 的记录。调用前须保证 limit <= writeOffset，
// 因此被丢弃的都是已提交记录。
func (w *Writer) advanceHead(limit uint64) {
	if limit <= w.head {
		return
	}
	h, hs := w.head, w.headSeq
	for h < limit {
		cw := w.seg.loadU64(w.seg.headerSize + h%w.seg.capacity)
		_, length, padding := decodeCommitWord(cw)
		if padding {
			h += uint64(length)
		} else {
			h += recordLen(uint64(length))
			hs++
		}
	}
	// 先写 headSeq 再写 head，读者按相反顺序读取，
	// 偶发的不一致会被序号与 CRC 校验识别为缺口。
	w.seg.storeU64(offHeadSeq, hs)
	w.seg.storeU64(offHead, h)
	w.head, w.headSeq = h, hs
}

// Generation 返回当前段的代际标识。
func (w *Writer) Generation() uint64 { return w.seg.generation }

// Stats 返回段的诊断信息快照。
func (w *Writer) Stats() (Stats, error) {
	return w.seg.stats()
}

// Close 关闭写入方并释放写锁。
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.seg.close()
}
