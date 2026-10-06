package broadcast

import (
	"errors"
	"fmt"
)

var (
	// ErrEmpty 表示当前没有可读的新事件（读取方已追上写入位置）。
	ErrEmpty = errors.New("broadcast: no new events")

	// ErrWouldBlock 表示在 PolicyBlock 策略下段已满，TryWrite 无法立即写入。
	ErrWouldBlock = errors.New("broadcast: segment is full")

	// ErrWriterActive 表示已有另一个写入进程持有该段的写锁。
	ErrWriterActive = errors.New("broadcast: another writer already holds the segment")

	// ErrTooManySubscribers 表示订阅者槽位已用尽。
	ErrTooManySubscribers = errors.New("broadcast: too many subscribers")

	// ErrEventTooLarge 表示事件超过段容量允许的最大长度。
	ErrEventTooLarge = errors.New("broadcast: event too large")

	// ErrSegmentNotFound 表示段文件不存在。
	ErrSegmentNotFound = errors.New("broadcast: segment not found")

	// ErrInvalidSegment 表示段文件魔数或版本不匹配。
	ErrInvalidSegment = errors.New("broadcast: invalid segment file")

	// ErrGenerationMismatch 表示订阅者持有的代际与段当前代际不一致，
	// 旧代际的读取进度在新段上失效。
	ErrGenerationMismatch = errors.New("broadcast: generation mismatch")

	// ErrPositionInvalid 表示恢复位置超出当前有效区间。
	ErrPositionInvalid = errors.New("broadcast: position out of range")

	// ErrClosed 表示对象已关闭。
	ErrClosed = errors.New("broadcast: closed")
)

// LaggedError 表示订阅者的读取进度已被覆盖策略淘汰，出现数据缺口。
// 调用方收到该错误后可使用 Resync 重新对齐到当前最旧可用事件。
type LaggedError struct {
	// Offset 是订阅者当前的读取偏移（已失效）。
	Offset uint64
	// Head 是段内当前最旧可用记录的偏移。
	Head uint64
	// Seq 是订阅者期望读取的下一个事件序号（已失效）。
	Seq uint64
	// HeadSeq 是段内当前最旧可用事件的序号。
	HeadSeq uint64
}

func (e *LaggedError) Error() string {
	return fmt.Sprintf("broadcast: subscriber lagged: offset %d overwritten (head=%d), seq %d lost (headSeq=%d)",
		e.Offset, e.Head, e.Seq, e.HeadSeq)
}
