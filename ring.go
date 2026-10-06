package broadcast

import (
	"encoding/binary"
	"sync/atomic"
	"unsafe"
)

// 数据区记录布局（8 字节对齐）：
//
//	+0  u64 提交字（原子）：高 32 位为事件序号低 32 位，
//	    低 32 位中 bit31 为填充标记、bit0..30 为长度
//	+8  u64 CRC 字：低 32 位为载荷 CRC32(Castagnoli)
//	+16 载荷，按 8 字节对齐填充
//
// 对正式事件，长度字段为载荷长度；对填充记录，长度字段为
// 包含记录头在内的整段填充跨度（用于跳过到数据区末尾）。
const (
	recordHeaderSize = 16
	recordAlign      = 8

	flagPadding uint32 = 1 << 31
	lenMask     uint32 = flagPadding - 1

	maxPayloadLen = lenMask // 单条事件载荷上限
)

func align8(n uint64) uint64 { return (n + recordAlign - 1) &^ (recordAlign - 1) }

// recordLen 返回一条正式记录占用的总字节数。
func recordLen(payloadLen uint64) uint64 { return recordHeaderSize + align8(payloadLen) }

func encodeCommitWord(seq uint64, length uint32, padding bool) uint64 {
	l := length
	if padding {
		l |= flagPadding
	}
	return uint64(uint32(seq))<<32 | uint64(l)
}

func decodeCommitWord(cw uint64) (seqLow uint32, length uint32, padding bool) {
	l := uint32(cw)
	return uint32(cw >> 32), l & lenMask, l&flagPadding != 0
}

// atomicStoreBytes 以 8 字节原子存储把 src 写入共享内存 dst，
// dst 长度须为 align8(len(src))，末尾不足一个字的部分补零。
// 与读取方的原子按字拷贝配合，保证在 -race 下无数据竞争，
// 撕裂的载荷由 CRC 校验识别。
func atomicStoreBytes(dst, src []byte) {
	n := len(src)
	for off := 0; off < n; off += 8 {
		var w uint64
		if off+8 <= n {
			w = binary.LittleEndian.Uint64(src[off:])
		} else {
			var buf [8]byte
			copy(buf[:], src[off:])
			w = binary.LittleEndian.Uint64(buf[:])
		}
		atomic.StoreUint64((*uint64)(unsafe.Pointer(&dst[off])), w)
	}
}

// atomicLoadBytes 以 8 字节原子加载从共享内存 src 拷贝 n 字节到 dst。
func atomicLoadBytes(dst, src []byte) {
	n := len(dst)
	for off := 0; off < n; off += 8 {
		w := atomic.LoadUint64((*uint64)(unsafe.Pointer(&src[off])))
		if off+8 <= n {
			binary.LittleEndian.PutUint64(dst[off:], w)
		} else {
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], w)
			copy(dst[off:], buf[:])
		}
	}
}
