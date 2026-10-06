//go:build unix

package broadcast

import (
	"os"
	"syscall"
)

// mapFile 将文件整体以共享读写方式映射进内存。
// 同一台机器上的多个进程映射同一文件时，页缓存保证一致性视图。
func mapFile(f *os.File, size int) ([]byte, error) {
	return syscall.Mmap(int(f.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}

func unmap(b []byte) error {
	return syscall.Munmap(b)
}

// tryLockExclusive 尝试对文件加排他锁（非阻塞），
// 用于保证同一时刻只有一个写入进程。进程死亡时内核自动释放。
func tryLockExclusive(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	return nil
}

// pidAlive 检测进程是否存活，用于回收崩溃订阅者残留的槽位。
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}
