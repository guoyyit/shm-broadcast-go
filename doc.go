// Package broadcast 提供单机共享内存的零拷贝广播通道：
// 同一台机器上的一个写入进程向可被多个读取进程映射的段文件追加事件，
// 每个订阅者维护相互独立的读取进度。
//
// # 基本用法
//
// 写入方创建（或重建）段：
//
//	w, err := broadcast.Create("/tmp/my.seg", broadcast.Options{
//		Capacity: 16 << 20,
//		Policy:   broadcast.PolicyOverwrite,
//	})
//	seq, err := w.Write(payload)
//
// 订阅方挂载并读取：
//
//	sub, err := broadcast.Subscribe("/tmp/my.seg")
//	ev, err := sub.Next() // ErrEmpty 表示暂无新事件
//
// # 一致性语义
//
//   - 单条事件是完整且不可分割的：写入方先写载荷与 CRC，再以一次原子存储
//     提交记录头，最后推进 tail；读取方以 tail 为可见性门槛，并在读取前后
//     校验序号与 CRC，因此永远不会观察到写入中间状态或撕裂的事件。
//   - 崩溃恢复：进程崩溃时未提交的写入不对任何读取方可见；重新挂载后
//     从最后提交位置继续，残留的中间字节会被后续写入覆盖，不会造成
//     重复或错位。
//   - 代际：同一路径重建段会分配新的代际标识，旧订阅者持有的进度
//     在新段上被判定为失效（ErrGenerationMismatch）。
//   - 零拷贝：Event.Data 直接引用共享内存。PolicyBlock 下该视图在
//     下一次 Next/Wait/Resync 调用前不被覆盖（由读取水位背压保证）；
//     PolicyOverwrite 下写入方随时可能覆盖，需要保留请自行拷贝。
//
// # 积压策略
//
//   - PolicyBlock：段满时写入方阻塞，直到最慢的已注册订阅者推进
//     读取水位；TryWrite 则返回 ErrWouldBlock。已退出的订阅者进程
//     的残留槽位会被自动回收，不会永久阻塞写入方。
//   - PolicyOverwrite：段满时覆盖最旧事件，落后的订阅者在下一次
//     读取时收到可区分的 *LaggedError，可调用 Resync 显式确认缺口
//     并继续，不会静默读到陈旧内容。
//
// # 平台支持
//
// 目前支持类 Unix 系统（darwin、linux 等，见 mmap_unix.go 的构建约束）。
// 段文件按本机字节序解释，仅供同一台机器上的进程共享。
package broadcast
