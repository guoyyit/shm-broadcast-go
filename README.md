# shm-broadcast-go

单机共享内存零拷贝广播通道：一个写入进程向可被多个读取进程映射的
共享内存段追加事件，每个订阅者维护相互独立的读取进度。仅依赖 Go
标准库（`syscall` 的 mmap/flock），无第三方依赖。

## 快速开始

```go
// 创建（或重建）段文件
seg, _ := shmbroadcast.Create("/tmp/demo.seg", shmbroadcast.Options{
    Capacity:     1024,   // 环形槽位数
    MaxEventSize: 4096,   // 单事件最大负载
    Policy:       shmbroadcast.PolicyOverwrite,
})
defer seg.Close()

// 写入方（同一时刻全机只允许一个 Writer，由文件锁保证）
w, _ := seg.Writer()
w.Publish([]byte("hello"))

// 订阅方（每个订阅者进度独立）
sub, _ := seg.Subscribe()
for {
    ev, err := sub.Next()
    switch {
    case errors.Is(err, shmbroadcast.ErrNoData):   // 已追上写者
    case errors.Is(err, shmbroadcast.ErrLapped):   // 有缺口，游标已自动对齐到最旧可用事件
    case err != nil:                               // ErrGenerationMismatch / ErrClosed
    default:
        _ = ev // 完整、不撕裂的单条事件
    }
}
```

零拷贝读取用 `sub.NextView(func(p []byte) error { ... })`：回调内的
`p` 直接别名共享内存，回调返回后库会校验读取期间槽位未被覆写；
若被覆写则返回 `ErrLapped`，回调中观察到的内容必须丢弃。

## 一致性语义

### 事件原子性（不撕裂、无中间态）

每个槽位有一个 64 位 `seq` 字。写者按三步提交：

1. 存 `seq | writing标记位`（使槽位对读者失效）；
2. 写入负载与长度；
3. 存 `seq`（release 语义）提交，随后推进头部写位置。

读者在读取前后各检查一次 `seq`，两次值相同且等于期望序号才接受。
任何与第 2 步重叠的读取必然被两次检查之一发现，因此**读者永远不
会看到写了一半的事件**。所有跨进程共享的序号、游标都通过
`sync/atomic` 访问，保证多进程下的内存可见性。

### 序号与代际

- 事件序号从 1 开始单调递增，槽位下标为 `seq % Capacity`。
- 每次 `Create` 重建段都会更换 **generation**（能读到旧头则旧值
  +1，否则取时间种子）。`Position{Generation, Seq}` 是可持久化游
  标；旧代际的 Position 在新段上会被 `SubscribeAt` 以
  `ErrGenerationMismatch` 拒绝，已持有旧代际游标的订阅者在下一次
  读取时收到同样的错误，**不会**把旧偏移当作新段进度。

### 积压策略

- `PolicyOverwrite`：写满后覆盖最旧事件。写者**永远不会**被任何
  订阅者阻塞。被覆盖的订阅者下次读取收到 `ErrLapped`（可区分的数
  据缺口），游标自动对齐到最旧保留事件，不会静默读到陈旧内容。
- `PolicyBlock`：写满后 `Publish` 等待，直到最慢的**已注册**订阅
  者腾出槽位；`TryPublish` 则返回 `ErrWouldBlock`。没有任何订阅
  者时写者永不阻塞。注意：一个注册了却长期不读取也不关闭的订阅者
  最终会让写者阻塞——需要写者绝对不被拖慢时请用 Overwrite 策略，
  或及时 `Close` 订阅者。任何策略下，一个订阅者的落后都不影响其
  他订阅者的读取。

### 崩溃与重挂载

- 进程崩溃不会丢失已提交事件：已提交数据在共享页缓存中，重新映射
  同一段文件即可继续读。
- 未提交的写入对任何读者不可见：崩溃时残留的 `writing` 标记或半
  截负载永远通不过读者的 seq 校验；新写者从头部记录的写位置继续，
  原槽位被干净复用，不产生重复或错位。
- 写者角色由 `flock(LOCK_EX)` 保护，进程死亡时内核自动释放，后继
  写者可立即接管（`ErrWriterExists` 仅在写者存活时返回）。
- 重建段请始终调用 `Create`（原地截断重写，同一 inode），**不要**
  先删除文件再新建，否则旧映射无法感知代际变化。

## 段文件对外约定

同一机器上的进程可按下列布局直接解析段文件（多字节整数为本机字
节序、8 字节对齐；仅保证同主机兼容）：

```
偏移        内容
0           magic uint64 = 0x53484D2D42435354 ("SHM-BCST")，最后发布
8           version uint32 = 1
12          policy uint32 (0=Overwrite, 1=Block)
16          generation uint64
24          capacity uint64（槽位数）
32          slotSize uint64（最大负载字节）
40          slotStride uint64（槽位步长 = align8(16+slotSize)）
64          nextSeq uint64（原子；下一个待写序号，即已提交写位置）
72          subIDSeq uint64（原子；订阅者 ID 计数器）
128..4096   订阅者注册表：124 项 × 32 字节
            每项: id uint64 | cursor uint64 | state uint64(0空/1占位/2活跃)
4096..      槽位区：capacity × slotStride
            每槽: seq uint64（最高位为 writing 标记）| len uint32 |
                  保留 uint32 | 负载 slotSize 字节
```

不变式：

- 序号 1 起；`seq==0` 表示槽位从未提交。
- 已提交事件集合为 `(max(1,nextSeq-capacity), nextSeq)` 左开右开区
  间内的序号，即最旧保留序号 `oldestSeq = max(1, nextSeq-capacity)`。
- 读者看到槽位 `seq > 期望序号` ⇒ 已被覆盖（缺口）；`seq < 期望序
  号` ⇒ 尚未提交。
- 订阅者应把读进度原子写回注册表自己的 `cursor`，供诊断与
  PolicyBlock 背压使用；退出时把 `state` 置 0。

## 诊断

`seg.Stats()` 返回代际、容量、写位置（`NextSeq`）、最旧保留序号
以及每个订阅者的游标与积压量（`Backlog = NextSeq - Cursor`）。

## 限制

- 仅单机：格式为本机字节序，同步原语依赖本机共享内存语义。
- 最多 124 个并发订阅者（注册表写满返回 `ErrSubscriberLimit`）。
- 单事件负载不超过创建时的 `MaxEventSize`。
- 段被 `Create` 重建后，旧句柄只能安全地感知代际变化（返回
  `ErrGenerationMismatch`），应 `Close` 后重新 `Open`。

## 测试

```sh
go test -race ./...
```

覆盖：多订阅者进度隔离、两种写满策略、崩溃后重挂载（含跨进程
helper 进程模拟崩溃写者）、未提交写入不可见、代际切换、同进程与
跨进程高并发读写。
