package broadcast

// Policy 决定段写满时的积压处理策略。
type Policy int

const (
	// PolicyBlock 在段满时阻塞写入方，直到最慢的已注册订阅者腾出空间。
	PolicyBlock Policy = iota
	// PolicyOverwrite 在段满时覆盖最旧的事件，落后的订阅者将收到 LaggedError。
	PolicyOverwrite
)

// Options 是创建段时的配置。
type Options struct {
	// Capacity 是数据区容量（字节），会向上对齐到页大小。
	// 为零时使用默认值 16MiB。
	Capacity uint64
	// Policy 是积压处理策略，零值为 PolicyBlock。
	Policy Policy
	// MaxSubscribers 是订阅者槽位数量，为零时使用默认值 64。
	MaxSubscribers int
}

const (
	defaultCapacity       = 16 << 20
	defaultMaxSubscribers = 64
	maxMaxSubscribers     = 4096
)

func (o *Options) normalize() {
	if o.Capacity == 0 {
		o.Capacity = defaultCapacity
	}
	if o.MaxSubscribers == 0 {
		o.MaxSubscribers = defaultMaxSubscribers
	}
	if o.MaxSubscribers > maxMaxSubscribers {
		o.MaxSubscribers = maxMaxSubscribers
	}
}

// Event 是一条读取到的事件。Data 是指向共享内存的零拷贝视图，
// 在 PolicyOverwrite 下若写入方已覆盖该区域，视图内容可能随后变化，
// 需要长期保留时请自行拷贝（如 bytes.Clone）。
type Event struct {
	// Seq 是事件的单调递增序号（从 0 开始，仅统计正式事件）。
	Seq uint64
	// Data 是事件载荷的零拷贝视图。
	Data []byte
}

// Position 标识一个订阅者在某一代际段内的读取进度，可用于断点恢复。
type Position struct {
	// Generation 是段的代际标识。
	Generation uint64
	// Offset 是下一条待读记录在数据区的单调字节偏移。
	Offset uint64
	// Seq 是下一条待读事件的序号。
	Seq uint64
}

// Stats 是段的诊断信息快照。
type Stats struct {
	Generation  uint64
	Capacity    uint64
	Policy      Policy
	Head        uint64 // 最旧可用记录偏移
	Tail        uint64 // 已提交写入位置
	Committed   uint64 // 已提交事件总数（即下一个待分配序号）
	Backlog     uint64 // Tail - Head，段内保留的字节数
	Subscribers []SubscriberStats
}

// SubscriberStats 是单个订阅者的诊断信息。
type SubscriberStats struct {
	ID        uint64
	Offset    uint64 // 订阅者当前读取偏移
	Seq       uint64 // 订阅者下一个待读序号
	LagBytes  uint64 // Tail - Offset，落后字节数
	LagEvents uint64 // Committed - Seq，落后事件数
}
