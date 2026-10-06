package broadcast

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testPath(t *testing.T) string {
	t.Helper()
	return t.TempDir() + "/segment.shm"
}

// makePayload 构造可校验的测试载荷：前 8 字节为序号，其余为序号的低字节填充。
func makePayload(seq uint64, size int) []byte {
	p := make([]byte, size)
	binary.LittleEndian.PutUint64(p, seq)
	for i := 8; i < size; i++ {
		p[i] = byte(seq)
	}
	return p
}

func checkPayload(t *testing.T, ev Event, seq uint64, size int) {
	t.Helper()
	if ev.Seq != seq {
		t.Fatalf("event seq = %d, want %d", ev.Seq, seq)
	}
	if len(ev.Data) != size {
		t.Fatalf("payload len = %d, want %d", len(ev.Data), size)
	}
	if binary.LittleEndian.Uint64(ev.Data) != seq {
		t.Fatalf("payload head seq = %d, want %d", binary.LittleEndian.Uint64(ev.Data), seq)
	}
	for i, b := range ev.Data[8:] {
		if b != byte(seq) {
			t.Fatalf("payload[%d] = %d, want %d (seq=%d)", i+8, b, byte(seq), seq)
		}
	}
}

func TestWriteReadBasic(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const n = 100
	const size = 128
	for i := 0; i < n; i++ {
		seq, err := w.Write(makePayload(uint64(i), size))
		if err != nil {
			t.Fatal(err)
		}
		if seq != uint64(i) {
			t.Fatalf("write seq = %d, want %d", seq, i)
		}
	}

	sub, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// 读一半后保存进度，用 SubscribeAt 恢复。
	var pos Position
	for i := 0; i < n; i++ {
		ev, err := sub.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPayload(t, ev, uint64(i), size)
		if i == n/2-1 {
			pos = sub.Position()
		}
	}
	if _, err := sub.Next(); err != ErrEmpty {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
	sub.Close()

	sub2, err := SubscribeAt(path, pos)
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Close()
	for i := n / 2; i < n; i++ {
		ev, err := sub2.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPayload(t, ev, uint64(i), size)
	}
	if _, err := sub2.Next(); err != ErrEmpty {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
}

func TestSubscriberIsolation(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const size = 64
	writeN := func(from, to int) {
		for i := from; i < to; i++ {
			if _, err := w.Write(makePayload(uint64(i), size)); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeN(0, 10)

	sub1, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub1.Close()
	sub2, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Close()
	sub3, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub3.Close()

	// 三个订阅者以不同速度读取，进度互不影响。
	for i := 0; i < 10; i++ {
		ev, err := sub1.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPayload(t, ev, uint64(i), size)
	}
	for i := 0; i < 3; i++ {
		ev, err := sub2.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPayload(t, ev, uint64(i), size)
	}
	writeN(10, 15)

	// sub2 从第 3 条继续读到 15。
	for i := 3; i < 15; i++ {
		ev, err := sub2.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPayload(t, ev, uint64(i), size)
	}
	// sub3 从头读到 15。
	for i := 0; i < 15; i++ {
		ev, err := sub3.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPayload(t, ev, uint64(i), size)
	}
	// sub1 读到 15。
	for i := 10; i < 15; i++ {
		ev, err := sub1.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPayload(t, ev, uint64(i), size)
	}
	if _, err := sub1.Next(); err != ErrEmpty {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
}

func TestOverwritePolicy(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 64 << 10, Policy: PolicyOverwrite})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	fast, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	slow, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()

	const n = 500
	const size = 1000
	// 快订阅者紧跟写入，必须读满全部事件且不受慢订阅者影响。
	for i := 0; i < n; i++ {
		if _, err := w.Write(makePayload(uint64(i), size)); err != nil {
			t.Fatal(err)
		}
		ev, err := fast.Next()
		if err != nil {
			t.Fatalf("fast subscriber lagged at %d: %v", i, err)
		}
		checkPayload(t, ev, uint64(i), size)
	}

	// 慢订阅者的进度已被覆盖，必须能区分出缺口。
	_, err = slow.Next()
	var lagged *LaggedError
	if !errors.As(err, &lagged) {
		t.Fatalf("want LaggedError, got %v", err)
	}
	if lagged.HeadSeq == 0 {
		t.Fatalf("expected some events to be overwritten, HeadSeq=0")
	}
	t.Logf("slow subscriber lagged: %+v", lagged)

	// Resync 后能从最旧可用事件继续读，且内容完整。
	if err := slow.Resync(); err != nil {
		t.Fatal(err)
	}
	pos := slow.Position()
	if pos.Seq != lagged.HeadSeq {
		t.Fatalf("resync seq = %d, want headSeq %d", pos.Seq, lagged.HeadSeq)
	}
	for i := pos.Seq; i < n; i++ {
		ev, err := slow.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPayload(t, ev, i, size)
	}
}

func TestBlockPolicy(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 64 << 10, Policy: PolicyBlock})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	sub, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// 写满后 TryWrite 必须返回 ErrWouldBlock。
	const size = 1000
	var n int
	for {
		_, err := w.TryWrite(makePayload(uint64(n), size))
		if err == ErrWouldBlock {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n == 0 {
		t.Fatal("writer never filled the segment")
	}
	t.Logf("segment full after %d events", n)

	// Write 必须阻塞，直到订阅者读出空间。
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5; i++ {
			if _, err := w.Write(makePayload(uint64(n+i), size)); err != nil {
				t.Error(err)
			}
		}
	}()
	select {
	case <-done:
		t.Fatal("Write did not block on full segment")
	case <-time.After(200 * time.Millisecond):
	}
	// 订阅者槽位发布的是读取水位（保护最后一条正在读的事件），
	// 因此需要多读一条才能腾出 5 条的空间。
	for i := 0; i < 6; i++ {
		if _, err := sub.Next(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Write still blocked after subscriber read")
	}

	// 订阅者关闭后不再构成背压，写入不再阻塞。
	sub.Close()
	for i := 0; i < 200; i++ {
		if _, err := w.Write(makePayload(uint64(i), size)); err != nil {
			t.Fatalf("write with no subscribers blocked: %v", err)
		}
	}
}

func TestGeneration(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	gen1 := w.Generation()
	for i := 0; i < 10; i++ {
		if _, err := w.Write(makePayload(uint64(i), 64)); err != nil {
			t.Fatal(err)
		}
	}
	sub, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := sub.Next(); err != nil {
			t.Fatal(err)
		}
	}
	oldPos := sub.Position()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 同路径重建段：代际必须改变。
	w2, err := Create(path, Options{Capacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	gen2 := w2.Generation()
	if gen1 == gen2 {
		t.Fatal("generation did not change after recreate")
	}

	// 旧订阅者必须收到可区分的代际失效状态。
	if _, err := sub.Next(); err != ErrGenerationMismatch {
		t.Fatalf("want ErrGenerationMismatch, got %v", err)
	}
	if err := sub.Resync(); err != ErrGenerationMismatch {
		t.Fatalf("want ErrGenerationMismatch, got %v", err)
	}
	sub.Close()

	// 旧代际的进度不得被当作新段进度。
	if _, err := SubscribeAt(path, oldPos); err != ErrGenerationMismatch {
		t.Fatalf("want ErrGenerationMismatch, got %v", err)
	}

	// 新订阅者从新段的起点读取。
	w2.Write(makePayload(100, 64))
	sub2, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Close()
	ev, err := sub2.Next()
	if err != nil {
		t.Fatal(err)
	}
	if ev.Seq != 0 {
		t.Fatalf("new generation seq = %d, want 0", ev.Seq)
	}
	if !bytes.Equal(ev.Data, makePayload(100, 64)) {
		t.Fatal("payload corrupted across generation")
	}
}

func TestDiagnostics(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const size = 100
	for i := 0; i < 20; i++ {
		if _, err := w.Write(makePayload(uint64(i), size)); err != nil {
			t.Fatal(err)
		}
	}
	sub1, _ := Subscribe(path)
	defer sub1.Close()
	sub2, _ := Subscribe(path)
	defer sub2.Close()
	for i := 0; i < 8; i++ {
		if _, err := sub1.Next(); err != nil {
			t.Fatal(err)
		}
	}

	st, err := w.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Committed != 20 {
		t.Fatalf("Committed = %d, want 20", st.Committed)
	}
	if st.Tail == 0 || st.Backlog != st.Tail-st.Head {
		t.Fatalf("bad backlog stats: %+v", st)
	}
	if len(st.Subscribers) != 2 {
		t.Fatalf("subscribers = %d, want 2", len(st.Subscribers))
	}
	lags := map[uint64]uint64{}
	for _, s := range st.Subscribers {
		lags[s.LagEvents]++
		if s.LagBytes == 0 {
			t.Fatalf("subscriber %d has zero lag bytes", s.ID)
		}
	}
	// 水位语义：一个水位在第 7 条（落后 13 条），一个未读（落后 20 条）。
	if lags[13] != 1 || lags[20] != 1 {
		t.Fatalf("unexpected lag distribution: %+v", st.Subscribers)
	}

	// 订阅者也能读取诊断信息。
	if _, err := sub1.Stats(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentBlockPolicy(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 1 << 20, Policy: PolicyBlock})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const numSubs = 8
	const numEvents = 2000
	const size = 100

	subs := make([]*Subscriber, numSubs)
	for i := range subs {
		subs[i], err = Subscribe(path)
		if err != nil {
			t.Fatal(err)
		}
		defer subs[i].Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i, sub := range subs {
		wg.Add(1)
		go func(id int, s *Subscriber) {
			defer wg.Done()
			for want := uint64(0); want < numEvents; {
				ev, err := s.Wait(ctx)
				if err != nil {
					t.Errorf("subscriber %d: %v", id, err)
					return
				}
				if ev.Seq != want {
					t.Errorf("subscriber %d: seq = %d, want %d", id, ev.Seq, want)
					return
				}
				if binary.LittleEndian.Uint64(ev.Data) != want {
					t.Errorf("subscriber %d: payload seq = %d, want %d",
						id, binary.LittleEndian.Uint64(ev.Data), want)
					return
				}
				want++
			}
		}(i, sub)
	}

	for i := 0; i < numEvents; i++ {
		if _, err := w.Write(makePayload(uint64(i), size)); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestConcurrentOverwrite(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 256 << 10, Policy: PolicyOverwrite})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const numSubs = 4
	const numEvents = 20000
	const size = 512

	subs := make([]*Subscriber, numSubs)
	for i := range subs {
		subs[i], err = Subscribe(path)
		if err != nil {
			t.Fatal(err)
		}
		defer subs[i].Close()
	}

	var done atomic.Bool
	var wg sync.WaitGroup
	for i, sub := range subs {
		wg.Add(1)
		go func(id int, s *Subscriber) {
			defer wg.Done()
			var last uint64
			var started bool
			var gaps, received int
			for {
				ev, err := s.Next()
				var lagged *LaggedError
				switch {
				case err == nil:
					if started && ev.Seq <= last {
						t.Errorf("subscriber %d: seq went backwards %d -> %d", id, last, ev.Seq)
						return
					}
					if len(ev.Data) != size {
						t.Errorf("subscriber %d: bad payload len %d", id, len(ev.Data))
						return
					}
					started, last = true, ev.Seq
					received++
				case errors.As(err, &lagged):
					gaps++
					if err := s.Resync(); err != nil {
						t.Errorf("subscriber %d: resync: %v", id, err)
						return
					}
				case err == ErrEmpty:
					if done.Load() {
						t.Logf("subscriber %d: received=%d gaps=%d", id, received, gaps)
						return
					}
					time.Sleep(50 * time.Microsecond)
				default:
					t.Errorf("subscriber %d: %v", id, err)
					return
				}
			}
		}(i, sub)
	}

	for i := 0; i < numEvents; i++ {
		if _, err := w.Write(makePayload(uint64(i), size)); err != nil {
			t.Fatal(err)
		}
	}
	done.Store(true)
	wg.Wait()
}

func TestEventTooLarge(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write(make([]byte, 64<<10)); err != ErrEventTooLarge {
		t.Fatalf("want ErrEventTooLarge, got %v", err)
	}
	if _, err := w.Write(make([]byte, 32<<10)); err != nil {
		t.Fatalf("write below capacity failed: %v", err)
	}
}

func TestWriterExclusion(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(path, Options{Capacity: 1 << 20}); err != ErrWriterActive {
		t.Fatalf("want ErrWriterActive, got %v", err)
	}
	if _, err := OpenWriter(path); err != ErrWriterActive {
		t.Fatalf("want ErrWriterActive, got %v", err)
	}
	w.Close()
	w2, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	w2.Close()
}

func TestOpenWriterNotFound(t *testing.T) {
	path := testPath(t)
	if _, err := OpenWriter(path); err != ErrSegmentNotFound {
		t.Fatalf("want ErrSegmentNotFound, got %v", err)
	}
	if _, err := Subscribe(path); err != ErrSegmentNotFound {
		t.Fatalf("want ErrSegmentNotFound, got %v", err)
	}
}

func TestResyncToLatest(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 10; i++ {
		w.Write(makePayload(uint64(i), 64))
	}
	sub, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.ResyncToLatest(); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Next(); err != ErrEmpty {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
	w.Write(makePayload(10, 64))
	ev, err := sub.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkPayload(t, ev, 10, 64)
}

func TestWaitContextCancel(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	sub, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := sub.Wait(ctx); err != context.DeadlineExceeded {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}
}
