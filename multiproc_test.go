package broadcast

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// TestHelperProcess 是子进程入口，通过环境变量与参数驱动不同角色。
func TestHelperProcess(t *testing.T) {
	if os.Getenv("SHM_TEST_HELPER") == "" {
		return
	}
	args := os.Args
	for i, a := range args {
		if a == "--" {
			args = args[i+1:]
			break
		}
	}
	mode, path := args[0], args[1]
	switch mode {
	case "writer": // writer <path> <n> <size>：写 n 条后不 Close 直接退出，模拟崩溃
		n, _ := strconv.Atoi(args[2])
		size, _ := strconv.Atoi(args[3])
		w, err := Create(path, Options{Capacity: 1 << 20})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		for i := 0; i < n; i++ {
			if _, err := w.Write(makePayload(uint64(i), size)); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
		}
		os.Exit(0) // 不 Close，模拟进程崩溃
	case "writer-loop": // writer-loop <path> <size>：持续写入直到被杀
		size, _ := strconv.Atoi(args[2])
		w, err := Create(path, Options{Capacity: 1 << 20, Policy: PolicyOverwrite})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		for i := uint64(0); ; i++ {
			if _, err := w.Write(makePayload(i, size)); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
		}
	case "subscriber": // subscriber <path> <n> <size>：校验完整收到 n 条
		n, _ := strconv.Atoi(args[2])
		size, _ := strconv.Atoi(args[3])
		sub, err := Subscribe(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for want := uint64(0); want < uint64(n); want++ {
			ev, err := sub.Wait(ctx)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(3)
			}
			if ev.Seq != want || len(ev.Data) != size ||
				binary.LittleEndian.Uint64(ev.Data) != want {
				fmt.Fprintf(os.Stderr, "mismatch at %d: seq=%d len=%d\n",
					want, ev.Seq, len(ev.Data))
				os.Exit(3)
			}
		}
		fmt.Println("OK")
		os.Exit(0)
	case "subscriber-hold": // subscriber-hold <path>：注册后挂起，等待被杀
		sub, err := Subscribe(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		_ = sub
		fmt.Println("READY")
		select {}
	}
	os.Exit(0)
}

func runHelper(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"-test.run=TestHelperProcess", "--"}, args...)...)
	cmd.Env = append(os.Environ(), "SHM_TEST_HELPER=1")
	return cmd
}

// TestCrashRecovery 验证写入进程崩溃（未 Close）后重新挂载：
// 已提交事件必须仍然可读，新写入从最后提交位置继续，不重复不错位。
func TestCrashRecovery(t *testing.T) {
	path := testPath(t)
	const n = 500
	const size = 100

	cmd := runHelper(t, "writer", path, strconv.Itoa(n), strconv.Itoa(size))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("writer helper: %v\n%s", err, out)
	}

	// 重新挂载写入方，继续写入。
	w, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	st, err := w.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Committed != n {
		t.Fatalf("committed = %d, want %d", st.Committed, n)
	}
	const more = 100
	for i := 0; i < more; i++ {
		seq, err := w.Write(makePayload(uint64(n+i), size))
		if err != nil {
			t.Fatal(err)
		}
		if seq != uint64(n+i) {
			t.Fatalf("seq = %d, want %d (duplicate or misaligned)", seq, n+i)
		}
	}

	// 订阅者必须读到全部已提交事件，序号连续、内容完整。
	sub, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for i := 0; i < n+more; i++ {
		ev, err := sub.Next()
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		checkPayload(t, ev, uint64(i), size)
	}
	if _, err := sub.Next(); err != ErrEmpty {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
}

// TestCrashMidWrite 验证写入进程在写入循环中被 SIGKILL 后：
// 所有已提交事件完整可读，无撕裂、无错位，新写入可继续。
func TestCrashMidWrite(t *testing.T) {
	path := testPath(t)
	const size = 64

	cmd := runHelper(t, "writer-loop", path, strconv.Itoa(size))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()

	w, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	st, err := w.Stats()
	if err != nil {
		t.Fatal(err)
	}
	n := st.Committed
	if n == 0 {
		t.Fatal("no events committed before kill")
	}
	t.Logf("committed before kill: %d", n)

	// 已提交且仍保留在段内的事件必须连续且完整；
	// 被覆盖的旧事件必须以可区分的缺口形式呈现。
	sub, err := Subscribe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	// 订阅起点为段内最旧保留事件，期望序号从该处开始。
	i := sub.Position().Seq
	t.Logf("retained window starts at seq=%d (committed=%d)", i, n)
	for {
		ev, err := sub.Next()
		var lagged *LaggedError
		switch {
		case errors.As(err, &lagged):
			if err := sub.Resync(); err != nil {
				t.Fatal(err)
			}
			i = sub.Position().Seq
			t.Logf("resynced to headSeq=%d (committed=%d)", i, n)
		case err == ErrEmpty:
			if i != n {
				t.Fatalf("read up to seq %d, committed %d", i, n)
			}
			goto drained
		case err != nil:
			t.Fatal(err)
		default:
			checkPayload(t, ev, i, size)
			i++
		}
	}
drained:

	// 新写入从最后提交位置继续，序号连续。
	for i := n; i < n+10; i++ {
		seq, err := w.Write(makePayload(i, size))
		if err != nil {
			t.Fatal(err)
		}
		if seq != i {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
		ev, err := sub.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPayload(t, ev, i, size)
	}
}

// TestMultiProcessBroadcast 验证一个写入进程向多个订阅进程广播。
func TestMultiProcessBroadcast(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 1 << 20, Policy: PolicyBlock})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const n = 2000
	const size = 100
	const numSubs = 3

	cmds := make([]*exec.Cmd, numSubs)
	for i := 0; i < numSubs; i++ {
		cmds[i] = runHelper(t, "subscriber", path, strconv.Itoa(n), strconv.Itoa(size))
		if err := cmds[i].Start(); err != nil {
			t.Fatal(err)
		}
	}
	// 给订阅进程一点注册时间。
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < n; i++ {
		if _, err := w.Write(makePayload(uint64(i), size)); err != nil {
			t.Fatal(err)
		}
	}
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("subscriber %d failed: %v", i, err)
		}
	}
}

// TestSubscriberReaping 验证崩溃的订阅者残留槽位会被回收，
// 不会永久阻塞 PolicyBlock 下的写入方。
func TestSubscriberReaping(t *testing.T) {
	path := testPath(t)
	w, err := Create(path, Options{Capacity: 64 << 10, Policy: PolicyBlock})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	cmd := runHelper(t, "subscriber-hold", path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "READY" {
		t.Fatal("subscriber did not become ready")
	}
	pid := cmd.Process.Pid

	// 订阅者存活且不读取时，写入方必须被阻塞（写满后 ErrWouldBlock）。
	const size = 1000
	var n int
	for {
		_, err := w.TryWrite(makePayload(uint64(n), size))
		if errors.Is(err, ErrWouldBlock) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
	}

	// 杀掉订阅者进程（模拟崩溃），其残留槽位应被回收，
	// 写入方通过回收继续，而不是永久阻塞。
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	if pidAlive(pid) {
		t.Fatal("subscriber process should be dead")
	}
	done := make(chan error, 1)
	go func() {
		_, err := w.Write(makePayload(uint64(n), size))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write after reaping failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("writer still blocked by dead subscriber")
	}
}
