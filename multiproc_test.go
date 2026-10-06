package shmbroadcast

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestHelperProcess is re-executed in a child process to act as an
// independent OS process sharing the segment. It is dispatched entirely
// through environment variables and never runs in the parent.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("SHMBC_ROLE") == "" {
		return
	}
	path := os.Getenv("SHMBC_PATH")
	switch os.Getenv("SHMBC_ROLE") {
	case "crash-writer":
		// Open, publish a few events, then die without closing anything,
		// simulating a crashed writer.
		s, err := Open(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "open:", err)
			os.Exit(3)
		}
		w, err := s.Writer()
		if err != nil {
			fmt.Fprintln(os.Stderr, "writer:", err)
			os.Exit(3)
		}
		for i := 0; i < 5; i++ {
			if err := w.Publish([]byte(fmt.Sprintf("proc-%04d", i))); err != nil {
				fmt.Fprintln(os.Stderr, "publish:", err)
				os.Exit(3)
			}
		}
		os.Exit(2) // "crash": no Close, lock released by the kernel
	case "reader":
		n, _ := strconv.Atoi(os.Getenv("SHMBC_EVENTS"))
		s, err := Open(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "open:", err)
			os.Exit(3)
		}
		sub, err := s.Subscribe()
		if err != nil {
			fmt.Fprintln(os.Stderr, "subscribe:", err)
			os.Exit(3)
		}
		var received, lapped uint64
		var last uint64
		var haveLast bool
		deadline := time.Now().Add(30 * time.Second)
		for {
			p, err := sub.Next()
			if errors.Is(err, ErrNoData) {
				if time.Now().After(deadline) {
					fmt.Fprintf(os.Stderr, "timeout: received=%d last=%d\n", received, last)
					os.Exit(1)
				}
				time.Sleep(time.Millisecond)
				continue
			}
			if errors.Is(err, ErrLapped) {
				lapped++
				continue
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, "next:", err)
				os.Exit(1)
			}
			if len(p) != 32 {
				fmt.Fprintf(os.Stderr, "bad length %d\n", len(p))
				os.Exit(1)
			}
			got := binary.LittleEndian.Uint64(p)
			for k := 8; k < len(p); k++ {
				if p[k] != byte(got) {
					fmt.Fprintf(os.Stderr, "torn event %d\n", got)
					os.Exit(1)
				}
			}
			if haveLast && got <= last {
				fmt.Fprintf(os.Stderr, "order violation: %d after %d\n", got, last)
				os.Exit(1)
			}
			last, haveLast = got, true
			received++
			if got == uint64(n-1) {
				break
			}
		}
		fmt.Printf("OK received=%d lapped=%d\n", received, lapped)
		os.Exit(0)
	}
	os.Exit(0)
}

func runHelper(t *testing.T, env ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestCrashRecoveryAcrossProcesses: a writer process dies without
// closing anything; its committed events must remain readable, the
// writer lock must be recoverable, and a successor writer must continue
// the sequence without duplication.
func TestCrashRecoveryAcrossProcesses(t *testing.T) {
	path := segPath(t)
	s := mustCreate(t, path, Options{Capacity: 16, MaxEventSize: 64, Policy: PolicyOverwrite})

	out, err := runHelper(t, "SHMBC_ROLE=crash-writer", "SHMBC_PATH="+path)
	if err == nil {
		t.Fatalf("helper should have crashed, output: %s", out)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 {
		t.Fatalf("helper exit = %v, want code 2, output: %s", err, out)
	}

	// Committed events survive the writer's death.
	sub := mustSubscribe(t, s)
	for i := 0; i < 5; i++ {
		expectEvent(t, sub, fmt.Sprintf("proc-%04d", i))
	}
	if _, err := sub.Next(); !errors.Is(err, ErrNoData) {
		t.Fatalf("want ErrNoData, got %v", err)
	}

	// The crashed writer's lock was released by the kernel; a new writer
	// resumes the sequence exactly where the committed stream ended.
	w := mustWriter(t, s)
	if err := w.Publish([]byte("post-crash")); err != nil {
		t.Fatal(err)
	}
	expectEvent(t, sub, "post-crash")
	if st := s.Stats(); st.NextSeq != 7 {
		t.Fatalf("NextSeq = %d, want 7", st.NextSeq)
	}
}

// TestConcurrentReadersAcrossProcesses: one writer and three independent
// reader processes share an overwrite-policy segment under load. Every
// reader must observe a strictly increasing, untorn subsequence; laps
// are reported, never silent.
func TestConcurrentReadersAcrossProcesses(t *testing.T) {
	path := segPath(t)
	s := mustCreate(t, path, Options{Capacity: 64, MaxEventSize: 64, Policy: PolicyOverwrite})
	w := mustWriter(t, s)

	const n = 3000
	const readers = 3
	cmds := make([]*exec.Cmd, readers)
	outs := make([]*strings.Builder, readers)
	for i := 0; i < readers; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
		cmd.Env = append(os.Environ(),
			"SHMBC_ROLE=reader", "SHMBC_PATH="+path,
			"SHMBC_EVENTS="+strconv.Itoa(n))
		var b strings.Builder
		cmd.Stdout = &b
		cmd.Stderr = &b
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds[i] = cmd
		outs[i] = &b
	}

	time.Sleep(300 * time.Millisecond) // let readers register
	for i := uint64(0); i < n; i++ {
		if err := w.Publish(makePayload(i)); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("reader %d failed: %v\n%s", i, err, outs[i].String())
		}
		t.Logf("reader %d: %s", i, outs[i].String())
	}
}
