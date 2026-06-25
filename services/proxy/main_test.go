package main

import (
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func sslRequest() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:4], 8)
	binary.BigEndian.PutUint32(b[4:8], codeSSLRequest)
	return b
}

func startupMessage(payload string) []byte {
	body := append([]byte{0, 3, 0, 0}, []byte(payload)...) // proto 3.0 + params
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(body)+4))
	return append(b, body...)
}

func TestReadStartupDeclinesSSLThenCaptures(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	defer srv.Close()

	go func() {
		cli.Write(sslRequest())
		// server should reply 'N' (decline) — read it before sending startup
		buf := make([]byte, 1)
		io.ReadFull(cli, buf)
		if buf[0] != 'N' {
			t.Errorf("expected SSL decline 'N', got %q", buf[0])
		}
		cli.Write(startupMessage("user\x00ztap\x00"))
	}()

	got, err := readStartup(srv)
	if err != nil {
		t.Fatalf("readStartup error: %v", err)
	}
	want := startupMessage("user\x00ztap\x00")
	if string(got) != string(want) {
		t.Fatalf("captured startup mismatch:\n got %v\nwant %v", got, want)
	}
}

func TestReadStartupPlainStartup(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	defer srv.Close()
	msg := startupMessage("database\x00ztap\x00")
	go func() { cli.Write(msg) }()

	got, err := readStartup(srv)
	if err != nil {
		t.Fatalf("readStartup error: %v", err)
	}
	if string(got) != string(msg) {
		t.Fatalf("startup not captured verbatim")
	}
}

func TestEnsureAwakeWakesOnceAndCounts(t *testing.T) {
	m := &manager{state: stateSuspended, wakeDuration: 2 * time.Millisecond}
	m.ensureAwake()
	if m.state != stateActive {
		t.Fatalf("expected active after wake, got %s", m.state)
	}
	if atomic.LoadInt64(&m.wakeCount) != 1 {
		t.Fatalf("expected wake_count 1, got %d", m.wakeCount)
	}
	// already active -> no extra wake
	m.ensureAwake()
	if atomic.LoadInt64(&m.wakeCount) != 1 {
		t.Fatalf("active wake should be a no-op, wake_count=%d", m.wakeCount)
	}
}

func TestConcurrentConnectionsTriggerSingleWake(t *testing.T) {
	m := &manager{state: stateSuspended, wakeDuration: 20 * time.Millisecond}
	done := make(chan struct{})
	for i := 0; i < 5; i++ {
		go func() { m.ensureAwake(); done <- struct{}{} }()
	}
	for i := 0; i < 5; i++ {
		<-done
	}
	if got := atomic.LoadInt64(&m.wakeCount); got != 1 {
		t.Fatalf("5 concurrent conns should cause exactly 1 wake, got %d", got)
	}
}

func TestForceSuspend(t *testing.T) {
	m := &manager{state: stateActive}
	m.forceSuspend()
	if m.state != stateSuspended {
		t.Fatalf("expected suspended, got %s", m.state)
	}
}

func TestIdleTimerSuspends(t *testing.T) {
	m := &manager{state: stateActive, idleTimeout: 10 * time.Millisecond, autoSuspend: true}
	m.connOpened()
	m.connClosed() // schedules suspend after idleTimeout
	time.Sleep(40 * time.Millisecond)
	if m.state != stateSuspended {
		t.Fatalf("expected suspend after idle, got %s", m.state)
	}
}

func TestConnOpenedCancelsIdleTimer(t *testing.T) {
	m := &manager{state: stateActive, idleTimeout: 20 * time.Millisecond, autoSuspend: true}
	m.connOpened()
	m.connClosed()    // schedule suspend
	m.connOpened()    // a new conn should cancel the pending suspend
	time.Sleep(40 * time.Millisecond)
	if m.state != stateActive {
		t.Fatalf("idle timer should have been cancelled, state=%s", m.state)
	}
	m.connClosed()
}

func TestRealSuspendDisabledWithoutDocker(t *testing.T) {
	m := &manager{state: stateActive}
	if m.realSuspend() {
		t.Fatal("realSuspend must be false when no docker client is set")
	}
}
