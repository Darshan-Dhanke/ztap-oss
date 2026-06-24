// ztap-oss connection proxy (custom component #1).
//
// A suspend/resume-aware Postgres proxy. When the (simulated) compute is
// "suspended" after an idle period, an incoming client connection is held open
// while the proxy "wakes" the compute, then the connection is transparently
// proxied through to the real Postgres backend — the client never sees the cold
// start beyond the latency.
//
// Honest scope: there is no real scale-to-zero compute to wake here, so the
// suspend/resume of the *compute* is simulated (a state machine plus a
// configurable wake delay). What is real: Postgres wire-protocol handling for
// SSL/GSS/startup negotiation, buffering the startup packet during the cold
// start, holding+serializing concurrent connections through a single wake, and
// transparent byte-level proxying of an actual psql session.
package main

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stateActive    = "active"
	stateWaking    = "waking"
	stateSuspended = "suspended"

	codeSSLRequest = 80877103
	codeGSSRequest = 80877104
	codeCancel     = 80877102
)

type manager struct {
	backendAddr  string
	idleTimeout  time.Duration
	wakeDuration time.Duration

	mu          sync.Mutex
	state       string
	activeConns int
	idleTimer   *time.Timer

	wakeMu     sync.Mutex // serializes cold-start wakes
	totalConns int64
	wakeCount  int64
}

func (m *manager) snapshot() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return map[string]any{
		"state":            m.state,
		"active_conns":     m.activeConns,
		"total_conns":      atomic.LoadInt64(&m.totalConns),
		"wake_count":       atomic.LoadInt64(&m.wakeCount),
		"idle_timeout_ms":  m.idleTimeout.Milliseconds(),
		"wake_duration_ms": m.wakeDuration.Milliseconds(),
		"backend":          m.backendAddr,
	}
}

// ensureAwake blocks the caller until the compute is active, performing (and
// counting) exactly one cold start even if many connections arrive at once.
func (m *manager) ensureAwake() {
	m.wakeMu.Lock()
	defer m.wakeMu.Unlock()

	m.mu.Lock()
	if m.state == stateActive {
		m.mu.Unlock()
		return
	}
	m.state = stateWaking
	m.mu.Unlock()

	log.Printf("cold start: compute suspended, waking (simulated %s)...", m.wakeDuration)
	time.Sleep(m.wakeDuration)

	m.mu.Lock()
	m.state = stateActive
	m.mu.Unlock()
	atomic.AddInt64(&m.wakeCount, 1)
	log.Printf("cold start complete: compute active")
}

func (m *manager) connOpened() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeConns++
	atomic.AddInt64(&m.totalConns, 1)
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
}

func (m *manager) connClosed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeConns--
	if m.activeConns <= 0 && m.idleTimeout > 0 {
		m.idleTimer = time.AfterFunc(m.idleTimeout, m.suspendIfIdle)
	}
}

func (m *manager) suspendIfIdle() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeConns == 0 && m.state == stateActive {
		m.state = stateSuspended
		log.Printf("idle %s elapsed: compute suspended", m.idleTimeout)
	}
}

// forceSuspend lets the /suspend endpoint simulate scale-to-zero on demand.
func (m *manager) forceSuspend() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = stateSuspended
	log.Printf("forced suspend via API")
}

// readStartup reads the client's initial message(s), declining SSL/GSS
// encryption requests (replying 'N') until it gets the real StartupMessage,
// which it returns verbatim so it can be replayed to the backend after waking.
func readStartup(conn net.Conn) ([]byte, error) {
	for {
		header := make([]byte, 8)
		if _, err := io.ReadFull(conn, header); err != nil {
			return nil, err
		}
		length := binary.BigEndian.Uint32(header[0:4])
		code := binary.BigEndian.Uint32(header[4:8])

		if length == 8 && (code == codeSSLRequest || code == codeGSSRequest) {
			// Decline encryption; the client will retry in plaintext.
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return nil, err
			}
			continue
		}
		// StartupMessage / CancelRequest: read the remaining body and return all.
		if length < 8 {
			return header, nil
		}
		remaining := make([]byte, length-8)
		if _, err := io.ReadFull(conn, remaining); err != nil {
			return nil, err
		}
		return append(header, remaining...), nil
	}
}

func (m *manager) handle(client net.Conn) {
	defer client.Close()

	// 1. Speak enough wire protocol to capture the startup packet (and decline
	//    SSL). This buffers the client's intent while the compute is still cold.
	startup, err := readStartup(client)
	if err != nil {
		log.Printf("startup read error: %v", err)
		return
	}

	// 2. Hold the connection open through the (simulated) cold start.
	m.ensureAwake()

	// 3. Now connect to the real backend and replay the buffered startup packet.
	backend, err := net.Dial("tcp", m.backendAddr)
	if err != nil {
		log.Printf("backend dial error: %v", err)
		return
	}
	defer backend.Close()
	if _, err := backend.Write(startup); err != nil {
		log.Printf("backend startup write error: %v", err)
		return
	}

	m.connOpened()
	defer m.connClosed()

	// 4. Transparent bidirectional splice for the rest of the session.
	done := make(chan struct{}, 2)
	go func() { io.Copy(backend, client); done <- struct{}{} }()
	go func() { io.Copy(client, backend); done <- struct{}{} }()
	<-done
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDur(key string, defMs int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n) * time.Millisecond
		}
	}
	return time.Duration(defMs) * time.Millisecond
}

func main() {
	listenAddr := env("PROXY_LISTEN", ":5432")
	httpAddr := env("HTTP_ADDR", ":8000")
	m := &manager{
		backendAddr:  env("BACKEND_ADDR", "postgres:5432"),
		idleTimeout:  envDur("IDLE_TIMEOUT_MS", 30000),
		wakeDuration: envDur("WAKE_DURATION_MS", 800),
		state:        stateActive,
	}

	// Observability / control HTTP API.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok","service":"ztap-proxy"}`))
	})
	mux.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.snapshot())
	})
	mux.HandleFunc("/suspend", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		m.forceSuspend()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.snapshot())
	})
	go func() {
		log.Printf("proxy http api on %s", httpAddr)
		log.Fatal(http.ListenAndServe(httpAddr, mux))
	}()

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}
	log.Printf("ztap proxy listening on %s -> %s (idle=%s wake=%s)",
		listenAddr, m.backendAddr, m.idleTimeout, m.wakeDuration)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go m.handle(conn)
	}
}
