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

	// real suspend/resume: when set, the proxy stops/starts an actual container
	// via the Docker API instead of using a sleep timer.
	docker           *dockerClient
	computeContainer string
	autoSuspend      bool

	mu          sync.Mutex
	state       string
	activeConns int
	idleTimer   *time.Timer

	wakeMu          sync.Mutex // serializes cold-start wakes
	totalConns      int64
	wakeCount       int64
	lastColdStartMs int64
}

func (m *manager) realSuspend() bool { return m.docker != nil && m.computeContainer != "" }

func (m *manager) snapshot() map[string]any {
	m.mu.Lock()
	st := m.state
	conns := m.activeConns
	m.mu.Unlock()

	mode := "simulated"
	containerRunning := any(nil)
	if m.realSuspend() {
		mode = "real (docker stop/start)"
		if r, err := m.docker.running(m.computeContainer); err == nil {
			containerRunning = r
			// keep reported state honest with the actual container
			if conns == 0 {
				m.mu.Lock()
				if m.state != stateWaking {
					if r {
						m.state = stateActive
					} else {
						m.state = stateSuspended
					}
					st = m.state
				}
				m.mu.Unlock()
			}
		}
	}
	return map[string]any{
		"state":              st,
		"mode":               mode,
		"compute_container":  m.computeContainer,
		"container_running":  containerRunning,
		"active_conns":       conns,
		"total_conns":        atomic.LoadInt64(&m.totalConns),
		"wake_count":         atomic.LoadInt64(&m.wakeCount),
		"last_cold_start_ms": atomic.LoadInt64(&m.lastColdStartMs),
		"auto_suspend":       m.autoSuspend,
		"backend":            m.backendAddr,
	}
}

// ensureAwake blocks the caller until the compute is active, performing (and
// counting) exactly one cold start even if many connections arrive at once.
func (m *manager) ensureAwake() {
	m.wakeMu.Lock()
	defer m.wakeMu.Unlock()

	if m.realSuspend() {
		running, err := m.docker.running(m.computeContainer)
		if err == nil && running {
			m.setState(stateActive)
			return
		}
		m.setState(stateWaking)
		log.Printf("cold start: starting container %s ...", m.computeContainer)
		if err := m.docker.start(m.computeContainer); err != nil {
			log.Printf("container start error: %v", err)
		}
		took, err := waitReady(m.backendAddr, 60*time.Second)
		if err != nil {
			log.Printf("compute did not become ready: %v", err)
		}
		atomic.StoreInt64(&m.lastColdStartMs, took.Milliseconds())
		m.setState(stateActive)
		atomic.AddInt64(&m.wakeCount, 1)
		log.Printf("cold start complete: %s ready in %dms", m.computeContainer, took.Milliseconds())
		return
	}

	// simulated mode (no Docker socket): hold for a fixed wake delay
	m.mu.Lock()
	if m.state == stateActive {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	m.setState(stateWaking)
	log.Printf("cold start: compute suspended, waking (simulated %s)...", m.wakeDuration)
	time.Sleep(m.wakeDuration)
	m.setState(stateActive)
	atomic.AddInt64(&m.wakeCount, 1)
	log.Printf("cold start complete: compute active")
}

func (m *manager) setState(s string) {
	m.mu.Lock()
	m.state = s
	m.mu.Unlock()
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
	// Only auto-suspend on idle when explicitly enabled. With real suspend the
	// compute is the shared platform Postgres, so auto-stopping it on proxy-idle
	// would disrupt the platform — suspend is triggered explicitly instead.
	if m.autoSuspend && m.activeConns <= 0 && m.idleTimeout > 0 {
		m.idleTimer = time.AfterFunc(m.idleTimeout, m.suspendIfIdle)
	}
}

func (m *manager) suspendIfIdle() {
	m.mu.Lock()
	idle := m.activeConns == 0
	m.mu.Unlock()
	if idle {
		m.doSuspend("idle timeout")
	}
}

// forceSuspend lets the /suspend endpoint scale the compute to zero on demand.
func (m *manager) forceSuspend() {
	m.doSuspend("api")
}

func (m *manager) doSuspend(reason string) {
	if m.realSuspend() {
		log.Printf("suspend (%s): stopping container %s ...", reason, m.computeContainer)
		if err := m.docker.stop(m.computeContainer); err != nil {
			log.Printf("container stop error: %v", err)
		}
	}
	m.setState(stateSuspended)
	log.Printf("compute suspended (%s)", reason)
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
		autoSuspend:  env("AUTO_SUSPEND", "false") == "true",
		state:        stateActive,
	}

	// Real suspend/resume: if a compute container is configured and the Docker
	// socket is mounted, stop/start the actual container instead of sleeping.
	if c := env("COMPUTE_CONTAINER", ""); c != "" {
		socket := env("DOCKER_SOCKET", "/var/run/docker.sock")
		m.docker = newDockerClient(socket)
		m.computeContainer = c
		if running, err := m.docker.running(c); err != nil {
			log.Printf("WARNING: cannot reach Docker (%v) — falling back to simulated mode", err)
			m.docker = nil
			m.computeContainer = ""
		} else {
			if running {
				m.state = stateActive
			} else {
				m.state = stateSuspended
			}
			log.Printf("real suspend/resume enabled for container %s (running=%v)", c, running)
		}
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
