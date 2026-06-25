// Minimal Docker Engine API client over the unix socket — just enough to
// start/stop/inspect the compute container, with no third-party dependencies.
// This is what makes the suspend/resume *real*: an actual container is stopped
// (freeing its CPU/RAM) and started (a measurable cold start), rather than a
// sleep timer.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

type dockerClient struct {
	http *http.Client
}

func newDockerClient(socket string) *dockerClient {
	return &dockerClient{
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", socket)
				},
			},
			Timeout: 30 * time.Second,
		},
	}
}

func (d *dockerClient) do(method, path string) (int, []byte, error) {
	req, err := http.NewRequest(method, "http://docker"+path, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body, nil
}

// running reports whether the container is currently running.
func (d *dockerClient) running(container string) (bool, error) {
	code, body, err := d.do(http.MethodGet, "/containers/"+container+"/json")
	if err != nil {
		return false, err
	}
	if code != 200 {
		return false, fmt.Errorf("inspect %s: HTTP %d: %s", container, code, string(body))
	}
	var out struct {
		State struct {
			Running bool `json:"Running"`
		} `json:"State"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return false, err
	}
	return out.State.Running, nil
}

// start starts the container (no-op if already running). Returns nil on success
// or a 304 (already started).
func (d *dockerClient) start(container string) error {
	code, body, err := d.do(http.MethodPost, "/containers/"+container+"/start")
	if err != nil {
		return err
	}
	if code != 204 && code != 304 {
		return fmt.Errorf("start %s: HTTP %d: %s", container, code, string(body))
	}
	return nil
}

// stop stops the container with a short grace period (no-op if already stopped).
func (d *dockerClient) stop(container string) error {
	code, body, err := d.do(http.MethodPost, "/containers/"+container+"/stop?t=3")
	if err != nil {
		return err
	}
	if code != 204 && code != 304 {
		return fmt.Errorf("stop %s: HTTP %d: %s", container, code, string(body))
	}
	return nil
}

// waitReady blocks until a TCP connection to addr succeeds or the deadline
// passes, returning how long it took. This measures the real cold start.
func waitReady(addr string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			c.Close()
			return time.Since(start), nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return time.Since(start), fmt.Errorf("timed out waiting for %s", addr)
}
