package rpc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestRPCClient_Probe_ParsesNetwork asserts that /status's node_info.network
// surfaces as StatusInfo.Network — the field MultiClient compares against
// the configured chain_id on startup.
func TestRPCClient_Probe_ParsesNetwork(t *testing.T) {
	const body = `{
		"jsonrpc":"2.0","id":1,
		"result":{
			"node_info":{"network":"lava-mainnet-1"},
			"sync_info":{
				"latest_block_height":"12345",
				"latest_block_time":"2024-01-01T00:00:00Z",
				"earliest_block_height":"1",
				"earliest_block_time":"2023-01-01T00:00:00Z"
			}
		}
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain so keep-alive works; body content doesn't matter for /status.
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)

	c := NewRPC(srv.URL, nil)
	t.Cleanup(c.Close)

	info, err := c.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if info.Network != "lava-mainnet-1" {
		t.Fatalf("Network = %q, want %q", info.Network, "lava-mainnet-1")
	}
	if info.LatestHeight != 12345 {
		t.Fatalf("LatestHeight = %d, want 12345", info.LatestHeight)
	}
	if info.EarliestHeight != 1 {
		t.Fatalf("EarliestHeight = %d, want 1", info.EarliestHeight)
	}
}

// TestRPCClient_call_5xxFailsFastNoRetry asserts the 5xx fast-fail
// contract: a server error returns *ErrServerError on the FIRST attempt
// with no in-place retry. The previous behaviour was up to 4 in-place
// retries against a sick node, which delayed failover and amplified
// load. MultiClient handles failover at the layer above.
func TestRPCClient_call_5xxFailsFastNoRetry(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := NewRPC(srv.URL, nil)
	t.Cleanup(c.Close)

	_, err := c.Probe(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	var se *ErrServerError
	if !errors.As(err, &se) {
		t.Fatalf("err type = %T (%v), want *ErrServerError", err, err)
	}
	if se.Status != http.StatusInternalServerError {
		t.Fatalf("Status = %d, want 500", se.Status)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("hits = %d, want 1 (5xx must NOT trigger same-node retries)", got)
	}
}

// TestRPCClient_call_NetworkErrRetriesOnce asserts that a single
// connection-level failure gets one in-place retry — the cheap recovery
// for the keepalive-closed race — and the second attempt succeeds. No
// further retries beyond the network budget.
func TestRPCClient_call_NetworkErrRetriesOnce(t *testing.T) {
	var hits atomic.Int32
	const okBody = `{"jsonrpc":"2.0","id":1,"result":{"sync_info":{"latest_block_height":"42","latest_block_time":"2024-01-01T00:00:00Z","earliest_block_height":"1","earliest_block_time":"2023-01-01T00:00:00Z"}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		if n == 1 {
			// Hijack and close to simulate a torn connection — Go's
			// http.Client surfaces this as an EOF, which isRetryableNetErr
			// classifies as transient.
			hj, _ := w.(http.Hijacker)
			conn, _, _ := hj.Hijack()
			_ = conn.Close()
			return
		}
		_, _ = io.WriteString(w, okBody)
	}))
	t.Cleanup(srv.Close)

	c := NewRPC(srv.URL, nil)
	t.Cleanup(c.Close)

	info, err := c.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if info.LatestHeight != 42 {
		t.Fatalf("LatestHeight = %d, want 42", info.LatestHeight)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("hits = %d, want 2 (one fail + one retry)", got)
	}
}

// TestRPCClient_Probe_MissingNetwork covers older/non-compliant nodes that
// don't include node_info.network — Probe must still succeed, leaving
// Network empty so the caller can treat it as "did not report".
func TestRPCClient_Probe_MissingNetwork(t *testing.T) {
	const body = `{
		"jsonrpc":"2.0","id":1,
		"result":{
			"sync_info":{
				"latest_block_height":"7",
				"latest_block_time":"2024-01-01T00:00:00Z",
				"earliest_block_height":"1",
				"earliest_block_time":"2023-01-01T00:00:00Z"
			}
		}
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)

	c := NewRPC(srv.URL, nil)
	t.Cleanup(c.Close)

	info, err := c.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if info.Network != "" {
		t.Fatalf("Network = %q, want empty", info.Network)
	}
	if info.LatestHeight != 7 {
		t.Fatalf("LatestHeight = %d, want 7", info.LatestHeight)
	}
}

