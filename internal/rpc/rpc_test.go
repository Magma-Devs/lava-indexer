package rpc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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

