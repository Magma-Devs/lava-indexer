package rpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRESTClient_Probe_ParsesNetwork spins a fake LCD that serves both the
// blocks/latest and node_info endpoints, and checks that StatusInfo.Network
// surfaces default_node_info.network from the latter.
func TestRESTClient_Probe_ParsesNetwork(t *testing.T) {
	const latest = `{
		"block":{"header":{"height":"9001","time":"2024-01-01T00:00:00Z"}}
	}`
	const nodeInfo = `{
		"default_node_info":{"network":"lava-mainnet-1"}
	}`
	mux := http.NewServeMux()
	mux.HandleFunc("/cosmos/base/tendermint/v1beta1/blocks/latest", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(latest))
	})
	mux.HandleFunc("/cosmos/base/tendermint/v1beta1/node_info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(nodeInfo))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := NewREST(srv.URL, nil)
	t.Cleanup(c.Close)

	info, err := c.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if info.Network != "lava-mainnet-1" {
		t.Fatalf("Network = %q, want %q", info.Network, "lava-mainnet-1")
	}
	if info.LatestHeight != 9001 {
		t.Fatalf("LatestHeight = %d, want 9001", info.LatestHeight)
	}
}

// TestRESTClient_Probe_NodeInfoFailureBubblesUp: if node_info is unreachable
// Probe must fail (not silently return an empty Network) so the caller
// doesn't accidentally treat an infra problem as "endpoint didn't report".
func TestRESTClient_Probe_NodeInfoFailureBubblesUp(t *testing.T) {
	const latest = `{
		"block":{"header":{"height":"42","time":"2024-01-01T00:00:00Z"}}
	}`
	mux := http.NewServeMux()
	mux.HandleFunc("/cosmos/base/tendermint/v1beta1/blocks/latest", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(latest))
	})
	mux.HandleFunc("/cosmos/base/tendermint/v1beta1/node_info", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := NewREST(srv.URL, nil)
	t.Cleanup(c.Close)

	if _, err := c.Probe(context.Background()); err == nil {
		t.Fatal("expected error when node_info is 404, got nil")
	}
}
