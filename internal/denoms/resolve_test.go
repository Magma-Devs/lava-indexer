package denoms

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestStripMicroPrefix covers the microdenom-stripping rules the
// deriver relies on to turn `ulava` / `uatom` into their pricing
// forms without calling the chain.
func TestStripMicroPrefix(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"ulava", "ulava", "lava"},
		{"uatom", "uatom", "atom"},
		{"uusdc", "uusdc", "usdc"},
		{"already-base", "lava", "lava"},
		{"uppercase-normalised", "ULAVA", "lava"},
		{"leading-whitespace", "  ulava  ", "lava"},
		{"too-short-no-strip", "uu", "uu"},
		{"non-alnum-no-strip", "u-foo", "u-foo"},
		{"empty", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := stripMicroPrefix(tc.in)
			if got != tc.want {
				t.Fatalf("stripMicroPrefix(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestResolver_IBCTraceLookup exercises the happy path of an IBC
// hash resolving against a mock trace endpoint. Also verifies the
// in-process cache — a second lookup of the same hash does NOT make
// another HTTP call.
func TestResolver_IBCTraceLookup(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/ibc/apps/transfer/v1/denom_traces/ABC123",
		func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"denom_trace": map[string]any{
					"path":       "transfer/channel-1",
					"base_denom": "uatom",
				},
			})
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	r := NewResolver(srv.URL, nil, 5*time.Second)
	got, transient, err := r.ResolveBaseDenom(context.Background(), "ibc/ABC123")
	if err != nil || transient {
		t.Fatalf("unexpected err=%v transient=%v", err, transient)
	}
	if got != "atom" {
		t.Fatalf("got %q, want %q", got, "atom")
	}
	// Cache hit — same hash, no new HTTP call.
	_, _, _ = r.ResolveBaseDenom(context.Background(), "ibc/ABC123")
	if n := calls.Load(); n != 1 {
		t.Fatalf("cache miss: calls = %d, want 1", n)
	}
}

// TestResolver_IBCTraceNotFound: a 404 from the chain means the hash
// is genuinely unknown — surface as a permanent (non-transient) error.
func TestResolver_IBCTraceNotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ibc/apps/transfer/v1/denom_traces/UNKNOWN",
		func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "not found", http.StatusNotFound)
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	r := NewResolver(srv.URL, nil, 5*time.Second)
	_, transient, err := r.ResolveBaseDenom(context.Background(), "ibc/UNKNOWN")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if transient {
		t.Fatalf("expected permanent error, got transient=%v err=%v", transient, err)
	}
}

// TestResolver_IBCTraceTransient5xx: 5xx responses are retryable —
// caller should see transient=true so the deriver retries next tick.
func TestResolver_IBCTraceTransient5xx(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ibc/apps/transfer/v1/denom_traces/FLAPPY",
		func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "internal", http.StatusInternalServerError)
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	r := NewResolver(srv.URL, nil, 5*time.Second)
	_, transient, err := r.ResolveBaseDenom(context.Background(), "ibc/FLAPPY")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !transient {
		t.Fatalf("expected transient=true on 5xx, got %v", transient)
	}
}

// TestResolver_PlainMicrodenomNoNetwork: a `ulava` resolves without
// hitting the mock at all — purely in-process.
func TestResolver_PlainMicrodenomNoNetwork(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/ibc/apps/transfer/v1/denom_traces/",
		func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			_, _ = w.Write([]byte(`{}`))
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	r := NewResolver(srv.URL, nil, 5*time.Second)
	got, transient, err := r.ResolveBaseDenom(context.Background(), "ulava")
	if err != nil || transient {
		t.Fatalf("unexpected err=%v transient=%v", err, transient)
	}
	if got != "lava" {
		t.Fatalf("got %q, want %q", got, "lava")
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("microdenom path should not hit chain; calls=%d", n)
	}
}
