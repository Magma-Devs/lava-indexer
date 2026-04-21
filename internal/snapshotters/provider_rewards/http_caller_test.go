package provider_rewards

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestHTTPCaller_EstimatedRewards_Success verifies the happy path: a
// 200 response with an info[] array is parsed into reward entries.
func TestHTTPCaller_EstimatedRewards_Success(t *testing.T) {
	const body = `{"info":[{"source":"Boost: ETH1","amount":[{"denom":"ulava","amount":"42"}]}]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/lavanet/lava/pairing/estimated_provider_rewards/lava@addr/1ulava",
		func(w http.ResponseWriter, r *http.Request) {
			if h := r.Header.Get("x-cosmos-block-height"); h != "123" {
				t.Errorf("x-cosmos-block-height = %q, want \"123\"", h)
			}
			_, _ = w.Write([]byte(body))
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := NewHTTPCaller(srv.URL, nil)
	entries, err := c.EstimatedRewards(context.Background(), "lava@addr", 123)
	if err != nil {
		t.Fatalf("EstimatedRewards: %v", err)
	}
	if len(entries) != 1 || entries[0].Spec != "ETH1" || entries[0].Amounts[0].Amount != "42" {
		t.Fatalf("parsed entries mismatch: %+v", entries)
	}
}

// TestHTTPCaller_EstimatedRewards_NoClaimable: the application-level
// "no claimable rewards" response is converted into empty entries, NOT
// an error.
func TestHTTPCaller_EstimatedRewards_NoClaimable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/lavanet/lava/pairing/estimated_provider_rewards/lava@addr/1ulava",
		func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"success": false, "message": "cannot get claimable rewards after distribution"}`))
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := NewHTTPCaller(srv.URL, nil)
	entries, err := c.EstimatedRewards(context.Background(), "lava@addr", 1)
	if err != nil {
		t.Fatalf("expected nil error for no-claimable, got %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries len = %d, want 0", len(entries))
	}
}

// TestHTTPCaller_EstimatedRewards_PrunedThenSuccess: a pruned-replica
// response first, then a valid response on retry — exercises the retry
// path without needing to wait for the full 10 attempts.
func TestHTTPCaller_EstimatedRewards_PrunedThenSuccess(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/lavanet/lava/pairing/estimated_provider_rewards/lava@addr/1ulava",
		func(w http.ResponseWriter, r *http.Request) {
			n := calls.Add(1)
			if n == 1 {
				_, _ = w.Write([]byte(`{"success": false, "message": "version mismatch"}`))
				return
			}
			_, _ = w.Write([]byte(`{"info":[]}`))
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := NewHTTPCaller(srv.URL, nil)
	// Use a context with a generous timeout — the backoff on attempt 0
	// is ~250ms with jitter so two calls should easily fit.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	entries, err := c.EstimatedRewards(ctx, "lava@addr", 1)
	if err != nil {
		t.Fatalf("EstimatedRewards: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries len = %d, want 0", len(entries))
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2 (one pruned retry, one success)", got)
	}
}

// TestHTTPCaller_Tip verifies the Tip parsing off the LCD response.
func TestHTTPCaller_Tip(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/cosmos/base/tendermint/v1beta1/blocks/latest",
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"block":{"header":{"height":"12345"}}}`))
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := NewHTTPCaller(srv.URL, nil)

	tip, err := c.Tip(context.Background())
	if err != nil {
		t.Fatalf("Tip: %v", err)
	}
	if tip != 12345 {
		t.Fatalf("tip = %d, want 12345", tip)
	}
}

// TestHTTPCaller_BlockTime verifies BlockTime sets x-cosmos-block-height
// is NOT used (BlockTime uses URL-based path instead) and parses the
// header.time field.
func TestHTTPCaller_BlockTime(t *testing.T) {
	const ts = "2025-06-01T12:34:56Z"
	mux := http.NewServeMux()
	mux.HandleFunc("/cosmos/base/tendermint/v1beta1/blocks/42",
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"block":{"header":{"time":"` + ts + `"}}}`))
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := NewHTTPCaller(srv.URL, nil)

	got, err := c.BlockTime(context.Background(), 42)
	if err != nil {
		t.Fatalf("BlockTime: %v", err)
	}
	want, _ := time.Parse(time.RFC3339, ts)
	if !got.Equal(want) {
		t.Fatalf("BlockTime = %s, want %s", got, want)
	}
}

// TestHTTPCaller_EstimatedRewards_HTTPErrorFailsFast verifies a
// non-2xx HTTP response surfaces as an error without retrying
// (non-2xx isn't a classified "retry" branch).
func TestHTTPCaller_EstimatedRewards_HTTPErrorFailsFast(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/lavanet/lava/pairing/estimated_provider_rewards/lava@addr/1ulava",
		func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			http.Error(w, "nope", http.StatusInternalServerError)
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := NewHTTPCaller(srv.URL, nil)

	_, err := c.EstimatedRewards(context.Background(), "lava@addr", 1)
	if err == nil {
		t.Fatal("expected error on 500, got nil")
	}
	// Single shot — no retry loop on raw HTTP errors.
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (no retry on http 500)", got)
	}
}
