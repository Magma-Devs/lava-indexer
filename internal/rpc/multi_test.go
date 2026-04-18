package rpc

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// stubClient is a fake Client+Prober for testing MultiClient.Probe. Only the
// Probe bits matter here — Tip/FetchBlocks panic if accidentally called so
// tests that need them must wire them explicitly.
type stubClient struct {
	info StatusInfo
	err  error
}

func (s *stubClient) Probe(ctx context.Context) (StatusInfo, error) { return s.info, s.err }
func (s *stubClient) Tip(ctx context.Context) (int64, error)        { panic("unused") }
func (s *stubClient) FetchBlocks(ctx context.Context, heights []int64) ([]*Block, error) {
	panic("unused")
}
func (s *stubClient) Close() {}

func newStubEndpoint(url, network string) *Endpoint {
	return &Endpoint{
		URL:    url,
		Kind:   "rpc",
		Client: &stubClient{info: StatusInfo{EarliestHeight: 1, LatestHeight: 100, Network: network}},
	}
}

// newFailingStubEndpoint returns an endpoint whose Probe always errors — used
// to exercise the probe-failure branch that now also populates Reason.
func newFailingStubEndpoint(url string, probeErr error) *Endpoint {
	return &Endpoint{
		URL:    url,
		Kind:   "rpc",
		Client: &stubClient{err: probeErr},
	}
}

// findEndpoint is a tiny helper — the endpoint list is the source of truth
// for Disabled/Reason state so tests re-look up by URL instead of leaning on
// slice ordering.
func findEndpoint(t *testing.T, eps []*Endpoint, url string) *Endpoint {
	t.Helper()
	for _, ep := range eps {
		if ep.URL == url {
			return ep
		}
	}
	t.Fatalf("endpoint %q not found", url)
	return nil
}

// TestMultiClient_Probe_ChainIDMatch covers the happy path: every endpoint
// reports the same chain_id the operator configured, Probe returns nil and
// nothing is flagged.
func TestMultiClient_Probe_ChainIDMatch(t *testing.T) {
	eps := []*Endpoint{
		newStubEndpoint("http://a", "lava-mainnet-1"),
		newStubEndpoint("http://b", "lava-mainnet-1"),
		newStubEndpoint("http://c", "lava-mainnet-1"),
	}
	m := NewMulti(eps)
	if err := m.Probe(context.Background(), "lava-mainnet-1"); err != nil {
		t.Fatalf("Probe: unexpected error: %v", err)
	}
	for _, ep := range eps {
		if ep.Disabled {
			t.Fatalf("%s: unexpectedly Disabled", ep.URL)
		}
		if ep.Reason != "" {
			t.Fatalf("%s: unexpected Reason %q on healthy endpoint", ep.URL, ep.Reason)
		}
	}
}

// TestMultiClient_Probe_ChainIDMismatchDisables: three endpoints, two match
// expected chain, one advertises something else. Probe must return nil —
// the mismatched endpoint is DISABLED at runtime (not fail-fast any more).
// The mismatched endpoint must carry a Reason that names both expected and
// advertised chain_ids so the dashboard can explain why it's out of rotation.
// The two good endpoints must be untouched.
func TestMultiClient_Probe_ChainIDMismatchDisables(t *testing.T) {
	eps := []*Endpoint{
		newStubEndpoint("http://a", "lava-mainnet-1"),
		newStubEndpoint("http://b", "lava-testnet-2"),
		newStubEndpoint("http://c", "lava-mainnet-1"),
	}
	m := NewMulti(eps)
	if err := m.Probe(context.Background(), "lava-mainnet-1"); err != nil {
		t.Fatalf("Probe: unexpected error: %v", err)
	}

	bad := findEndpoint(t, eps, "http://b")
	if !bad.Disabled {
		t.Fatalf("%s: expected Disabled=true on mismatch", bad.URL)
	}
	for _, want := range []string{"lava-mainnet-1", "lava-testnet-2"} {
		if !strings.Contains(bad.Reason, want) {
			t.Fatalf("%s: Reason %q should mention %q", bad.URL, bad.Reason, want)
		}
	}

	for _, url := range []string{"http://a", "http://c"} {
		ep := findEndpoint(t, eps, url)
		if ep.Disabled {
			t.Fatalf("%s: matching endpoint unexpectedly Disabled", url)
		}
		if ep.Reason != "" {
			t.Fatalf("%s: matching endpoint has Reason %q", url, ep.Reason)
		}
	}
}

// TestMultiClient_Probe_AllMismatchFailsFast: with validation active, when
// EVERY endpoint mismatches there's nothing left to index from — Probe must
// still return the existing "no healthy endpoints" error so startup aborts
// before we silently write nothing into the DB for hours.
func TestMultiClient_Probe_AllMismatchFailsFast(t *testing.T) {
	eps := []*Endpoint{
		newStubEndpoint("http://a", "lava-testnet-2"),
		newStubEndpoint("http://b", "lava-testnet-2"),
		newStubEndpoint("http://c", "lava-testnet-2"),
	}
	m := NewMulti(eps)
	err := m.Probe(context.Background(), "lava-mainnet-1")
	if err == nil {
		t.Fatal("Probe: expected error when every endpoint mismatches, got nil")
	}
	if !strings.Contains(err.Error(), "no healthy endpoints") {
		t.Fatalf("error should be the 'no healthy endpoints' fail-fast; got: %v", err)
	}
	for _, ep := range eps {
		if !ep.Disabled {
			t.Fatalf("%s: expected Disabled=true", ep.URL)
		}
	}
}

// TestMultiClient_Probe_EndpointDidNotReport: with an expected chain_id, an
// endpoint that returns an empty Network is treated the same as a mismatch —
// we can't verify it's the right chain. Disabled=true and the Reason must
// include the "did not report" phrase so operators can tell this apart from
// a genuine mismatch when reading the dashboard.
func TestMultiClient_Probe_EndpointDidNotReport(t *testing.T) {
	eps := []*Endpoint{
		newStubEndpoint("http://a", "lava-mainnet-1"),
		newStubEndpoint("http://b", ""), // didn't report
	}
	m := NewMulti(eps)
	if err := m.Probe(context.Background(), "lava-mainnet-1"); err != nil {
		t.Fatalf("Probe: unexpected error: %v", err)
	}
	bad := findEndpoint(t, eps, "http://b")
	if !bad.Disabled {
		t.Fatalf("%s: expected Disabled=true on empty Network", bad.URL)
	}
	if !strings.Contains(bad.Reason, "did not report") {
		t.Fatalf("%s: Reason %q should mention 'did not report'", bad.URL, bad.Reason)
	}
}

// TestMultiClient_Probe_EmptyExpectedSkips: when the operator leaves
// network.chain_id empty (backward compat), validation is skipped entirely —
// even an obviously-wrong advertised network must not disable the endpoint.
func TestMultiClient_Probe_EmptyExpectedSkips(t *testing.T) {
	eps := []*Endpoint{
		newStubEndpoint("http://a", "lava-mainnet-1"),
		newStubEndpoint("http://b", "some-other-chain"),
		newStubEndpoint("http://c", ""),
	}
	m := NewMulti(eps)
	if err := m.Probe(context.Background(), ""); err != nil {
		t.Fatalf("Probe: expected nil when chain_id empty, got: %v", err)
	}
	for _, ep := range eps {
		if ep.Disabled {
			t.Fatalf("%s: should not be Disabled when validation is skipped", ep.URL)
		}
	}
}

// TestMultiClient_Probe_FailureSetsReason: probe failures (HTTP / network)
// were the only way an endpoint got disabled before chain_id validation
// existed; now they must also populate Reason so the dashboard can show the
// underlying error rather than a bare red "✗".
func TestMultiClient_Probe_FailureSetsReason(t *testing.T) {
	eps := []*Endpoint{
		newStubEndpoint("http://a", "lava-mainnet-1"),
		newFailingStubEndpoint("http://b", errors.New("dial tcp: connection refused")),
	}
	m := NewMulti(eps)
	if err := m.Probe(context.Background(), "lava-mainnet-1"); err != nil {
		t.Fatalf("Probe: unexpected error: %v", err)
	}
	bad := findEndpoint(t, eps, "http://b")
	if !bad.Disabled {
		t.Fatalf("%s: expected Disabled=true after probe failure", bad.URL)
	}
	if !strings.Contains(bad.Reason, "probe failed") {
		t.Fatalf("%s: Reason %q should start with 'probe failed'", bad.URL, bad.Reason)
	}
	if !strings.Contains(bad.Reason, "connection refused") {
		t.Fatalf("%s: Reason %q should include underlying error", bad.URL, bad.Reason)
	}
}
