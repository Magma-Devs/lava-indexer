package provider_rewards

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestExpectedDates_StepsByMonthOn17th(t *testing.T) {
	earliest := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)
	now := time.Date(2025, 4, 20, 16, 0, 0, 0, time.UTC)
	got := ExpectedDates(earliest, now)

	want := []time.Time{
		time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 2, 17, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 17, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 4, 17, 0, 0, 0, 0, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d, want %d (got=%v)", len(got), len(want), got)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("[%d] got %s, want %s", i, got[i], want[i])
		}
	}
}

func TestExpectedDates_ExcludesFutureSlot(t *testing.T) {
	earliest := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)
	// Right at the 17th but before 15:00 UTC — slot is in the future,
	// must be excluded.
	now := time.Date(2025, 3, 17, 14, 59, 59, 0, time.UTC)
	got := ExpectedDates(earliest, now)

	// Expect only Jan and Feb; March hasn't hit 15:00 UTC yet.
	if len(got) != 2 {
		t.Fatalf("len=%d (%v), want 2", len(got), got)
	}
	if got[len(got)-1].Month() != time.February {
		t.Fatalf("last date month = %s, want February", got[len(got)-1].Month())
	}
}

func TestExpectedDates_EarliestIsAlreadyNormalizedTo17th(t *testing.T) {
	// Caller supplies an off-by-one date (e.g. 10th). ExpectedDates
	// normalises to the 17th of that same month so operators can't
	// accidentally shift the cadence.
	earliest := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)
	now := time.Date(2025, 2, 18, 0, 0, 0, 0, time.UTC)
	got := ExpectedDates(earliest, now)
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2", len(got))
	}
	if got[0].Day() != 17 || got[1].Day() != 17 {
		t.Fatalf("dates not normalised to 17th: %v", got)
	}
}

func TestParseSource(t *testing.T) {
	tests := []struct {
		in       string
		wantKind SourceKind
		wantSpec string
	}{
		{"Boost: ETH1", SourceBoost, "ETH1"},
		{"Pools: LAVA", SourcePools, "LAVA"},
		{"Subscription: ETH1", SourceSubscription, "ETH1"},
		{"Boost: some: spec", SourceBoost, "some: spec"}, // only first ': ' splits
		{"NoColon", SourceSubscription, "NoColon"},       // fallback
		{"Unknown: xyz", SourceSubscription, "xyz"},      // unknown label → Subscription
	}
	for _, tc := range tests {
		k, s := parseSource(tc.in)
		if k != tc.wantKind || s != tc.wantSpec {
			t.Errorf("parseSource(%q) = (%d, %q), want (%d, %q)", tc.in, k, s, tc.wantKind, tc.wantSpec)
		}
	}
}

func TestParseEstimatedRewards_Success(t *testing.T) {
	body := []byte(`{
		"info": [
			{
				"source": "Boost: ETH1",
				"amount": [{"denom": "ulava", "amount": "30737293.000000000000000000"}]
			},
			{
				"source": "Subscription: LAVA",
				"amount": [
					{"denom": "ulava", "amount": "123"},
					{"denom": "ibc/ABC", "amount": "456.0"}
				]
			}
		]
	}`)
	entries, err := ParseEstimatedRewards(body)
	if err != nil {
		t.Fatalf("ParseEstimatedRewards: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
	if entries[0].SourceKind != SourceBoost || entries[0].Spec != "ETH1" {
		t.Errorf("entry 0: got kind=%d spec=%s, want Boost/ETH1", entries[0].SourceKind, entries[0].Spec)
	}
	if len(entries[0].Amounts) != 1 || entries[0].Amounts[0].Amount != "30737293" {
		t.Errorf("entry 0 amount: got %v, want [ulava=30737293]", entries[0].Amounts)
	}
	if entries[1].SourceKind != SourceSubscription || entries[1].Spec != "LAVA" {
		t.Errorf("entry 1: got kind=%d spec=%s, want Subscription/LAVA",
			entries[1].SourceKind, entries[1].Spec)
	}
	if len(entries[1].Amounts) != 2 {
		t.Errorf("entry 1 amounts len = %d, want 2", len(entries[1].Amounts))
	}
	if entries[1].Amounts[1].Denom != "ibc/ABC" || entries[1].Amounts[1].Amount != "456" {
		t.Errorf("entry 1 ibc amount: got %v, want 456", entries[1].Amounts[1])
	}
}

func TestParseEstimatedRewards_PrunedReplica(t *testing.T) {
	body := []byte(`{"success": false, "message": "upstream_error: failed to get a response within the deadline after 6 retries, last error: rpc error: code = Unknown desc = post failed: Post \"http://127.0.0.1:26657\": context deadline exceeded, in most cases this is due to the requested state being pruned or version mismatch"}`)
	_, err := ParseEstimatedRewards(body)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errRetryPruned) {
		t.Fatalf("error = %v, want errRetryPruned (wrapped)", err)
	}
}

func TestParseEstimatedRewards_PrunedReplica_VersionDoesNotExist(t *testing.T) {
	body := []byte(`{"success": false, "message": "upstream_error: version does not exist: key 0x..."}`)
	_, err := ParseEstimatedRewards(body)
	if err == nil || !errors.Is(err, errRetryPruned) {
		t.Fatalf("got %v, want errRetryPruned", err)
	}
}

func TestParseEstimatedRewards_NoClaimableRewards(t *testing.T) {
	body := []byte(`{"success": false, "message": "cannot get claimable rewards after distribution"}`)
	_, err := ParseEstimatedRewards(body)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errNoClaimableRewards) {
		t.Fatalf("got %v, want errNoClaimableRewards", err)
	}
}

func TestParseEstimatedRewards_OpaqueSuccessFalse(t *testing.T) {
	body := []byte(`{"success": false, "message": "something else broke"}`)
	_, err := ParseEstimatedRewards(body)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errRetryTransient) {
		t.Fatalf("got %v, want errRetryTransient", err)
	}
}

func TestParseEstimatedRewards_EmptyInfoArray(t *testing.T) {
	// 200 OK with empty info — legitimate "no rewards accrued" response.
	body := []byte(`{"info": []}`)
	entries, err := ParseEstimatedRewards(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries = %d, want 0", len(entries))
	}
}

func TestCleanIntegerString(t *testing.T) {
	tests := []struct {
		in   string
		want string
		ok   bool
	}{
		{"123", "123", true},
		{"-123", "-123", true},
		{"123.0", "123", true},
		{"123.000000000000000000", "123", true},
		{"123.1", "", false},
		{"123.0001", "", false},
		{"abc", "", false},
		{"", "", false},
		{".0", "0", true},
		{"-", "", false},
	}
	for _, tc := range tests {
		got, ok := cleanIntegerString(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("cleanIntegerString(%q) = (%q, %v), want (%q, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// TestClassifyChainMessage exercises the full error mapping directly.
func TestClassifyChainMessage(t *testing.T) {
	tests := []struct {
		msg     string
		want    error
		wantFmt string
	}{
		{"upstream_error: version does not exist", errRetryPruned, "version does not exist"},
		{"version mismatch: 1 vs 2", errRetryPruned, "version mismatch"},
		{"no commit info found for height 5", errRetryPruned, "no commit info"},
		{"cannot get claimable rewards after distribution", errNoClaimableRewards, ""},
		{"some_other_failure", errRetryTransient, "some_other_failure"},
	}
	for _, tc := range tests {
		err := classifyChainMessage(tc.msg)
		if !errors.Is(err, tc.want) {
			t.Errorf("classifyChainMessage(%q) = %v, want wrapping %v", tc.msg, err, tc.want)
		}
		if tc.wantFmt != "" && !strings.Contains(err.Error(), tc.wantFmt) {
			t.Errorf("error text doesn't carry message: %q missing %q", err.Error(), tc.wantFmt)
		}
	}
}
