// Package denoms derives a dictionary of pricing-ready base denoms
// from app.provider_rewards. Raw on-chain denoms (ulava, ibc/<hash>,
// etc.) are resolved to their base form (lava, atom, …) and upserted
// into app.priced_denoms. The table is the surface an eventual
// CoinGecko price snapshotter reads from — it doesn't need to know
// about IBC traces or microdenom prefixes.
package denoms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/magma-devs/lava-indexer/internal/rpc"
)

// maxTraceBodyBytes bounds the response body we read from an IBC
// denom_traces query. Traces are short; 256 KiB is more than enough
// headroom while still stopping a compromised upstream from OOMing
// the process.
const maxTraceBodyBytes = 256 << 10

// Resolver turns a raw on-chain denom into its base form. Two kinds
// of work happen here:
//
//   - microdenom strip ("ulava" → "lava", "uatom" → "atom"). Purely
//     in-process, no network.
//   - IBC trace lookup ("ibc/<hash>" → trace.base_denom). Calls the
//     chain's REST /ibc/apps/transfer/v1/denom_traces/{hash}. The
//     result is cached in-process forever — an IBC trace is immutable
//     once issued.
//
// The caller can be nil; in that case IBC denoms surface as a
// transient error (next tick retries) since there's nothing to look
// up against.
type Resolver struct {
	restURL string
	headers map[string]string
	timeout time.Duration
	http    *http.Client

	mu    sync.RWMutex
	cache map[string]string // ibc-hash → base_denom
}

// NewResolver returns a Resolver pointed at restURL. Headers are
// attached to every trace lookup — typically the same
// `lava-extension: archive` that the snapshotter uses.
//
// When restURL is empty the resolver still handles the microdenom
// strip path; any ibc/... denom returns a transient error so the
// deriver skips + retries on its next tick.
func NewResolver(restURL string, headers map[string]string, timeout time.Duration) *Resolver {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Resolver{
		restURL: strings.TrimRight(restURL, "/"),
		headers: headers,
		timeout: timeout,
		http: &http.Client{
			Transport: rpc.SharedTransport,
			Timeout:   timeout,
		},
		cache: make(map[string]string, 64),
	}
}

// ResolveBaseDenom converts a raw on-chain denom to a pricing base
// denom. The contract:
//
//   - ("", false, nil)      raw is empty / obviously unroutable
//   - (base, false, nil)    success — base is the pricing-ready denom
//   - ("", true,  err)      transient — chain RPC unreachable; caller
//                           should skip + retry on the next tick
//   - ("", false, err)      persistent — trace 404 or similar; denom
//                           is genuinely unroutable
func (r *Resolver) ResolveBaseDenom(ctx context.Context, raw string) (string, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false, nil
	}
	low := strings.ToLower(raw)

	// IBC denom — look up the trace and run the result through the
	// microdenom strip (trace.base_denom is often already a microdenom
	// like `uatom`).
	if strings.HasPrefix(low, "ibc/") {
		hash := raw[len("ibc/"):]
		if hash == "" {
			return "", false, errors.New("ibc/ prefix with empty hash")
		}
		base, transient, err := r.resolveIBC(ctx, hash)
		if err != nil || base == "" {
			return "", transient, err
		}
		return stripMicroPrefix(base), false, nil
	}

	// Plain microdenom or a bare token name.
	return stripMicroPrefix(low), false, nil
}

// stripMicroPrefix returns the pricing denom for a non-IBC raw denom.
// Lowercases input, strips a leading `u` when the result is still a
// recognisable token name (at least 3 chars, all alphanumeric). The
// 3-char floor keeps us from turning `uusdc` → `usdc` (correct) but
// also keeps bare 2-letter tokens like `uu` alone — never observed in
// practice, just defensive.
func stripMicroPrefix(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) >= 4 && s[0] == 'u' && isAsciiAlnum(s[1:]) {
		return s[1:]
	}
	return s
}

func isAsciiAlnum(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		default:
			return false
		}
	}
	return len(s) > 0
}

// resolveIBC hits /ibc/apps/transfer/v1/denom_traces/{hash}. Returns
// the raw `base_denom` from the trace response (still in microdenom
// form) or an error with transient flag set appropriately.
func (r *Resolver) resolveIBC(ctx context.Context, hash string) (string, bool, error) {
	// Hot-path cache lookup — a resolved IBC trace never changes.
	r.mu.RLock()
	if b, ok := r.cache[hash]; ok {
		r.mu.RUnlock()
		return b, false, nil
	}
	r.mu.RUnlock()

	if r.restURL == "" {
		// No REST client configured; caller decides whether to treat
		// as transient (so next tick with a future config retries) or
		// permanent. We flag transient since the config may still be
		// coming.
		return "", true, errors.New("ibc resolution requested but no rest_url configured")
	}

	path := r.restURL + "/ibc/apps/transfer/v1/denom_traces/" + hash
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", false, err
	}
	for k, v := range r.headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("accept", "application/json")
	resp, err := r.http.Do(req)
	if err != nil {
		// Connection-level problem — transient.
		return "", true, fmt.Errorf("ibc trace %s: %w", hash, err)
	}
	defer resp.Body.Close()
	body, rerr := io.ReadAll(io.LimitReader(resp.Body, maxTraceBodyBytes))
	if rerr != nil {
		return "", true, fmt.Errorf("ibc trace %s body: %w", hash, rerr)
	}
	if resp.StatusCode == http.StatusNotFound {
		// Permanent — the hash isn't known to the chain.
		return "", false, fmt.Errorf("ibc trace %s: http 404", hash)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", true, fmt.Errorf("ibc trace %s: http %d", hash, resp.StatusCode)
	}

	var parsed struct {
		DenomTrace struct {
			BaseDenom string `json:"base_denom"`
		} `json:"denom_trace"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", true, fmt.Errorf("ibc trace %s decode: %w", hash, err)
	}
	base := strings.TrimSpace(parsed.DenomTrace.BaseDenom)
	if base == "" {
		return "", false, fmt.Errorf("ibc trace %s: empty base_denom", hash)
	}

	r.mu.Lock()
	r.cache[hash] = base
	r.mu.Unlock()
	return base, false, nil
}
