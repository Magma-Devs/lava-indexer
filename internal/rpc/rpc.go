package rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// NewRPC builds a Tendermint/CometBFT JSON-RPC 2.0 client. A single instance
// reuses one http.Client (keep-alive) and is safe for concurrent use.
// `headers` are attached to every outbound request (e.g. `lava-extension:
// archive` for Lava gateways). Safe to pass nil.
func NewRPC(baseURL string, headers map[string]string) *RPCClient {
	transport := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     90 * time.Second,
	}
	return &RPCClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		headers: headers,
		http: &http.Client{
			Transport: transport,
			Timeout:   60 * time.Second,
		},
	}
}

type RPCClient struct {
	baseURL string
	headers map[string]string
	http    *http.Client
}

func (c *RPCClient) Close() { c.http.CloseIdleConnections() }

// ---------------------------------------------------------------------------
// JSON-RPC 2.0 plumbing
// ---------------------------------------------------------------------------

type rpcReq struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      int               `json:"id"`
	Method  string            `json:"method"`
	Params  map[string]string `json:"params"`
}

type rpcResp struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    string `json:"data"`
	} `json:"error,omitempty"`
}

// call does a POST with an arbitrary JSON body (single request or batch array)
// and returns the raw response body.
//
// Retry semantics:
//   - HTTP 429 and 5xx: retried up to `maxRetries` times with
//     exponential backoff + jitter. Retry-After (seconds or HTTP-date) is
//     honoured when the server provides it.
//   - Network errors (connection refused, reset, timeout awaiting headers):
//     retried the same way.
//   - 4xx other than 429: treated as terminal — the body is malformed or
//     we're being told the request is wrong, retrying won't fix that.
//   - Context cancellation always wins immediately.
func (c *RPCClient) call(ctx context.Context, body []byte) ([]byte, error) {
	return retryableCall(ctx, c.http, c.baseURL+"/", body, c.headers)
}

const (
	maxRetries = 4
	baseDelay  = 200 * time.Millisecond
	maxDelay   = 5 * time.Second
)

// ErrRateLimited is returned when an endpoint 429's us. The MultiClient
// recognises it and fails over to another endpoint immediately instead of
// burning the retry budget on a node that is actively telling us to slow
// down. If all endpoints return ErrRateLimited, the outer caller's
// retry-with-backoff catches it.
type ErrRateLimited struct {
	RetryAfter time.Duration
	Status     int
	Body       string
}

func (e *ErrRateLimited) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("http %d rate-limited; retry-after %s", e.Status, e.RetryAfter)
	}
	return fmt.Sprintf("http %d rate-limited", e.Status)
}

// retryableCall is the shared HTTP-retry loop for both the RPC and REST
// clients. Returns the raw response body on 2xx. Retries 5xx/network errors
// with exponential backoff + jitter. 429 bails immediately with
// ErrRateLimited so the caller can fail over to another endpoint.
func retryableCall(ctx context.Context, httpc *http.Client, url string, body []byte, extraHeaders map[string]string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		// User headers first, then our protocol header — content-type is ours
		// to set so the request can't be misformatted from config.
		for k, v := range extraHeaders {
			req.Header.Set(k, v)
		}
		if body != nil {
			req.Header.Set("content-type", "application/json")
		}
		resp, err := httpc.Do(req)
		if err != nil {
			lastErr = err
			if !isRetryableNetErr(err) {
				return nil, err
			}
			if !sleepBackoff(ctx, attempt, 0) {
				return nil, ctx.Err()
			}
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			if !sleepBackoff(ctx, attempt, 0) {
				return nil, ctx.Err()
			}
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			// Don't retry the same node. Fail fast and let MultiClient
			// route around it.
			return nil, &ErrRateLimited{
				RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
				Status:     resp.StatusCode,
				Body:       truncate(string(data), 200),
			}
		}
		if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			lastErr = fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(data), 200))
			retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
			if !sleepBackoff(ctx, attempt, retryAfter) {
				return nil, ctx.Err()
			}
			continue
		}
		// 4xx other than 429 — don't retry, the request is malformed and
		// hitting again won't help. Wrap as a typed error so the pipeline
		// classifier can distinguish permanent failures (404 = pruned)
		// from transient ones without string-matching error text.
		return nil, &HTTPStatusError{
			Status: resp.StatusCode,
			Body:   truncate(string(data), 300),
			URL:    url,
		}
	}
	return nil, fmt.Errorf("exceeded %d retries: %w", maxRetries, lastErr)
}

// sleepBackoff waits for the next attempt's delay. If retryAfter > 0 it wins
// over the exponential schedule. Returns false if ctx was cancelled.
func sleepBackoff(ctx context.Context, attempt int, retryAfter time.Duration) bool {
	delay := retryAfter
	if delay == 0 {
		delay = baseDelay << attempt
		if delay > maxDelay {
			delay = maxDelay
		}
		// ±25% jitter to desynchronise contending retriers against a
		// recovering node.
		jitter := time.Duration((float64(delay) * 0.25) * (rand.Float64()*2 - 1))
		delay += jitter
		if delay < 0 {
			delay = baseDelay
		}
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(delay):
		return true
	}
}

// parseRetryAfter understands both numeric seconds and an HTTP-date value.
// Returns 0 if the header is missing or unparseable.
func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if when, err := http.ParseTime(h); err == nil {
		d := time.Until(when)
		if d > 0 {
			return d
		}
	}
	return 0
}

func isRetryableNetErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	// Anything that smells like a connection or timeout issue: retry.
	return strings.Contains(s, "connection reset") ||
		strings.Contains(s, "connection refused") ||
		strings.Contains(s, "context deadline exceeded") ||
		strings.Contains(s, "EOF") ||
		strings.Contains(s, "timeout") ||
		strings.Contains(s, "tls: ") ||
		strings.Contains(s, "no such host") ||
		strings.Contains(s, "i/o timeout") ||
		strings.Contains(s, "broken pipe")
}

// ---------------------------------------------------------------------------
// /status — latest height
// ---------------------------------------------------------------------------

func (c *RPCClient) Tip(ctx context.Context) (int64, error) {
	info, err := c.Probe(ctx)
	return info.LatestHeight, err
}

// Probe returns this endpoint's (earliest, latest) heights and times.
// Pruning nodes set earliest to the oldest block they still serve; archive
// nodes return 1 (or close to it).
func (c *RPCClient) Probe(ctx context.Context) (StatusInfo, error) {
	body, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "status", Params: map[string]string{}})
	raw, err := c.call(ctx, body)
	if err != nil {
		return StatusInfo{}, err
	}
	var resp rpcResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return StatusInfo{}, fmt.Errorf("status decode: %w", err)
	}
	if resp.Error != nil {
		return StatusInfo{}, fmt.Errorf("status rpc error: %s", resp.Error.Message)
	}
	var s struct {
		NodeInfo struct {
			Network string `json:"network"`
		} `json:"node_info"`
		SyncInfo struct {
			LatestBlockHeight   string    `json:"latest_block_height"`
			LatestBlockTime     time.Time `json:"latest_block_time"`
			EarliestBlockHeight string    `json:"earliest_block_height"`
			EarliestBlockTime   time.Time `json:"earliest_block_time"`
		} `json:"sync_info"`
	}
	if err := json.Unmarshal(resp.Result, &s); err != nil {
		return StatusInfo{}, err
	}
	var info StatusInfo
	info.LatestHeight, _ = strconv.ParseInt(s.SyncInfo.LatestBlockHeight, 10, 64)
	info.EarliestHeight, _ = strconv.ParseInt(s.SyncInfo.EarliestBlockHeight, 10, 64)
	info.LatestTime = s.SyncInfo.LatestBlockTime
	info.EarliestTime = s.SyncInfo.EarliestBlockTime
	info.Network = s.NodeInfo.Network
	return info, nil
}

// ---------------------------------------------------------------------------
// Batched block + block_results
// ---------------------------------------------------------------------------

// FetchBlocks batches {block, block_results} calls for every requested height
// into a single HTTP POST.
func (c *RPCClient) FetchBlocks(ctx context.Context, heights []int64) ([]*Block, error) {
	if len(heights) == 0 {
		return nil, nil
	}

	// Build batch: id = 2*idx for block, 2*idx+1 for block_results.
	batch := make([]rpcReq, 0, len(heights)*2)
	for i, h := range heights {
		hStr := strconv.FormatInt(h, 10)
		batch = append(batch,
			rpcReq{JSONRPC: "2.0", ID: 2 * i, Method: "block", Params: map[string]string{"height": hStr}},
			rpcReq{JSONRPC: "2.0", ID: 2*i + 1, Method: "block_results", Params: map[string]string{"height": hStr}},
		)
	}
	body, _ := json.Marshal(batch)
	raw, err := c.call(ctx, body)
	if err != nil {
		return nil, err
	}

	var responses []rpcResp
	if err := json.Unmarshal(raw, &responses); err != nil {
		// Some servers return an error object when any item in a batch fails.
		// Try to decode as single error response for a clearer message.
		var single rpcResp
		if jerr := json.Unmarshal(raw, &single); jerr == nil && single.Error != nil {
			return nil, fmt.Errorf("batch rpc error: %s", single.Error.Message)
		}
		return nil, fmt.Errorf("batch decode: %w; body=%s", err, truncate(string(raw), 200))
	}

	byID := make(map[int]rpcResp, len(responses))
	for _, r := range responses {
		byID[r.ID] = r
	}

	out := make([]*Block, len(heights))
	for i, h := range heights {
		blockResp := byID[2*i]
		resultsResp := byID[2*i+1]
		if blockResp.Error != nil {
			return nil, fmt.Errorf("block h=%d: %s", h, blockResp.Error.Message)
		}
		if resultsResp.Error != nil {
			return nil, fmt.Errorf("block_results h=%d: %s", h, resultsResp.Error.Message)
		}
		blk, err := decodeBlock(h, blockResp.Result, resultsResp.Result)
		if err != nil {
			return nil, fmt.Errorf("decode h=%d: %w", h, err)
		}
		out[i] = blk
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Block decoding — a minimal, tolerant shape over CometBFT/Tendermint
// ---------------------------------------------------------------------------

type rawBlock struct {
	Block struct {
		Header struct {
			Height string    `json:"height"`
			Time   time.Time `json:"time"`
		} `json:"header"`
		Data struct {
			Txs []string `json:"txs"` // base64-encoded raw txs
		} `json:"data"`
	} `json:"block"`
}

type rawAttr struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type rawEvent struct {
	Type       string    `json:"type"`
	Attributes []rawAttr `json:"attributes"`
}

type rawTxResult struct {
	Events []rawEvent `json:"events"`
}

type rawBlockResults struct {
	TxsResults          []rawTxResult `json:"txs_results"`
	BeginBlockEvents    []rawEvent    `json:"begin_block_events"`
	EndBlockEvents      []rawEvent    `json:"end_block_events"`
	FinalizeBlockEvents []rawEvent    `json:"finalize_block_events"` // CometBFT 0.38+
}

func decodeBlock(height int64, blockRaw, resultsRaw json.RawMessage) (*Block, error) {
	var rb rawBlock
	if err := json.Unmarshal(blockRaw, &rb); err != nil {
		return nil, fmt.Errorf("block: %w", err)
	}
	var rr rawBlockResults
	if err := json.Unmarshal(resultsRaw, &rr); err != nil {
		return nil, fmt.Errorf("block_results: %w", err)
	}

	// Compute tx hashes from the block's raw tx bytes. Each hash pairs with
	// txs_results[i] by index.
	hashes := make([]string, len(rb.Block.Data.Txs))
	for i, b64 := range rb.Block.Data.Txs {
		rawBytes, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("txs[%d] b64: %w", i, err)
		}
		sum := sha256.Sum256(rawBytes)
		hashes[i] = strings.ToUpper(hex.EncodeToString(sum[:]))
	}

	events := make([]Event, 0, 32)
	idx := 0

	// Order within a block: begin_block → per-tx → end_block → finalize.
	// We don't care about order for our use case — we just want every event
	// with a stable block-scoped index. CometBFT 0.37+ (which Lava runs)
	// returns attributes as plain UTF-8 strings, so no decoding heuristic is
	// needed; we just take them as-is.
	appendEvents := func(src []rawEvent, txHash string) {
		for _, e := range src {
			ev := Event{
				TxHash:   txHash,
				Type:     e.Type,
				Attrs:    make(map[string]string, len(e.Attributes)),
				EventIdx: idx,
			}
			idx++
			for _, a := range e.Attributes {
				ev.Attrs[a.Key] = a.Value
			}
			events = append(events, ev)
		}
	}

	appendEvents(rr.BeginBlockEvents, "")
	for i, tx := range rr.TxsResults {
		h := ""
		if i < len(hashes) {
			h = hashes[i]
		}
		appendEvents(tx.Events, h)
	}
	appendEvents(rr.EndBlockEvents, "")
	appendEvents(rr.FinalizeBlockEvents, "")

	return &Block{
		Height: height,
		Time:   rb.Block.Header.Time,
		Events: events,
	}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
