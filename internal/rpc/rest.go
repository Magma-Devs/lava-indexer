package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// NewREST builds a Cosmos LCD (REST) client. This only covers events attached
// to transactions, NOT begin/end/finalize_block events — the LCD surface
// doesn't expose those uniformly. For lava_relay_payment this is fine because
// the event is emitted from MsgRelayPayment transactions.
//
// If you add handlers that need block-scope events later, switch the endpoint
// kind back to "rpc".
func NewREST(baseURL string, headers map[string]string) *RESTClient {
	transport := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
	}
	return &RESTClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		headers: headers,
		http: &http.Client{
			Transport: transport,
			Timeout:   60 * time.Second,
		},
	}
}

type RESTClient struct {
	baseURL string
	headers map[string]string
	http    *http.Client
}

func (c *RESTClient) Close() { c.http.CloseIdleConnections() }

// Tip reads the latest block height from LCD.
func (c *RESTClient) Tip(ctx context.Context) (int64, error) {
	var r struct {
		Block struct {
			Header struct {
				Height string `json:"height"`
			} `json:"header"`
		} `json:"block"`
	}
	if err := c.getJSON(ctx, "/cosmos/base/tendermint/v1beta1/blocks/latest", &r); err != nil {
		return 0, err
	}
	return strconv.ParseInt(r.Block.Header.Height, 10, 64)
}

// Probe reports latest height and time. LCD doesn't expose an explicit
// earliest_height so we return 1 — attempts to fetch below the node's
// pruning horizon will fail at FetchBlocks time, which the multi-client
// will surface.
func (c *RESTClient) Probe(ctx context.Context) (StatusInfo, error) {
	var r struct {
		Block struct {
			Header struct {
				Height string    `json:"height"`
				Time   time.Time `json:"time"`
			} `json:"header"`
		} `json:"block"`
	}
	if err := c.getJSON(ctx, "/cosmos/base/tendermint/v1beta1/blocks/latest", &r); err != nil {
		return StatusInfo{}, err
	}
	h, _ := strconv.ParseInt(r.Block.Header.Height, 10, 64)
	return StatusInfo{
		EarliestHeight: 1,
		LatestHeight:   h,
		LatestTime:     r.Block.Header.Time,
	}, nil
}

// FetchBlocks fetches each requested block via LCD. LCD has no batch endpoint,
// so this is N parallel goroutines, 2 requests each (block header + txs). It
// will generally be slower than the RPC client unless your LCD is dramatically
// better at concurrency than your RPC.
func (c *RESTClient) FetchBlocks(ctx context.Context, heights []int64) ([]*Block, error) {
	out := make([]*Block, len(heights))
	type result struct {
		idx int
		blk *Block
		err error
	}
	ch := make(chan result, len(heights))
	for i, h := range heights {
		go func(i int, h int64) {
			b, err := c.fetchOne(ctx, h)
			ch <- result{i, b, err}
		}(i, h)
	}
	for range heights {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		out[r.idx] = r.blk
	}
	return out, nil
}

func (c *RESTClient) fetchOne(ctx context.Context, height int64) (*Block, error) {
	// 1) Block header (for timestamp)
	var hdr struct {
		Block struct {
			Header struct {
				Height string    `json:"height"`
				Time   time.Time `json:"time"`
			} `json:"header"`
		} `json:"block"`
	}
	path := fmt.Sprintf("/cosmos/base/tendermint/v1beta1/blocks/%d", height)
	if err := c.getJSON(ctx, path, &hdr); err != nil {
		if perr := classifyLCDStatus(err, height, c.baseURL); perr != nil {
			return nil, perr
		}
		return nil, fmt.Errorf("header h=%d: %w", height, err)
	}

	// 2) Txs with decoded events
	var txs struct {
		TxResponses []struct {
			TxHash string `json:"txhash"`
			Events []struct {
				Type       string `json:"type"`
				Attributes []struct {
					Key   string `json:"key"`
					Value string `json:"value"`
				} `json:"attributes"`
			} `json:"events"`
		} `json:"tx_responses"`
	}
	path = fmt.Sprintf("/cosmos/tx/v1beta1/txs/block/%d?pagination.limit=1000", height)
	if err := c.getJSON(ctx, path, &txs); err != nil {
		if perr := classifyLCDStatus(err, height, c.baseURL); perr != nil {
			return nil, perr
		}
		return nil, fmt.Errorf("txs h=%d: %w", height, err)
	}

	events := make([]Event, 0, 32)
	idx := 0
	for _, tx := range txs.TxResponses {
		for _, e := range tx.Events {
			ev := Event{
				TxHash:   strings.ToUpper(tx.TxHash),
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

	return &Block{
		Height: height,
		Time:   hdr.Block.Header.Time,
		Events: events,
	}, nil
}

// getJSON runs a retrying GET (same policy as the RPC client) and decodes
// the response body into out.
func (c *RESTClient) getJSON(ctx context.Context, path string, out any) error {
	body, err := retryableGet(ctx, c.http, c.baseURL+path, c.headers)
	if err != nil {
		return fmt.Errorf("lcd %s: %w", path, err)
	}
	return json.Unmarshal(body, out)
}

// classifyLCDStatus inspects an error returned by getJSON and promotes
// pruned-height signals to a typed *HeightPrunedError. Cosmos SDK LCD
// answers with 400 or 404 for heights below the node's pruning horizon
// (the exact body varies by SDK version, so we match on status alone).
// Returns nil if the error isn't an HTTP status or isn't recognisable as
// pruning — callers fall through to the generic error path.
func classifyLCDStatus(err error, height int64, url string) error {
	var hse *HTTPStatusError
	if !errors.As(err, &hse) {
		return nil
	}
	if hse.Status == http.StatusNotFound || hse.Status == http.StatusBadRequest {
		return &HeightPrunedError{Height: height, URL: url, Status: hse.Status}
	}
	return nil
}

// retryableGet mirrors retryableCall but for GETs. Sharing would be nice
// but GET has no body to pass in, and adding an option for that just
// muddies the RPC hot path.
func retryableGet(ctx context.Context, httpc *http.Client, url string, extraHeaders map[string]string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, err
		}
		// User headers first, then the required Accept — ours wins so a
		// malformed config can't break content negotiation.
		for k, v := range extraHeaders {
			req.Header.Set(k, v)
		}
		req.Header.Set("accept", "application/json")
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
			return nil, &ErrRateLimited{
				RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
				Status:     resp.StatusCode,
				Body:       truncate(string(data), 200),
			}
		}
		if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			lastErr = fmt.Errorf("http %d", resp.StatusCode)
			retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
			if !sleepBackoff(ctx, attempt, retryAfter) {
				return nil, ctx.Err()
			}
			continue
		}
		// 4xx other than 429 — terminal. Wrap so callers can classify
		// pruned-height responses (Cosmos LCD returns 400/404 for blocks
		// below the node's pruning horizon) without parsing strings.
		return nil, &HTTPStatusError{
			Status: resp.StatusCode,
			Body:   truncate(string(data), 300),
			URL:    url,
		}
	}
	return nil, fmt.Errorf("exceeded %d retries: %w", maxRetries, lastErr)
}
