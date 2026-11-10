package rpc

import (
	"encoding/json"
	"strings"
	"testing"
)

// blockJSON returns a /block result at height with n txs.
func blockJSON(height string, n int) json.RawMessage {
	txs := make([]string, n)
	for i := range txs {
		txs[i] = `"dHg="` // base64("tx")
	}
	return json.RawMessage(`{"block":{"header":{"height":"` + height +
		`","time":"2026-01-01T00:00:00Z"},"data":{"txs":[` + strings.Join(txs, ",") + `]}}}`)
}

// resultsJSON returns a /block_results result with n txs_results, each
// carrying one lava_relay_payment event.
func resultsJSON(n int) json.RawMessage {
	res := make([]string, n)
	for i := range res {
		res[i] = `{"events":[{"type":"lava_relay_payment","attributes":[{"key":"provider.0","value":"p"}]}]}`
	}
	return json.RawMessage(`{"txs_results":[` + strings.Join(res, ",") + `]}`)
}

func TestDecodeBlockOK(t *testing.T) {
	b, err := decodeBlock(42, blockJSON("42", 2), resultsJSON(2))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if b.Height != 42 || len(b.Events) != 2 {
		t.Fatalf("got height=%d events=%d, want 42 and 2", b.Height, len(b.Events))
	}
}

func TestDecodeBlockEmptyBlock(t *testing.T) {
	b, err := decodeBlock(7, blockJSON("7", 0), json.RawMessage(`{"txs_results":null}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(b.Events) != 0 {
		t.Fatalf("events = %d, want 0", len(b.Events))
	}
}

// Responses that cannot be the requested block's complete results must be
// rejected, so MultiClient fails over instead of the pipeline recording a
// block with missing events as indexed (MAG-5084).
func TestDecodeBlockRejectsIncompleteResponses(t *testing.T) {
	cases := []struct {
		name           string
		block, results json.RawMessage
		wantErr        string
	}{
		{"fewer txs_results than txs", blockJSON("42", 4), resultsJSON(0), "0 txs_results for 4 txs"},
		{"more txs_results than txs", blockJSON("42", 1), resultsJSON(2), "2 txs_results for 1 txs"},
		{"null block_results", blockJSON("42", 1), json.RawMessage(`null`), "block_results: empty result"},
		{"missing block_results", blockJSON("42", 1), nil, "block_results: empty result"},
		{"null block", json.RawMessage(`null`), resultsJSON(1), "block: empty result"},
		{"other height", blockJSON("41", 1), resultsJSON(1), `header height "41", want 42`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := decodeBlock(42, c.block, c.results)
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, c.wantErr)
			}
		})
	}
}
