package rpc

import (
	"context"
	"time"
)

type Client interface {
	// Tip returns the current head block height of the chain.
	Tip(ctx context.Context) (int64, error)

	// FetchBlocks returns one Block per requested height, in no particular order.
	// Implementations should batch network calls where possible.
	FetchBlocks(ctx context.Context, heights []int64) ([]*Block, error)

	// Close releases any underlying resources (keep-alive connections, etc.).
	Close()
}

// Block is a denormalised view of what we need from a chain block:
// just enough to extract typed events for parsing.
type Block struct {
	Height int64
	Time   time.Time
	Events []Event
}

// Event is a typed event from either a transaction or a block-scope hook
// (begin_block, end_block, finalize_block). TxHash is empty for non-tx events.
// EventIdx is a block-scoped counter assigned at fetch time and stable across
// the same block fetched twice, so callers can use it as part of a primary key.
type Event struct {
	TxHash   string
	Type     string
	Attrs    map[string]string
	EventIdx int
}
