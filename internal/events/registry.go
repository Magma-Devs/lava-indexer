package events

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/magma-devs/lava-indexer/internal/rpc"
)

// Handler is the extensibility seam for new event types. Each concrete handler
// owns a set of event types, its own DDL, and its write logic.
//
// To index a new Lava event:
//   1. Create a new package (e.g. internal/events/provider_stake/).
//   2. Implement this interface: Name returns a stable identifier used in the
//      state table; EventTypes returns the event.Type strings the handler
//      cares about; DDL returns the SQL to create its tables/indexes
//      (idempotent — use IF NOT EXISTS); Persist writes all its events for
//      one batch inside the passed transaction.
//   3. Register it in main.go alongside the existing handlers.
//
// The pipeline dispatches events by type, groups them per-handler, and
// invokes Persist once per batch — in the same pgx.Tx that also merges the
// batch's height-range into each handler's own row-set in
// app.indexer_ranges. That single commit is what makes "already indexed"
// robust: if the process dies mid-batch, neither the rows NOR the range
// update land, so the batch is re-fetched on next run.
type Handler interface {
	// Name is the stable key used in app.indexer_ranges. Changing it looks
	// like a brand-new handler with no coverage — so pick once and keep it.
	Name() string
	EventTypes() []string
	DDL() []string
	Persist(ctx context.Context, tx pgx.Tx, events []HandledEvent) error
}

// HandledEvent is one event paired with its block context (for timestamp,
// height). The same Block pointer is shared across multiple events.
type HandledEvent struct {
	Block *rpc.Block
	Event rpc.Event
}

// Registry groups handlers by event type for O(1) dispatch during indexing.
type Registry struct {
	handlers     []Handler
	byEventType  map[string][]Handler
}

func NewRegistry() *Registry {
	return &Registry{byEventType: make(map[string][]Handler)}
}

func (r *Registry) Register(h Handler) {
	r.handlers = append(r.handlers, h)
	for _, t := range h.EventTypes() {
		r.byEventType[t] = append(r.byEventType[t], h)
	}
}

// All returns every registered handler. Useful for running DDL on startup.
func (r *Registry) All() []Handler {
	out := make([]Handler, len(r.handlers))
	copy(out, r.handlers)
	return out
}

// Dispatch groups a batch of blocks' events into per-handler HandledEvent
// slices, ready to hand to Handler.Persist. Events whose type has no
// registered handler are dropped.
func (r *Registry) Dispatch(blocks []*rpc.Block) map[Handler][]HandledEvent {
	out := make(map[Handler][]HandledEvent)
	for _, b := range blocks {
		for _, ev := range b.Events {
			for _, h := range r.byEventType[ev.Type] {
				out[h] = append(out[h], HandledEvent{Block: b, Event: ev})
			}
		}
	}
	return out
}
