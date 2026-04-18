package pipeline

import (
	"context"
	"errors"

	"github.com/magma-devs/lava-indexer/internal/rpc"
)

// isPermanentFetchError reports whether a fetch error is permanent — i.e.
// no amount of retrying will fix it, so the height should short-circuit
// straight to the dead-letter table with permanent=true instead of cycling
// through the retry sweep forever.
//
// Permanent:
//   - ErrNoEndpointCovers: no endpoint's coverage window includes the
//     height. Retrying makes sense only if the endpoint fleet changes,
//     which is a deploy-time event, not a runtime one.
//   - ErrHeightPruned: the endpoint has pruned this height. Same story —
//     the data isn't coming back.
//
// Transient (returns false — current retry path continues to apply):
//   - ErrRateLimited: the endpoint is asking us to slow down.
//   - Context errors: the parent cancelled or the deadline expired; the
//     parent decides what to do, not the classifier.
//   - Any other error (5xx, network failures, decode errors): treated as
//     transient by default. The cost of mis-classifying a transient as
//     permanent is silent data loss, so the classifier is deliberately
//     conservative.
func isPermanentFetchError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, rpc.ErrNoEndpointCovers) {
		return true
	}
	if errors.Is(err, rpc.ErrHeightPruned) {
		return true
	}
	return false
}
