package rpc

import (
	"errors"
	"fmt"
)

// ErrNoEndpointCovers is returned when no healthy endpoint's coverage window
// includes the requested heights. It's a permanent failure for that range —
// no amount of retrying changes what an endpoint serves. Callers classify
// this so the dead-letter retry sweep doesn't churn on it forever.
var ErrNoEndpointCovers = errors.New("no endpoint covers requested heights")

// NoEndpointCoversError carries the offending range so operators can see
// exactly what wasn't covered. Implements Is() so `errors.Is(err,
// ErrNoEndpointCovers)` matches either the sentinel or the struct form.
type NoEndpointCoversError struct {
	MinHeight int64
	MaxHeight int64
}

func (e *NoEndpointCoversError) Error() string {
	return fmt.Sprintf("no endpoint covers heights %d-%d", e.MinHeight, e.MaxHeight)
}

func (e *NoEndpointCoversError) Is(target error) bool {
	return target == ErrNoEndpointCovers
}

// ErrHeightPruned is returned by a REST/LCD client when the node no longer
// serves the requested height because it's past its pruning horizon. Like
// ErrNoEndpointCovers this is permanent for the height — retrying against
// the same endpoint won't un-prune the data.
var ErrHeightPruned = errors.New("height pruned by endpoint")

// HeightPrunedError carries the height and originating endpoint for
// diagnostics. Matches ErrHeightPruned via Is().
type HeightPrunedError struct {
	Height int64
	URL    string
	Status int
}

func (e *HeightPrunedError) Error() string {
	return fmt.Sprintf("height %d pruned by %s (http %d)", e.Height, e.URL, e.Status)
}

func (e *HeightPrunedError) Is(target error) bool {
	return target == ErrHeightPruned
}

// HTTPStatusError wraps an HTTP response whose status code we want to
// reason about later (classification into permanent vs transient). The
// retryable loop only returns this for non-retryable statuses — 5xx and
// 429 are handled before it's constructed.
type HTTPStatusError struct {
	Status int
	Body   string
	URL    string
}

func (e *HTTPStatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("http %d", e.Status)
	}
	return fmt.Sprintf("http %d: %s", e.Status, e.Body)
}
