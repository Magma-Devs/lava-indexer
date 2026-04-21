package snapshotters

import (
	"context"
	"fmt"
	"time"
)

// BlockTimeLookup resolves a block height's on-chain timestamp, plus the
// chain tip. Implementations typically wrap a Cosmos LCD REST client.
//
// Only these two probes are needed for the "find the block closest to
// timestamp T" search in FindBlockAtTime — BlockTime narrows the range,
// Tip provides the upper bound.
type BlockTimeLookup interface {
	BlockTime(ctx context.Context, height int64) (time.Time, error)
	Tip(ctx context.Context) (int64, error)
}

// FindBlockAtTime binary-searches for the block whose header timestamp
// is closest to target (without going over). Returns the resolved
// height + that block's actual timestamp.
//
// Algorithm:
//
//   - Read tip. If target >= tip's time, return tip (can't look into
//     the future).
//   - Read block `low` (caller-supplied — e.g. chain genesis or a
//     known-early height). If target <= low's time, return low.
//   - Otherwise binary-search between [low, tip]. Each step reads the
//     midpoint's timestamp to decide which half to keep.
//   - On convergence, pick the side closer to `target`.
//
// Converges in O(log₂(tip - low)) block-header reads — ~26 calls for a
// 100M-block range. Mirrors the pattern used by mainstream indexers
// (SubQuery, Subgraph) for "block at timestamp" resolution.
//
// Caller supplies low to constrain the search. Using 1 works but wastes
// ~log₂(tip) reads on ancient history the target will never be in —
// supply a sensible earliest (e.g. genesis or a known-historical height)
// where possible.
func FindBlockAtTime(ctx context.Context, src BlockTimeLookup, target time.Time, low int64) (int64, time.Time, error) {
	if low < 1 {
		low = 1
	}
	high, err := src.Tip(ctx)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("tip: %w", err)
	}
	if high < low {
		return 0, time.Time{}, fmt.Errorf("tip %d below low %d", high, low)
	}

	lowTime, err := src.BlockTime(ctx, low)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("block time h=%d: %w", low, err)
	}
	if !target.After(lowTime) {
		return low, lowTime, nil
	}
	highTime, err := src.BlockTime(ctx, high)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("block time h=%d: %w", high, err)
	}
	if !target.Before(highTime) {
		return high, highTime, nil
	}

	// Invariant: lowTime <= target <= highTime, so both ends bracket the
	// target. Binary-search narrows the range. The invariant is preserved
	// by only ever reassigning low or high based on direct comparison.
	for high-low > 1 {
		if ctx.Err() != nil {
			return 0, time.Time{}, ctx.Err()
		}
		mid := low + (high-low)/2
		midTime, err := src.BlockTime(ctx, mid)
		if err != nil {
			return 0, time.Time{}, fmt.Errorf("block time h=%d: %w", mid, err)
		}
		if midTime.After(target) {
			high = mid
			highTime = midTime
			continue
		}
		low = mid
		lowTime = midTime
	}

	// low.time <= target <= high.time, with high - low ≤ 1. Pick the side
	// closer to target; ties go to `low` (conservative: never picks a
	// block that's later than target).
	if target.Sub(lowTime) <= highTime.Sub(target) {
		return low, lowTime, nil
	}
	return high, highTime, nil
}
