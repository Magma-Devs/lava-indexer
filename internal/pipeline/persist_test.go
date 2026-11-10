package pipeline

import (
	"context"
	"errors"
	"testing"
)

func jobs(froms ...int64) []writeJob {
	out := make([]writeJob, len(froms))
	for i, f := range froms {
		out[i] = writeJob{from: f, to: f + 9, blocksThisBatch: 10}
	}
	return out
}

func froms(js []writeJob) []int64 {
	out := make([]int64, len(js))
	for i, j := range js {
		out[i] = j.from
	}
	return out
}

func equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// persistFailing fails any persist call whose batch contains a job starting
// at one of bad, mimicking a tx that aborts on that job.
func persistFailing(calls *int, bad ...int64) func([]writeJob) error {
	return func(js []writeJob) error {
		*calls++
		for _, j := range js {
			for _, b := range bad {
				if j.from == b {
					return errors.New("duplicate key")
				}
			}
		}
		return nil
	}
}

func TestPersistWithFallbackAllCommit(t *testing.T) {
	var calls int
	var failed []int64
	got := persistWithFallback(context.Background(), jobs(1, 11, 21), persistFailing(&calls),
		func(j writeJob, _ error) { failed = append(failed, j.from) })
	if !equal(froms(got), []int64{1, 11, 21}) || len(failed) != 0 || calls != 1 {
		t.Fatalf("committed=%v failed=%v calls=%d, want [1 11 21] [] 1", froms(got), failed, calls)
	}
}

// One bad job dead-letters only itself; its batch-mates still commit
// (MAG-5084: previously every job in the aggregated tx was dead-lettered).
func TestPersistWithFallbackIsolatesBadJob(t *testing.T) {
	var calls int
	var failed []int64
	got := persistWithFallback(context.Background(), jobs(1, 11, 21), persistFailing(&calls, 11),
		func(j writeJob, _ error) { failed = append(failed, j.from) })
	if !equal(froms(got), []int64{1, 21}) {
		t.Fatalf("committed = %v, want [1 21]", froms(got))
	}
	if !equal(failed, []int64{11}) {
		t.Fatalf("failed = %v, want [11]", failed)
	}
	if calls != 4 {
		t.Fatalf("persist calls = %d, want 4 (1 aggregated + 3 single)", calls)
	}
}

func TestPersistWithFallbackSingleJobFails(t *testing.T) {
	var calls int
	var failed []int64
	got := persistWithFallback(context.Background(), jobs(5), persistFailing(&calls, 5),
		func(j writeJob, _ error) { failed = append(failed, j.from) })
	if len(got) != 0 || !equal(failed, []int64{5}) || calls != 1 {
		t.Fatalf("committed=%v failed=%v calls=%d, want [] [5] 1", froms(got), failed, calls)
	}
}

// On shutdown the failure is the cancelled ctx, not the jobs: nothing is
// retried or dead-lettered.
func TestPersistWithFallbackCancelledCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls int
	var failed []int64
	got := persistWithFallback(ctx, jobs(1, 11), persistFailing(&calls, 1),
		func(j writeJob, _ error) { failed = append(failed, j.from) })
	if len(got) != 0 || len(failed) != 0 || calls != 1 {
		t.Fatalf("committed=%v failed=%v calls=%d, want [] [] 1", froms(got), failed, calls)
	}
}
