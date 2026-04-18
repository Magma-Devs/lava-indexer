package rpc

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// BenchmarkResult is one concurrency-sweep data point for one endpoint.
type BenchmarkResult struct {
	URL            string        `json:"url"`
	Concurrency    int           `json:"concurrency"`
	Requests       int           `json:"requests"`
	Errors         int           `json:"errors"`
	RequestsPerSec float64       `json:"requests_per_sec"`
	LatencyP50     time.Duration `json:"latency_p50"`
	LatencyP99     time.Duration `json:"latency_p99"`
}

// RunBenchmark sweeps each endpoint at increasing concurrency levels and
// returns per-endpoint curves showing req/s and latency. Useful answer to
// "how much can my RPC take?" — the knee is where req/s plateaus while p99
// climbs. Call once at startup (via -benchmark CLI flag).
//
// Each level runs `requestsPerLevel` random single-block fetches in the
// endpoint's covered range with `c` goroutines worth of in-flight at once.
func RunBenchmark(ctx context.Context, endpoints []*Endpoint, levels []int, requestsPerLevel int) map[string][]BenchmarkResult {
	out := make(map[string][]BenchmarkResult)
	for _, ep := range endpoints {
		if ep.Disabled || ep.Earliest <= 0 || ep.Latest <= ep.Earliest {
			slog.Info("benchmark: skipping endpoint", "url", ep.URL, "reason", "not probed or no coverage")
			continue
		}
		slog.Info("benchmarking endpoint", "url", ep.URL, "earliest", ep.Earliest, "latest", ep.Latest)
		var curve []BenchmarkResult
		for _, c := range levels {
			res := benchOne(ctx, ep, c, requestsPerLevel)
			curve = append(curve, res)
			slog.Info("bench level",
				"url", ep.URL,
				"concurrency", c,
				"req_per_sec", fmt.Sprintf("%.1f", res.RequestsPerSec),
				"p50", res.LatencyP50,
				"p99", res.LatencyP99,
				"errors", res.Errors,
			)
			if ctx.Err() != nil {
				break
			}
		}
		out[ep.URL] = curve
	}
	return out
}

func benchOne(ctx context.Context, ep *Endpoint, concurrency, total int) BenchmarkResult {
	lo, hi := ep.Earliest, ep.Latest
	if hi <= lo {
		hi = lo + 1
	}
	// Random heights from the covered range; caching / replay won't trivially
	// hide the real fetch cost.
	//nolint:gosec // non-crypto randomness is fine for a load probe
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	type result struct {
		dur time.Duration
		err error
	}
	sem := make(chan struct{}, concurrency)
	results := make([]result, 0, total)
	var mu sync.Mutex
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < total; i++ {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		h := lo + rng.Int63n(hi-lo)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			t0 := time.Now()
			_, err := ep.Client.FetchBlocks(ctx, []int64{h})
			mu.Lock()
			results = append(results, result{dur: time.Since(t0), err: err})
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	errors := 0
	latencies := make([]time.Duration, 0, len(results))
	for _, r := range results {
		if r.err != nil {
			errors++
		}
		latencies = append(latencies, r.dur)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p50, p99 time.Duration
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		idx := len(latencies) * 99 / 100
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		p99 = latencies[idx]
	}
	return BenchmarkResult{
		URL:            ep.URL,
		Concurrency:    concurrency,
		Requests:       len(results),
		Errors:         errors,
		RequestsPerSec: float64(len(results)) / elapsed,
		LatencyP50:     p50,
		LatencyP99:     p99,
	}
}
