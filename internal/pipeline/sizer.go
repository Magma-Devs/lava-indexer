package pipeline

import (
	"context"
	"log/slog"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveSizer owns the runtime-tunable knobs that aren't already per-
// endpoint: the batch size each Fetch sends, and the effective worker
// ceiling derived from host resources + endpoint budgets.
//
// The rpc package's AIMD already takes care of per-endpoint concurrency;
// this package's job is the pieces that live above that layer.
//
// Adjustments fire on a timer (default 10s):
//
//   Batch:
//     - shrink (÷2, floor = 1)          if any endpoint's recent error
//                                        rate ≥ 3% OR p99 > 10 × p50
//     - grow   (+1, cap = BatchMax)     if all endpoints have error
//                                        rate < 0.5% AND p99 < 3 × p50
//     - otherwise hold
//
//   Workers:
//     - cap = min(
//         Σ endpoint budgets × 2,        // enough slots for every ep
//         NumCPU × 8,                    // I/O-bound so multi-core safe
//         GOMEMLIMIT / 2 MiB,            // one batch buffer per worker
//         HardMax,
//       )
//     - exposed to the UI; actual goroutine pool size is fixed at startup
//       to this value so we never have to spawn/kill goroutines at
//       runtime — extras just back-pressure on the AIMD budget gate.
type AdaptiveSizer struct {
	BatchMin int32
	BatchMax int32
	HardMax  int

	batch    atomic.Int32 // current dynamic batch size
	workers  atomic.Int32 // current effective worker cap
	interval time.Duration

	mu sync.Mutex
	// Sampler and signal source are injected — keeps sizer testable.
	signals SignalSource
}

// SignalSource is what the sizer queries each tick. The rpc.MultiClient
// fulfils it (via the Pipeline's connection) without the pipeline package
// having to import rpc for type reasons.
type SignalSource interface {
	// EndpointSignals reports per-endpoint error rate (0-1) and the
	// current AIMD budget, for as many endpoints as are healthy.
	EndpointSignals() []EndpointSignal
}

type EndpointSignal struct {
	URL               string
	RecentErrorRate   float64
	LatencyP50Ms      float64
	LatencyP99Ms      float64
	Budget            int
}

func NewAdaptiveSizer(batchMin, batchMax int, hardMax int, interval time.Duration, signals SignalSource) *AdaptiveSizer {
	if batchMin < 1 {
		batchMin = 1
	}
	if batchMax < batchMin {
		batchMax = batchMin
	}
	if hardMax < 1 {
		hardMax = 128
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}
	s := &AdaptiveSizer{
		BatchMin: int32(batchMin),
		BatchMax: int32(batchMax),
		HardMax:  hardMax,
		interval: interval,
		signals:  signals,
	}
	// Start at the upper bound — shrink only if signals demand it.
	s.batch.Store(int32(batchMax))
	return s
}

func (s *AdaptiveSizer) BatchSize() int { return int(s.batch.Load()) }
func (s *AdaptiveSizer) Workers() int   { return int(s.workers.Load()) }

// Start runs the tick loop until ctx is cancelled.
func (s *AdaptiveSizer) Start(ctx context.Context) {
	// First evaluation: size workers against the host + initial budgets.
	s.tick()
	go func() {
		t := time.NewTicker(s.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.tick()
			}
		}
	}()
}

func (s *AdaptiveSizer) tick() {
	s.mu.Lock()
	defer s.mu.Unlock()

	signals := s.signals.EndpointSignals()

	// --- batch size ---
	current := s.batch.Load()
	anyHot := false    // any endpoint stressed → shrink
	allCalm := true    // every endpoint healthy → grow
	budgetSum := 0
	for _, sg := range signals {
		budgetSum += sg.Budget
		// "hot": the node is actively telling us to slow down.
		if sg.RecentErrorRate >= 0.03 ||
			(sg.LatencyP50Ms > 0 && sg.LatencyP99Ms > sg.LatencyP50Ms*10) {
			anyHot = true
		}
		// "calm": the node has capacity left.
		if !(sg.RecentErrorRate < 0.005 &&
			(sg.LatencyP50Ms == 0 || sg.LatencyP99Ms < sg.LatencyP50Ms*3)) {
			allCalm = false
		}
	}
	switch {
	case anyHot:
		newBatch := current / 2
		if newBatch < s.BatchMin {
			newBatch = s.BatchMin
		}
		if newBatch != current {
			s.batch.Store(newBatch)
			slog.Info("adaptive: batch size shrunk", "from", current, "to", newBatch)
		}
	case allCalm && current < s.BatchMax:
		s.batch.Store(current + 1)
		// Not worth logging every +1 tick; it'd be noisy.
	}

	// --- worker cap ---
	epCap := budgetSum * 2
	if epCap < 4 {
		epCap = 4
	}
	cpuCap := runtime.NumCPU() * 8
	memCap := estimateMemWorkerCap()
	cap := minInt(epCap, cpuCap, memCap, s.HardMax)
	prev := int(s.workers.Load())
	if cap != prev {
		s.workers.Store(int32(cap))
		slog.Info("adaptive: worker cap updated",
			"from", prev, "to", cap,
			"ep_cap", epCap, "cpu_cap", cpuCap, "mem_cap", memCap, "hard_max", s.HardMax)
	}
}

// estimateMemWorkerCap returns how many in-flight workers the process can
// afford in memory. Assumes ~2 MiB per in-flight worker (one decoded batch
// of blocks + goroutine stack). If GOMEMLIMIT isn't set, falls back to the
// Go runtime's current heap ceiling from debug.ReadGCStats.
func estimateMemWorkerCap() int {
	const bytesPerWorker = 2 * 1024 * 1024 // 2 MiB — generous for batch=50 of dense blocks
	limit := debug.SetMemoryLimit(-1)      // read without setting
	if limit <= 0 {
		// GOMEMLIMIT unset → be conservative and assume 1 GiB budget.
		limit = 1 * 1024 * 1024 * 1024
	}
	// Reserve half for Postgres driver, pipeline queues, etc.
	budget := limit / 2
	n := int(budget / int64(bytesPerWorker))
	if n < 4 {
		n = 4
	}
	return n
}

func minInt(vals ...int) int {
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return m
}
