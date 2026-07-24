package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
)

const topK = 10

// discoveryState tracks an in-progress discovery for a route key.
type discoveryState struct {
	mu           sync.Mutex
	done         chan struct{}
	proxy        C.Proxy
	conn         C.Conn
	err          error
	leaderCtx    context.Context
	leaderCancel context.CancelFunc
}

// ProbeCoordinator manages concurrent TCP discovery with per-route-key merging.
// Only one discovery (leader) runs per route key; followers wait for the result.
type ProbeCoordinator struct {
	mu          sync.Mutex
	discoveries map[string]*discoveryState
	closed      bool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewProbeCoordinator creates a new ProbeCoordinator.
func NewProbeCoordinator() *ProbeCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &ProbeCoordinator{
		discoveries: make(map[string]*discoveryState),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Discover runs a discovery for the given route key. If another goroutine is
// already discovering this key, the caller waits for that result. Otherwise,
// this goroutine becomes the leader and probes the top-K proxies concurrently,
// returning the first successful connection.
func (pc *ProbeCoordinator) Discover(
	ctx context.Context,
	key string,
	proxies []C.Proxy,
	metadata *C.Metadata,
	preRanked []string,
	singleDial func(context.Context, C.Proxy, *C.Metadata, time.Time) (C.Conn, int64, error),
	rt *smart.RouteTable,
) (C.Proxy, C.Conn, int64, error) {

	// Check if we should join an existing discovery
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return nil, nil, 0, errors.New("probe coordinator closed")
	}

	ds, exists := pc.discoveries[key]
	if exists {
		// Follower path: wait for leader
		done := ds.done
		pc.mu.Unlock()

		select {
		case <-done:
			ds.mu.Lock()
			p, e := ds.proxy, ds.err
			ds.mu.Unlock()

			if p != nil && e == nil {
				// Follower gets a NEW connection to the same proxy
				start := time.Now(); newConn, connectTime, dialErr := singleDial(ctx, p, metadata, start)
				if dialErr != nil {
					return nil, nil, 0, dialErr
				}
				return p, newConn, connectTime, nil
			}
			return nil, nil, 0, e
		case <-ctx.Done():
			return nil, nil, 0, ctx.Err()
		}
	}

	// Leader path: create discovery state
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	ds = &discoveryState{
		done:         make(chan struct{}),
		leaderCtx:    leaderCtx,
		leaderCancel: leaderCancel,
	}
	pc.discoveries[key] = ds
	pc.wg.Add(1)
	pc.mu.Unlock()

	defer func() {
		leaderCancel()
		pc.mu.Lock()
		delete(pc.discoveries, key)
		pc.mu.Unlock()
		close(ds.done)
		pc.wg.Done()
	}()

	// Probe top-K in batches
	proxy := pc.probeBatch(leaderCtx, key, proxies, metadata, preRanked, singleDial, rt)

	ds.mu.Lock()
	ds.proxy = proxy.proxy
	ds.conn = proxy.conn
	ds.err = proxy.err
	ds.mu.Unlock()

	return proxy.proxy, proxy.conn, proxy.connectTime, proxy.err
}

type probeResult struct {
	proxy       C.Proxy
	conn        C.Conn
	connectTime int64
	err         error
}

// probeMetric records a single proxy's connectTime during parallel dial.
type probeMetric struct {
	proxyName   string
	connectTime int64
}

// dialResult is the result of a single dial attempt in parallelDial.
type dialResult struct {
	proxy       C.Proxy
	conn        C.Conn
	connectTime int64
	err         error
}

// probeBatch probes proxies in batches of topK. Returns the first successful result.
func (pc *ProbeCoordinator) probeBatch(
	ctx context.Context,
	key string,
	proxies []C.Proxy,
	metadata *C.Metadata,
	preRanked []string,
	singleDial func(context.Context, C.Proxy, *C.Metadata, time.Time) (C.Conn, int64, error),
	rt *smart.RouteTable,
) probeResult {
	// Build a name→proxy lookup
	proxyMap := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		proxyMap[p.Name()] = p
	}

	n := len(preRanked)
	for offset := 0; offset < n; {
		// Take up to topK proxies from the current offset
		batch := make([]C.Proxy, 0, topK)
		batchNames := make([]string, 0, topK)
		for i := offset; i < n && len(batch) < topK; i++ {
			if p, ok := proxyMap[preRanked[i]]; ok {
				batch = append(batch, p)
				batchNames = append(batchNames, p.Name())
			}
		}
		offset += len(batch)

		if len(batch) == 0 {
			break
		}

		log.Infoln("[Smart] probeBatch key=%s batch=%v", key, batchNames)
		result, metrics, dialResults := pc.parallelDial(ctx, key, batch, metadata, singleDial, rt)
		// Record all successful connectTimes to the route table so losers'
		// measurements are not wasted — they improve prerank accuracy for
		// subsequent discoveries on this route key.
		for _, m := range metrics {
			rt.UpdateLatency(key, m.proxyName, m.connectTime)
		}
		if result.err == nil {
			log.Infoln("[Smart] probeBatch key=%s winner=%s connectTime=%dms", key, result.proxy.Name(), result.connectTime)
			return result
		}

		// All failed in this batch.
		// Only mark proxies that had node-level errors (not fatal/target-level
		// errors and not cancellations). Target-level errors like DNS failures
		// or loopback rejects are not the proxy's fault and should not penalize
		// the proxy's score.
		log.Infoln("[Smart] probeBatch key=%s batch-ALL-FAILED err=%v", key, result.err)
		hasFatal := false
		var fatalErr error
		for _, dr := range dialResults {
			if dr.err == nil {
				continue
			}
			if tunnel.ShouldStopRetry(dr.err) {
				hasFatal = true
				fatalErr = dr.err
				continue // Don't penalize proxy for target-level error
			}
			if errors.Is(dr.err, context.Canceled) {
				continue // Don't penalize proxy for cancellation
			}
			rt.MarkFailed(key, dr.proxy.Name())
		}

		if hasFatal {
			return probeResult{err: fatalErr}
		}

		// If ctx is done, stop
		if ctx.Err() != nil {
			return probeResult{err: ctx.Err()}
		}
	}

	return probeResult{err: fmt.Errorf("all %d proxies failed for key=%s", n, key)}
}

// parallelDial concurrently dials all proxies in the batch.
// Returns the FIRST successful connection immediately without waiting for
// slower goroutines.  Remaining goroutines complete in the background and
// write their connectTimes directly to the route table.
func (pc *ProbeCoordinator) parallelDial(
	ctx context.Context,
	key string,
	batch []C.Proxy,
	metadata *C.Metadata,
	singleDial func(context.Context, C.Proxy, *C.Metadata, time.Time) (C.Conn, int64, error),
	rt *smart.RouteTable,
) (probeResult, []probeMetric, []dialResult) {
	n := len(batch)
	if n == 0 {
		return probeResult{err: errors.New("empty batch")}, nil, nil
	}

	results := make(chan dialResult, n)

	for i := 0; i < n; i++ {
		pc.wg.Add(1)
		go func(p C.Proxy) {
			defer pc.wg.Done()
			start := time.Now()
			conn, connectTime, err := singleDial(ctx, p, metadata, start)
			results <- dialResult{proxy: p, conn: conn, connectTime: connectTime, err: err}
		}(batch[i])
	}

	var allMetrics []probeMetric
	var allResults []dialResult
	received := 0

	for received < n {
		select {
		case res := <-results:
			received++
			allResults = append(allResults, res)
			if res.err == nil {
				log.Infoln("[Smart] parallelDial key=%s proxy=%s connectTime=%dms (winner)",
					key, res.proxy.Name(), res.connectTime)
				allMetrics = append(allMetrics, probeMetric{proxyName: res.proxy.Name(), connectTime: res.connectTime})
				// Drain remaining results in background — close slower
				// connections but still collect their connectTimes.
				remaining := n - received
				if remaining > 0 {
					pc.wg.Add(1)
					go func() {
						defer pc.wg.Done()
						drainResults(results, remaining, rt, key, true)
					}()
				}
				return probeResult{proxy: res.proxy, conn: res.conn, connectTime: res.connectTime}, allMetrics, allResults
			}
			log.Infoln("[Smart] parallelDial key=%s proxy=%s FAILED err=%v",
				key, res.proxy.Name(), res.err)
		case <-ctx.Done():
			log.Infoln("[Smart] parallelDial key=%s ctx-done received=%d/%d",
				key, received, n)
			// Drain remaining results to close any successful connections
			// from goroutines that completed concurrently with cancellation.
			remaining := n - received
			if remaining > 0 {
				pc.wg.Add(1)
				go func() {
					defer pc.wg.Done()
					drainResults(results, remaining, nil, "", false)
				}()
			}
			return probeResult{err: ctx.Err()}, allMetrics, allResults
		}
	}

	log.Infoln("[Smart] parallelDial key=%s ALL-FAILED (%d proxies)", key, n)
	errs := make([]error, 0, len(allResults))
	for _, r := range allResults {
		if r.err != nil {
			errs = append(errs, r.err)
		}
	}
	return probeResult{err: fmt.Errorf("all %d proxies failed for key=%s: %w", n, key, errors.Join(errs...))}, allMetrics, allResults
}

// drainResults drains n results from the channel, optionally updating the
// route table with latency measurements, and closing any successful connections.
func drainResults(results <-chan dialResult, n int, rt *smart.RouteTable, key string, updateRT bool) {
	for i := 0; i < n; i++ {
		r := <-results
		if r.err == nil {
			if updateRT && rt != nil {
				rt.UpdateLatency(key, r.proxy.Name(), r.connectTime)
				log.Debugln("[Smart] parallelDial key=%s proxy=%s connectTime=%dms (loser)",
					key, r.proxy.Name(), r.connectTime)
			}
			if r.conn != nil {
				r.conn.Close()
			}
		}
	}
}

// Close cancels all active discoveries and waits for workers to finish.
func (pc *ProbeCoordinator) Close() error {
	pc.mu.Lock()
	pc.closed = true
	for _, ds := range pc.discoveries {
		ds.leaderCancel()
	}
	pc.mu.Unlock()

	pc.cancel()
	pc.wg.Wait()
	return nil
}
