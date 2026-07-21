package outboundgroup

import (
	"context"
	"errors"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/tunnel"
)

const topK = 5

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
	leaderCtx, leaderCancel := context.WithCancel(pc.ctx)
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
		for i := offset; i < n && len(batch) < topK; i++ {
			if p, ok := proxyMap[preRanked[i]]; ok {
				batch = append(batch, p)
			}
		}
		offset += len(batch)

		if len(batch) == 0 {
			break
		}

		result := pc.parallelDial(ctx, batch, metadata, singleDial)
		if result.err == nil {
			return result
		}

		// All failed in this batch; update route table and try next batch
		for _, p := range batch {
			rt.MarkFailed(key, p.Name())
		}

		// Check for fatal error
		if tunnel.ShouldStopRetry(result.err) {
			return result
		}

		// If ctx is done, stop
		if ctx.Err() != nil {
			return probeResult{err: ctx.Err()}
		}
	}

	return probeResult{err: errors.New("all proxies failed")}
}

// parallelDial concurrently dials all proxies in the batch.
// Waits for ALL goroutines to complete, then returns the connection with the
// lowest connectTime.  This avoids the "first goroutine wins" scheduling bias
// that previously caused the same proxy to win every probe race on localhost.
func (pc *ProbeCoordinator) parallelDial(
	ctx context.Context,
	batch []C.Proxy,
	metadata *C.Metadata,
	singleDial func(context.Context, C.Proxy, *C.Metadata, time.Time) (C.Conn, int64, error),
) probeResult {
	n := len(batch)
	if n == 0 {
		return probeResult{err: errors.New("empty batch")}
	}

	type dialResult struct {
		proxy       C.Proxy
		conn        C.Conn
		connectTime int64
		err         error
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

	var best *dialResult
	for received := 0; received < n; received++ {
		select {
		case res := <-results:
			if res.err == nil {
				if best == nil || res.connectTime < best.connectTime {
					// Close previous best connection
					if best != nil && best.conn != nil {
						best.conn.Close()
					}
					best = &res
				} else {
					// This one is slower — close it
					if res.conn != nil {
						res.conn.Close()
					}
				}
			}
		case <-ctx.Done():
			// Drain remaining in background
			go func(remaining int) {
				for i := 0; i < remaining; i++ {
					r := <-results
					if r.conn != nil && r.err == nil {
						r.conn.Close()
					}
				}
			}(n - received - 1)
			// If we already have a best, return it
			if best != nil {
				return probeResult{proxy: best.proxy, conn: best.conn, connectTime: best.connectTime}
			}
			return probeResult{err: ctx.Err()}
		}
	}

	if best != nil {
		return probeResult{proxy: best.proxy, conn: best.conn, connectTime: best.connectTime}
	}
	return probeResult{err: errors.New("all proxies failed")}
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
