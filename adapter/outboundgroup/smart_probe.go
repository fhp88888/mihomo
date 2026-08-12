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

const topK = 15

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
				start := time.Now()
				newConn, connectTime, dialErr := singleDial(ctx, p, metadata, start)
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
		// NOTE: leaderCancel is NOT called here.  raceStaggered (reached via
		// probeBatch below) owns cancellation of raceCtx for the normal path:
		// on a winner it defers cancelRace until the background drain finishes
		// sampling late losers' connectTime (keepLosersAlive), and on failure
		// paths its deferred cancelRace fires immediately.  Canceling
		// leaderCtx here would abort those in-flight loser dials the instant
		// the winner returns, defeating the loser sampling.  leaderCtx is only
		// canceled by Close() for shutdown, or GC'd once this discovery is
		// deleted and unreferenced (no goroutine blocks on it besides the
		// self-bounded 2s dials).
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

// dialResult is the result of a single dial attempt in a staggered race.
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

		var failMu sync.Mutex
		var fatalErr error

		winner, conn, connectTime, err := raceStaggered(ctx, key, batch, &pc.wg,
			// Discovery dials use the caller's raw dial (no MarkFailed inside —
			// probeBatch classifies node-level vs fatal itself).
			func(dialCtx context.Context, p C.Proxy) (C.Conn, int64, error) {
				start := time.Now()
				return singleDial(dialCtx, p, metadata, start)
			},
			// onConnect: record successful connectTimes so losers' measurements
			// are not wasted — they improve prerank accuracy for subsequent
			// discoveries on this route key.
			func(proxyName string, connectTime int64) {
				rt.UpdateLatency(key, proxyName, connectTime)
			},
			// onFail: classify node-level vs fatal/cancellation.  Only proxies
			// with node-level errors are penalized (MarkFailed 1.0); fatal
			// target-level errors and cancellations are not the proxy's fault.
			func(p C.Proxy, dialErr error) {
				if tunnel.ShouldStopRetry(dialErr) {
					failMu.Lock()
					if fatalErr == nil {
						fatalErr = dialErr
					}
					failMu.Unlock()
					return // Don't penalize proxy for target-level error
				}
				if errors.Is(dialErr, context.Canceled) {
					return // Don't penalize proxy for cancellation
				}
				rt.MarkFailed(key, p.Name(), 1.0)
			},
			// onWinner: probeBatch does its own winner bookkeeping via the
			// returned winner, so nothing to do here.
			nil,
		)

		if err == nil && conn != nil {
			log.Infoln("[Smart] probeBatch key=%s winner=%s connectTime=%dms", key, winner.Name(), connectTime)
			return probeResult{proxy: winner, conn: conn, connectTime: connectTime}
		}

		// No winner.  Fatal errors were captured live in onFail; node-level
		// failures were penalized via MarkFailed there too.
		log.Infoln("[Smart] probeBatch key=%s batch-ALL-FAILED err=%v", key, err)

		failMu.Lock()
		fe := fatalErr
		failMu.Unlock()

		if fe != nil {
			return probeResult{err: fe}
		}

		// If ctx is done, stop
		if ctx.Err() != nil {
			return probeResult{err: ctx.Err()}
		}

		// All dials failed with node-level errors — continue to the next batch.
	}

	return probeResult{err: fmt.Errorf("all %d proxies failed for key=%s", n, key)}
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
