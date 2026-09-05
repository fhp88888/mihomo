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

type discoveryKey struct {
	routeKey string
	domain   string
}

// discoveryState tracks an in-progress discovery for a route key and domain.
type discoveryState struct {
	mu           sync.Mutex
	done         chan struct{}
	proxy        C.Proxy
	conn         C.Conn
	err          error
	leaderCtx    context.Context
	leaderCancel context.CancelFunc
}

// ProbeCoordinator merges TCP discovery for the same route key and domain.
// Only one leader runs per pair; followers wait for its result.
type ProbeCoordinator struct {
	mu          sync.Mutex
	discoveries map[discoveryKey]*discoveryState
	closed      bool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewProbeCoordinator creates a new ProbeCoordinator.
func NewProbeCoordinator() *ProbeCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &ProbeCoordinator{
		discoveries: make(map[discoveryKey]*discoveryState),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Discover runs a discovery for the given route key and normalized domain.
// If another goroutine is discovering that pair, the caller waits. Otherwise,
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
	domain := routeDomain(metadata)
	discoveryID := discoveryKey{routeKey: key, domain: domain}

	// Check if we should join an existing discovery
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return nil, nil, 0, errors.New("probe coordinator closed")
	}

	ds, exists := pc.discoveries[discoveryID]
	if exists {
		// Register the complete follower lifetime while holding pc.mu. Close
		// takes the same lock before Wait, so the counter cannot reach zero and
		// then be incremented by a late follower fallback.
		pc.wg.Add(1)
		done := ds.done
		pc.mu.Unlock()
		defer pc.wg.Done()

		// A follower is owned by both its caller and the coordinator. This keeps
		// its re-dial and any fallback discovery cancellable during Smart.Close.
		followerCtx, cancelFollower := context.WithCancel(ctx)
		stopCoordinatorCancel := context.AfterFunc(pc.ctx, cancelFollower)
		defer func() {
			stopCoordinatorCancel()
			cancelFollower()
		}()

		select {
		case <-done:
			ds.mu.Lock()
			p, e := ds.proxy, ds.err
			ds.mu.Unlock()

			if p != nil && e == nil {
				// Follower gets a NEW connection to the same proxy
				start := time.Now()
				newConn, connectTime, dialErr := singleDial(followerCtx, p, metadata, start)
				if dialErr == nil {
					rt.UpdateLatency(key, domain, p.Name(), connectTime)
					return p, newConn, connectTime, nil
				}

				// A winner is only a hint for followers: its next connection can
				// still fail because of a transient node error or a concurrency
				// limit. Penalize node-level failures and continue discovery with
				// the remaining candidates instead of failing the request outright.
				if err := followerCtx.Err(); err != nil {
					return nil, nil, 0, err
				}
				if tunnel.ShouldStopRetry(dialErr) || errors.Is(dialErr, context.Canceled) {
					return nil, nil, 0, dialErr
				}
				rt.MarkFailed(key, p.Name(), domain, 1.0)

				remainingNames := make([]string, 0, len(preRanked))
				for _, name := range preRanked {
					if name != p.Name() {
						remainingNames = append(remainingNames, name)
					}
				}
				if len(remainingNames) == 0 {
					return nil, nil, 0, dialErr
				}
				fallback := pc.probeBatch(followerCtx, key, proxies, metadata, remainingNames, singleDial, rt)
				return fallback.proxy, fallback.conn, fallback.connectTime, fallback.err
			}
			return nil, nil, 0, e
		case <-followerCtx.Done():
			return nil, nil, 0, followerCtx.Err()
		}
	}

	// Leader path: create discovery state
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	ds = &discoveryState{
		done:         make(chan struct{}),
		leaderCtx:    leaderCtx,
		leaderCancel: leaderCancel,
	}
	pc.discoveries[discoveryID] = ds
	pc.wg.Add(1)
	pc.mu.Unlock()

	defer func() {
		// leaderCancel is not called here: raceStaggered owns cancellation, so
		// in-flight loser dials keep sampling their connectTime after the winner
		// returns.  leaderCtx is only canceled by Close().
		pc.mu.Lock()
		delete(pc.discoveries, discoveryID)
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
) dialResult {
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

		var failMu sync.Mutex
		var fatalErr error

		winner, conn, connectTime, err := raceStaggered(ctx, batch, &pc.wg, smartTCPFallbackStagger,
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
				rt.UpdateLatency(key, routeDomain(metadata), proxyName, connectTime)
			},
			// onFail: classify node-level vs fatal/cancellation.  Only proxies
			// with node-level errors are penalized (MarkFailed 1.0); fatal
			// target-level errors and cancellations are not the proxy's fault.
			func(p C.Proxy, dialErr error) {
				// The parent request or coordinator may have expired while a dial
				// result was becoming ready. Check the parent first so a select race
				// cannot turn caller cancellation/deadline into a node penalty.
				if ctx.Err() != nil {
					return
				}
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
				rt.MarkFailed(key, p.Name(), routeDomain(metadata), 1.0)
			},
			// onWinner: discovery has no distinguished first candidate; every
			// winner belongs to the discovery race.
			func(proxy C.Proxy, connectTime int64) {
				log.Infoln("[Smart] route key=%s routed via %s (%dms, %s)", key, proxy.Name(), connectTime, smartDiscoveryTag)
			},
		)

		if err == nil && conn != nil {
			return dialResult{proxy: winner, conn: conn, connectTime: connectTime}
		}

		// No winner.  Fatal errors were captured live in onFail; node-level
		// failures were penalized via MarkFailed there too.
		log.Infoln("[Smart] probeBatch key=%s batch-ALL-FAILED err=%v", key, err)

		failMu.Lock()
		fe := fatalErr
		failMu.Unlock()

		if fe != nil {
			return dialResult{err: fe}
		}

		// If ctx is done, stop
		if ctx.Err() != nil {
			return dialResult{err: ctx.Err()}
		}

		// All dials failed with node-level errors — continue to the next batch.
	}

	return dialResult{err: fmt.Errorf("all %d proxies failed for key=%s", n, key)}
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
