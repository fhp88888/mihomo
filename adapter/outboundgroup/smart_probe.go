package outboundgroup

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
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
// this goroutine becomes the leader and probes the proxies sequentially,
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

	// Probe proxies sequentially in pre-ranked order
	proxy := pc.probeSequential(leaderCtx, key, proxies, metadata, preRanked, singleDial, rt)

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

// probeSequential tries proxies in the given order (pre-ranked by SecondaryRank).
// Returns the first successful connection. Failed proxies are marked in the
// route table and skipped.
func (pc *ProbeCoordinator) probeSequential(
	ctx context.Context,
	key string,
	proxies []C.Proxy,
	metadata *C.Metadata,
	ranked []string,
	singleDial func(context.Context, C.Proxy, *C.Metadata, time.Time) (C.Conn, int64, error),
	rt *smart.RouteTable,
) probeResult {
	proxyMap := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		proxyMap[p.Name()] = p
	}

	for _, name := range ranked {
		if ctx.Err() != nil {
			return probeResult{err: ctx.Err()}
		}

		p, ok := proxyMap[name]
		if !ok {
			continue
		}

		start := time.Now()
		conn, connectTime, err := singleDial(ctx, p, metadata, start)
		if err != nil {
			rt.MarkFailed(key, name)
			if tunnel.ShouldStopRetry(err) {
				return probeResult{err: err}
			}
			continue
		}

		rt.UpdateLatency(key, name, connectTime)
		return probeResult{proxy: p, conn: conn, connectTime: connectTime}
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
