package outboundgroup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
)

type manualDeadlineContext struct {
	done    chan struct{}
	expired atomic.Bool
}

func newManualDeadlineContext() *manualDeadlineContext {
	return &manualDeadlineContext{done: make(chan struct{})}
}

func (c *manualDeadlineContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *manualDeadlineContext) Done() <-chan struct{}       { return c.done }
func (c *manualDeadlineContext) Err() error {
	if c.expired.Load() {
		return context.DeadlineExceeded
	}
	return nil
}
func (c *manualDeadlineContext) Value(any) any { return nil }
func (c *manualDeadlineContext) expire() {
	if c.expired.CompareAndSwap(false, true) {
		close(c.done)
	}
}

func TestDiscoverSeparatesDomains(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rt := smart.NewRouteTable(10)
	const key = "ASN:13335 Cloudflare"
	started := make(chan struct{})
	leaderDone := make(chan error, 1)
	proxies := makeStubProxies("a", "b")
	go func() {
		_, _, _, err := pc.Discover(ctx, key, proxies, &C.Metadata{Host: "a.example.com"}, []string{"a"},
			func(ctx context.Context, _ C.Proxy, _ *C.Metadata, _ time.Time) (C.Conn, int64, error) {
				close(started)
				<-ctx.Done()
				return nil, 0, ctx.Err()
			}, rt)
		leaderDone <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first domain did not start")
	}
	conn := &stubConn{}
	p, got, _, err := pc.Discover(ctx, key, proxies, &C.Metadata{Host: "b.example.com"}, []string{"b"},
		func(_ context.Context, p C.Proxy, _ *C.Metadata, _ time.Time) (C.Conn, int64, error) {
			if p.Name() != "b" {
				return nil, 0, errors.New("wrong domain's proxy")
			}
			return conn, 80, nil
		}, rt)
	if err != nil || p == nil || p.Name() != "b" || got != conn {
		t.Fatalf("second domain did not discover independently: proxy=%v conn=%v err=%v", p, got, err)
	}
	got.Close()
	cancel()
	<-leaderDone
	for _, row := range rt.Snapshot("").Rows {
		if row.Key == key && domainRecords(row, routeDomain(&C.Metadata{Host: "b.example.com"}))["b"].Attributes.Latency == 80 {
			return
		}
	}
	t.Fatal("second domain latency was not recorded")
}

func TestDiscoverFollowerRecordsOwnLatency(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			pc := NewProbeCoordinator()
			defer pc.Close()
			rt := smart.NewRouteTable(10)
			const key = "ASN:13335 Cloudflare"
			metadata := &C.Metadata{Host: "a.example.com"}
			domain := routeDomain(metadata)
			proxies := makeStubProxies("leader", "alternative")
			// Seed a completed leader still available to joining followers. Their
			// preferred order differs, so using leader proves same-domain merging.
			ds := &discoveryState{done: make(chan struct{}), proxy: proxies[0], leaderCancel: func() {}}
			close(ds.done)
			pc.discoveries[discoveryKey{routeKey: key, domain: domain}] = ds
			rt.UpdateLatency(key, domain, "leader", 100)
			conn := &stubConn{}
			fallbackConn := &stubConn{}
			dialErr := errors.New("follower dial failed")
			calls := 0
			p, got, latency, err := pc.Discover(context.Background(), key, proxies, metadata, []string{"alternative"},
				func(_ context.Context, p C.Proxy, m *C.Metadata, _ time.Time) (C.Conn, int64, error) {
					calls++
					if m != metadata {
						t.Fatal("follower did not dial with its own metadata")
					}
					if fail && p == proxies[0] {
						return nil, 300, dialErr
					}
					if p == proxies[1] {
						return fallbackConn, 80, nil
					}
					return conn, 300, nil
				}, rt)
			wantCalls := 1
			if fail {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("dial calls = %d, want %d", calls, wantCalls)
			}
			wantLatency := int64(150) // 100*0.75 + 300*0.25, exactly one new sample.
			if fail {
				wantLatency = 100
				if err != nil || p != proxies[1] || got != fallbackConn || latency != 80 {
					t.Fatalf("follower did not fall back: proxy=%v conn=%v latency=%d err=%v", p, got, latency, err)
				}
				if failed := rt.ProxyFailedCount(key, domain, "leader"); failed != 1 {
					t.Fatalf("leader failure not recorded: %v", failed)
				}
				got.Close()
			} else {
				if err != nil || p != proxies[0] || got != conn || latency != 300 {
					t.Fatalf("unexpected follower result: %v %v %d %v", p, got, latency, err)
				}
				got.Close()
			}
			rows := rt.Snapshot("").Rows
			actual := domainRecords(rows[0], domain)["leader"].Attributes.Latency
			if actual != wantLatency {
				t.Fatalf("latency = %d, want %d", actual, wantLatency)
			}
		})
	}
}

func TestDiscoverFollowerCallerDeadlineIsNotNodeFailure(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(10)
	const key = "TARGET:deadline.example.com"
	metadata := &C.Metadata{Host: "deadline.example.com"}
	domain := routeDomain(metadata)
	proxies := makeStubProxies("leader", "alternative")
	ds := &discoveryState{done: make(chan struct{}), proxy: proxies[0], leaderCancel: func() {}}
	close(ds.done)
	pc.discoveries[discoveryKey{routeKey: key, domain: domain}] = ds
	rt.UpdateLatency(key, domain, "leader", 100)

	ctx := newManualDeadlineContext()
	dialStarted := make(chan struct{})
	discoverDone := make(chan error, 1)
	calls := 0
	go func() {
		_, _, _, err := pc.Discover(ctx, key, proxies, metadata, []string{"alternative"},
			func(ctx context.Context, _ C.Proxy, _ *C.Metadata, _ time.Time) (C.Conn, int64, error) {
				calls++
				close(dialStarted)
				<-ctx.Done()
				return nil, 1, ctx.Err()
			}, rt)
		discoverDone <- err
	}()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("follower redial did not start")
	}
	ctx.expire()
	var err error
	select {
	case err = <-discoverDone:
	case <-time.After(time.Second):
		t.Fatal("follower did not return after deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want caller deadline", err)
	}
	if calls != 1 {
		t.Fatalf("dial calls = %d, want no fallback after caller deadline", calls)
	}
	if failed := rt.ProxyFailedCount(key, domain, "leader"); failed != 0 {
		t.Fatalf("caller deadline counted as node failure: %v", failed)
	}
}

func TestDiscoverFollowerFallbackIsClosedWithCoordinator(t *testing.T) {
	pc := NewProbeCoordinator()
	rt := smart.NewRouteTable(10)
	const key = "TARGET:close.example.com"
	metadata := &C.Metadata{Host: "close.example.com"}
	domain := routeDomain(metadata)
	proxies := makeStubProxies("leader", "alternative")
	ds := &discoveryState{done: make(chan struct{}), proxy: proxies[0], leaderCancel: func() {}}
	close(ds.done)
	pc.discoveries[discoveryKey{routeKey: key, domain: domain}] = ds
	rt.UpdateLatency(key, domain, "leader", 100)

	fallbackStarted := make(chan struct{})
	discoverDone := make(chan error, 1)
	go func() {
		_, _, _, err := pc.Discover(context.Background(), key, proxies, metadata, []string{"alternative"},
			func(ctx context.Context, p C.Proxy, _ *C.Metadata, _ time.Time) (C.Conn, int64, error) {
				if p == proxies[0] {
					return nil, 1, errors.New("leader redial failed")
				}
				close(fallbackStarted)
				<-ctx.Done()
				return nil, 1, ctx.Err()
			}, rt)
		discoverDone <- err
	}()

	select {
	case <-fallbackStarted:
	case <-time.After(time.Second):
		t.Fatal("follower fallback did not start")
	}
	if err := pc.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-discoverDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("follower error = %v, want coordinator cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("follower did not return after Close canceled its fallback")
	}
}

func TestDiscoverFollowerFallbackDeadlineIsNotNodeFailure(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(10)
	const key = "TARGET:fallback-deadline.example.com"
	metadata := &C.Metadata{Host: "fallback-deadline.example.com"}
	domain := routeDomain(metadata)
	proxies := makeStubProxies("leader", "alternative")
	ds := &discoveryState{done: make(chan struct{}), proxy: proxies[0], leaderCancel: func() {}}
	close(ds.done)
	pc.discoveries[discoveryKey{routeKey: key, domain: domain}] = ds
	rt.UpdateLatency(key, domain, "leader", 100)

	ctx := newManualDeadlineContext()
	fallbackStarted := make(chan struct{})
	discoverDone := make(chan error, 1)
	go func() {
		_, _, _, err := pc.Discover(ctx, key, proxies, metadata, []string{"alternative"},
			func(ctx context.Context, p C.Proxy, _ *C.Metadata, _ time.Time) (C.Conn, int64, error) {
				if p == proxies[0] {
					return nil, 1, errors.New("leader redial failed")
				}
				close(fallbackStarted)
				<-ctx.Done()
				return nil, 1, ctx.Err()
			}, rt)
		discoverDone <- err
	}()
	select {
	case <-fallbackStarted:
	case <-time.After(time.Second):
		t.Fatal("follower fallback did not start")
	}
	ctx.expire()
	var err error
	select {
	case err = <-discoverDone:
	case <-time.After(time.Second):
		t.Fatal("follower fallback did not return after deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want caller deadline", err)
	}
	if failed := rt.ProxyFailedCount(key, domain, "leader"); failed != 1 {
		t.Fatalf("leader node failure = %v, want 1", failed)
	}
	if failed := rt.ProxyFailedCount(key, domain, "alternative"); failed != 0 {
		t.Fatalf("fallback caller deadline counted as node failure: %v", failed)
	}
}
