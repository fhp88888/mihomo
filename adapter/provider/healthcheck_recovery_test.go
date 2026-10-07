package provider

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

type healthTestProxy struct {
	C.Proxy
	name  string
	mu    sync.Mutex
	alive map[string]bool
	test  func(context.Context, string) bool
}

func (p *healthTestProxy) Name() string { return p.name }
func (p *healthTestProxy) AliveForTestUrl(url string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.alive[url]
}
func (p *healthTestProxy) LastDelayForTestUrl(string) uint16 { return 1 }
func (p *healthTestProxy) URLTest(ctx context.Context, url string, _ utils.IntRanges[uint16]) (uint16, error) {
	alive := p.test(ctx, url)
	p.mu.Lock()
	if p.alive == nil {
		p.alive = make(map[string]bool)
	}
	p.alive[url] = alive
	p.mu.Unlock()
	return 1, nil
}

func TestHealthRetryDelay(t *testing.T) {
	delay := 10 * time.Second
	for _, seconds := range []time.Duration{20, 40, 80, 160, 300, 300} {
		delay = nextHealthRetryDelay(delay, 5*time.Minute)
		if delay != seconds*time.Second {
			t.Fatalf("delay = %v", delay)
		}
	}
}

func TestHealthRecoveryMergesRequestsAndPeriodicChecks(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	p := &healthTestProxy{name: "p", test: func(ctx context.Context, _ string) bool {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return false
	}}
	hc := NewHealthCheck([]C.Proxy{p}, "url", 1000, 0, false, nil)
	hc.retryInitial, hc.retryMaximum = time.Hour, time.Hour
	defer hc.close()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = hc.recover(context.Background(), "url") }()
	}
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	joined := make(chan error, 1)
	go func() { joined <- hc.recover(ctx, "url") }()
	cancel()
	if err := <-joined; err != context.Canceled {
		t.Fatalf("cancel = %v", err)
	}
	close(release)
	wg.Wait()
	for i := 0; i < 100; i++ {
		hc.check()
		_ = hc.recover(context.Background(), "url")
	}
	if calls.Load() != 1 {
		t.Fatalf("checks = %d, want one throughout backoff", calls.Load())
	}
}

func TestHealthRecoveryUsesRecentPeriodicResult(t *testing.T) {
	var calls atomic.Int32
	p := &healthTestProxy{name: "p", test: func(context.Context, string) bool { calls.Add(1); return false }}
	hc := NewHealthCheck([]C.Proxy{p}, "url", 1000, 0, false, nil)
	hc.retryInitial, hc.retryMaximum = time.Hour, time.Hour
	defer hc.close()
	hc.check()
	_ = hc.recover(context.Background(), "url")
	if calls.Load() != 1 {
		t.Fatal("recovery duplicated just-completed periodic check")
	}
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if !hc.tasks["url"].recovering || hc.tasks["url"].timer == nil {
		t.Fatal("failed result did not start background recovery")
	}
}

func TestHealthRecoveryRetriesWithoutTrafficAndResets(t *testing.T) {
	checks := make(chan time.Time, 10)
	var calls atomic.Int32
	p := &healthTestProxy{name: "p", test: func(context.Context, string) bool {
		n := calls.Add(1)
		checks <- time.Now()
		return n == 4
	}}
	hc := NewHealthCheck([]C.Proxy{p}, "url", 1000, 0, true, nil)
	hc.retryInitial, hc.retryMaximum = 15*time.Millisecond, 30*time.Millisecond
	defer hc.close()
	_ = hc.recover(context.Background(), "url")
	var previous time.Time
	for i := 0; i < 4; i++ {
		select {
		case now := <-checks:
			minimum := 15 * time.Millisecond
			if i > 1 {
				minimum = 30 * time.Millisecond
			}
			if i > 0 && now.Sub(previous) < minimum {
				t.Fatalf("retry too early: %v", now.Sub(previous))
			}
			previous = now
		case <-time.After(time.Second):
			t.Fatal("no background retry")
		}
	}
	// Join completion before inspecting reset state.
	_ = hc.recover(context.Background(), "url")
	hc.mu.Lock()
	if hc.tasks["url"].recovering || hc.tasks["url"].delay != 0 {
		t.Error("successful check did not reset recovery")
	}
	hc.mu.Unlock()
	p.mu.Lock()
	p.alive["url"] = false
	p.mu.Unlock()
	_ = hc.recover(context.Background(), "url")
	hc.mu.Lock()
	if hc.tasks["url"].delay != hc.retryInitial {
		t.Error("new outage did not reset initial delay")
	}
	hc.mu.Unlock()
}

func TestHealthRecoveryScopesURLsAndFilters(t *testing.T) {
	var unexpected atomic.Int32
	a := &healthTestProxy{name: "a", test: func(context.Context, string) bool { return false }}
	b := &healthTestProxy{name: "b", alive: map[string]bool{"failed": true}, test: func(context.Context, string) bool { unexpected.Add(1); return true }}
	hc := NewHealthCheck([]C.Proxy{a, b}, "default", 1000, 0, false, nil)
	hc.registerHealthCheckTask("failed", nil, "^a$", 0)
	hc.registerHealthCheckTask("healthy", nil, "^b$", 0)
	hc.retryInitial, hc.retryMaximum = time.Hour, time.Hour
	defer hc.close()
	_ = hc.recover(context.Background(), "failed")
	_ = hc.recover(context.Background(), "healthy")
	_ = hc.recover(context.Background(), "unregistered")
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if !hc.tasks["failed"].recovering {
		t.Fatal("healthy excluded node suppressed recovery")
	}
	if hc.tasks["healthy"].recovering {
		t.Fatal("one failed URL contaminated healthy URL")
	}
	if _, ok := hc.tasks["unregistered"]; ok {
		t.Fatal("unregistered URL must not create a task")
	}
	if unexpected.Load() != 1 {
		t.Fatalf("filtered node checked %d times", unexpected.Load())
	}
}

func TestHealthCheckGlobalBudgetAndClose(t *testing.T) {
	var running, peak atomic.Int32
	started := make(chan struct{}, 30)
	var checks []*HealthCheck
	for i := 0; i < 20; i++ {
		p := &healthTestProxy{name: fmt.Sprint(i), test: func(ctx context.Context, _ string) bool {
			n := running.Add(1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			started <- struct{}{}
			<-ctx.Done()
			running.Add(-1)
			return false
		}}
		hc := NewHealthCheck([]C.Proxy{p}, "url", 5000, 0, false, nil)
		checks = append(checks, hc)
		hc.requestCheck("url", true)
	}
	for i := 0; i < 10; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("budget did not fill")
		}
	}
	var wg sync.WaitGroup
	for _, hc := range checks {
		wg.Add(1)
		go func(hc *HealthCheck) { defer wg.Done(); hc.close() }(hc)
	}
	wg.Wait()
	if peak.Load() > 10 || running.Load() != 0 {
		t.Fatalf("peak=%d remaining=%d", peak.Load(), running.Load())
	}
	for _, hc := range checks {
		if hc.requestCheck("url", true) != nil {
			t.Fatal("closed provider scheduled a check")
		}
	}
}

func TestHealthRecoveryForGroupSubsetIgnoresHealthyOutsideNodes(t *testing.T) {
	var calls atomic.Int32
	a := &healthTestProxy{name: "a", test: func(context.Context, string) bool { calls.Add(1); return false }}
	b := &healthTestProxy{name: "b", alive: map[string]bool{"url": true}, test: func(context.Context, string) bool { return true }}
	hc := NewHealthCheck([]C.Proxy{a, b}, "url", 1000, 0, false, nil)
	hc.retryInitial, hc.retryMaximum = time.Hour, time.Hour
	defer hc.close()
	_ = hc.recover(context.Background(), "url", "a")
	if calls.Load() != 1 {
		t.Fatal("healthy outside node prevented group recovery check")
	}
	hc.mu.Lock()
	if !hc.tasks["url"].recovering || hc.tasks["url"].timer == nil {
		t.Error("healthy outside node stopped background recovery")
	}
	hc.mu.Unlock()
	_ = hc.recover(context.Background(), "url", "b")
	if calls.Load() != 1 {
		t.Fatal("healthy group bypassed failing group's backoff")
	}
	a.mu.Lock()
	a.alive["url"] = true
	a.mu.Unlock()
	_ = hc.recover(context.Background(), "url", "a")
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if hc.tasks["url"].recovering || hc.tasks["url"].delay != 0 {
		t.Fatal("subset recovery did not reset backoff")
	}
}
