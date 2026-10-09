package outboundgroup

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
)

// stubProxy implements C.Proxy with minimal behavior for testing.
type stubProxy struct {
	name  string
	delay uint16
	dial  func(context.Context, *C.Metadata) (C.Conn, error)
}

type nilPacketProxy struct{ *stubProxy }

func (p *nilPacketProxy) SupportUDP() bool { return true }
func (p *nilPacketProxy) ListenPacketContext(context.Context, *C.Metadata) (C.PacketConn, error) {
	return nil, nil
}

type udpErrorProxy struct {
	*stubProxy
	calls int
}

func (p *udpErrorProxy) SupportUDP() bool { return true }
func (p *udpErrorProxy) ListenPacketContext(context.Context, *C.Metadata) (C.PacketConn, error) {
	p.calls++
	return nil, errors.New("udp dial failed")
}

func (s *stubProxy) Name() string           { return s.name }
func (s *stubProxy) Type() C.AdapterType    { return C.Direct }
func (s *stubProxy) Addr() string           { return "" }
func (s *stubProxy) SupportUDP() bool       { return false }
func (s *stubProxy) ProxyInfo() C.ProxyInfo { return C.ProxyInfo{} }
func (s *stubProxy) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.name + `"`), nil
}
func (s *stubProxy) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if s.dial != nil {
		return s.dial(ctx, metadata)
	}
	return nil, errors.New("stub: DialContext not implemented")
}
func (s *stubProxy) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	return nil, errors.New("stub: ListenPacketContext not implemented")
}
func (s *stubProxy) SupportUOT() bool                                   { return false }
func (s *stubProxy) IsL3Protocol(metadata *C.Metadata) bool             { return false }
func (s *stubProxy) Unwrap(metadata *C.Metadata, touch bool) C.Proxy    { return nil }
func (s *stubProxy) Close() error                                       { return nil }
func (s *stubProxy) Adapter() C.ProxyAdapter                            { return s }
func (s *stubProxy) AliveForTestUrl(url string) bool                    { return true }
func (s *stubProxy) DelayHistory() []C.DelayHistory                     { return nil }
func (s *stubProxy) DelayHistoryForTestUrl(url string) []C.DelayHistory { return nil }
func (s *stubProxy) ExtraDelayHistories() map[string]C.ProxyState       { return nil }
func (s *stubProxy) LastDelayForTestUrl(url string) uint16              { return s.delay }
func (s *stubProxy) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (uint16, error) {
	return 0, nil
}
func (s *stubProxy) StatusTest(ctx context.Context, url string) (status uint16, ok bool, err error) {
	return 0, false, nil
}

var _ C.Proxy = (*stubProxy)(nil)

// stubConn is a minimal C.Conn for testing successful dial results.
type stubConn struct {
	mu     sync.Mutex
	closes int
}

func (s *stubConn) Read(b []byte) (n int, err error)  { return 0, io.EOF }
func (s *stubConn) Write(b []byte) (n int, err error) { return len(b), nil }
func (s *stubConn) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	return nil
}
func (s *stubConn) CloseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}
func (s *stubConn) LocalAddr() net.Addr                   { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0} }
func (s *stubConn) RemoteAddr() net.Addr                  { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443} }
func (s *stubConn) SetDeadline(t time.Time) error         { return nil }
func (s *stubConn) SetReadDeadline(t time.Time) error     { return nil }
func (s *stubConn) SetWriteDeadline(t time.Time) error    { return nil }
func (s *stubConn) ReadBuffer(buffer *buf.Buffer) error   { return io.EOF }
func (s *stubConn) WriteBuffer(buffer *buf.Buffer) error  { return nil }
func (s *stubConn) Upstream() any                         { return nil }
func (s *stubConn) NeedHandshake() bool                   { return false }
func (s *stubConn) ReaderReplaceable() bool               { return false }
func (s *stubConn) WriterReplaceable() bool               { return false }
func (s *stubConn) Chains() C.Chain                       { return nil }
func (s *stubConn) ProviderChains() C.Chain               { return nil }
func (s *stubConn) AppendToChains(adapter C.ProxyAdapter) {}
func (s *stubConn) RemoteDestination() string             { return "" }

var _ C.Conn = (*stubConn)(nil)

// domainRecords returns the ProxyRecord map for a named domain within a row
// snapshot (nil if the domain isn't present).  Proxy metrics live per domain,
// not per row, so tests that used to read row.Proxies directly go through this.
func domainRecords(row smart.RowSnapshot, domain string) map[string]smart.ProxyRecord {
	for _, d := range row.Domains {
		if d.Name == domain {
			return d.Proxies
		}
	}
	return nil
}

func routeFailedCount(t *testing.T, table *smart.RouteTable, key, domain, proxy string) float64 {
	t.Helper()
	for _, row := range table.Snapshot("").Rows {
		if row.Key != key {
			continue
		}
		for _, dom := range row.Domains {
			if dom.Name == domain {
				return dom.Proxies[proxy].Attributes.FailedCount
			}
		}
		// Domain has no recorded state yet — equivalent to FailedCount 0.
		return 0
	}
	t.Fatalf("route row %q not found", key)
	return 0
}

func TestRaceAndWrap_FirstSuccessCancelsLosers(t *testing.T) {
	const key = "TARGET:example.com"
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	winner := &stubConn{}

	first := &stubProxy{name: "first", delay: 10, dial: func(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
		close(firstStarted)
		<-ctx.Done()
		close(firstCanceled)
		return nil, ctx.Err()
	}}
	second := &stubProxy{name: "second", delay: 20, dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		close(secondStarted)
		return winner, nil
	}}
	third := &stubProxy{name: "third", delay: 30, dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		t.Error("third proxy should not be launched after second succeeds")
		return nil, errors.New("unexpected third dial")
	}}

	table := smart.NewRouteTable(10)
	table.UpdateLatency(key, "example.com", first.Name(), 10)
	table.SetBestProxy(key, "example.com", first.Name())
	s := &Smart{testUrl: "test", routeTable: table}

	result := make(chan struct {
		conn C.Conn
		err  error
	}, 1)
	go func() {
		conn, err := s.raceAndWrap(context.Background(), &C.Metadata{Host: "example.com"}, key, "example.com",
			[]C.Proxy{first, second, third}, smartTCPFallbackStagger, nil, "")
		result <- struct {
			conn C.Conn
			err  error
		}{conn, err}
	}()

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first proxy did not start")
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second proxy did not start after stagger interval")
	}

	var got struct {
		conn C.Conn
		err  error
	}
	select {
	case got = <-result:
	case <-time.After(time.Second):
		t.Fatal("staggered fallback did not return after second succeeded")
	}
	if got.err != nil {
		t.Fatalf("staggered fallback returned error: %v", got.err)
	}
	if got.conn == nil {
		t.Fatal("staggered fallback returned nil connection")
	}
	select {
	case <-firstCanceled:
	case <-time.After(time.Second):
		t.Fatal("first proxy did not observe cancellation")
	}
	if got := routeFailedCount(t, table, key, "example.com", first.Name()); got != 0 {
		t.Fatalf("canceled first proxy failed count = %v, want 0", got)
	}
	if winner.CloseCount() != 0 {
		t.Fatalf("winner was closed %d times", winner.CloseCount())
	}
	_ = got.conn.Close()
}

func TestRaceAndWrap_ClosesLateSuccessfulLoser(t *testing.T) {
	const key = "TARGET:example.com"
	firstStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	late := &stubConn{}
	winner := &stubConn{}

	first := &stubProxy{name: "first", dial: func(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
		close(firstStarted)
		<-ctx.Done()
		close(firstCanceled)
		return late, nil
	}}
	second := &stubProxy{name: "second", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		return winner, nil
	}}

	s := &Smart{testUrl: "test", routeTable: smart.NewRouteTable(10)}
	result := make(chan struct {
		conn C.Conn
		err  error
	}, 1)
	go func() {
		conn, err := s.raceAndWrap(context.Background(), &C.Metadata{Host: "example.com"}, key, "example.com",
			[]C.Proxy{first, second}, smartTCPFallbackStagger, nil, "")
		result <- struct {
			conn C.Conn
			err  error
		}{conn, err}
	}()

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first proxy did not start")
	}
	var got struct {
		conn C.Conn
		err  error
	}
	select {
	case got = <-result:
	case <-time.After(time.Second):
		t.Fatal("staggered fallback did not return")
	}
	if got.err != nil || got.conn == nil {
		t.Fatalf("winner result = (%v, %v), want non-nil second connection and nil error", got.conn, got.err)
	}
	select {
	case <-firstCanceled:
	case <-time.After(time.Second):
		t.Fatal("first proxy did not observe cancellation")
	}
	if late.CloseCount() != 1 {
		t.Fatalf("late successful loser was closed %d times, want 1", late.CloseCount())
	}
	if winner.CloseCount() != 0 {
		t.Fatalf("winner was closed %d times", winner.CloseCount())
	}
	if best, ok := s.routeTable.GetBestProxy(key, "example.com"); !ok || best != second.Name() {
		t.Fatalf("best proxy = %q ok=%v, want selected winner %q", best, ok, second.Name())
	}
	if !s.routeTable.IsTCPProbed(key, "example.com") {
		t.Fatal("selected winner did not mark route TCP-probed")
	}
	var firstUseCount, secondUseCount int64
	for _, row := range s.routeTable.Snapshot("").Rows {
		if row.Key == key {
			proxies := domainRecords(row, "example.com")
			firstUseCount = proxies[first.Name()].UseCount
			secondUseCount = proxies[second.Name()].UseCount
			break
		}
	}
	if firstUseCount != 0 {
		t.Fatalf("late successful loser use count = %d, want 0", firstUseCount)
	}
	if secondUseCount != 1 {
		t.Fatalf("winner use count = %d, want 1", secondUseCount)
	}
	_ = got.conn.Close()
}

func TestRaceAndWrap_FatalErrorStopsScheduling(t *testing.T) {
	const key = "TARGET:example.com"
	secondStarted := make(chan struct{}, 1)
	first := &stubProxy{name: "first", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		return nil, resolver.ErrIPNotFound
	}}
	second := &stubProxy{name: "second", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		secondStarted <- struct{}{}
		return nil, errors.New("unexpected second dial")
	}}

	table := smart.NewRouteTable(10)
	table.UpdateLatency(key, "example.com", first.Name(), 10)
	table.SetBestProxy(key, "example.com", first.Name())
	s := &Smart{testUrl: "test", routeTable: table}
	_, err := s.raceAndWrap(context.Background(), &C.Metadata{Host: "example.com"}, key, "example.com",
		[]C.Proxy{first, second}, smartTCPFallbackStagger, nil, "")
	if !errors.Is(err, resolver.ErrIPNotFound) || !tunnel.ShouldStopRetry(err) {
		t.Fatalf("fatal error = %v, want ErrIPNotFound", err)
	}
	select {
	case <-secondStarted:
		t.Fatal("second proxy started after fatal first result")
	case <-time.After(2 * smartTCPFallbackStagger):
	}
	if best, ok := table.GetBestProxy(key, "example.com"); !ok || best != first.Name() {
		t.Fatalf("fatal error cleared best proxy: best=%q ok=%v", best, ok)
	}
}

func TestRaceAndWrap_ParentCancellationStopsScheduling(t *testing.T) {
	const key = "TARGET:example.com"
	firstStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	secondStarted := make(chan struct{}, 1)
	first := &stubProxy{name: "first", dial: func(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
		close(firstStarted)
		<-ctx.Done()
		close(firstCanceled)
		return nil, ctx.Err()
	}}
	second := &stubProxy{name: "second", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		secondStarted <- struct{}{}
		return nil, errors.New("unexpected second dial")
	}}

	table := smart.NewRouteTable(10)
	table.UpdateLatency(key, "example.com", first.Name(), 10)
	table.SetBestProxy(key, "example.com", first.Name())
	s := &Smart{testUrl: "test", routeTable: table}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := s.raceAndWrap(ctx, &C.Metadata{Host: "example.com"}, key, "example.com",
			[]C.Proxy{first, second}, smartTCPFallbackStagger, nil, "")
		result <- err
	}()

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first proxy did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("parent cancellation error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("staggered fallback did not return on parent cancellation")
	}
	select {
	case <-firstCanceled:
	case <-time.After(time.Second):
		t.Fatal("first proxy did not observe parent cancellation")
	}
	select {
	case <-secondStarted:
		t.Fatal("second proxy started after parent cancellation")
	case <-time.After(2 * smartTCPFallbackStagger):
	}
	if got := routeFailedCount(t, table, key, "example.com", first.Name()); got != 0 {
		t.Fatalf("canceled first proxy failed count = %v, want 0", got)
	}
}

// =========================================================================
// best-first race tests (serialTcpConn fast-path)
// =========================================================================

func newBestRaceSmart() (*Smart, *smart.RouteTable, *smartRaceCoordinator) {
	rt := smart.NewRouteTable(10)
	pc := newSmartRaceCoordinator()
	return &Smart{testUrl: "test", routeTable: rt, raceCoordinator: pc}, rt, pc
}

// bestProxy returns a stub proxy whose dial blocks until unblock is closed or
// returns conn immediately when conn is non-nil.  firstCalled is closed on the
// first dial.
func blockingProxy(name string, unblock chan struct{}, conn *stubConn, firstCalled chan struct{}) *stubProxy {
	var once sync.Once
	return &stubProxy{name: name, dial: func(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
		once.Do(func() { close(firstCalled) })
		select {
		case <-unblock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return conn, nil
	}}
}

func TestBestFirstRace_BestWinsWithinWindow(t *testing.T) {
	const key = "TARGET:example.com"
	s, rt, pc := newBestRaceSmart()
	defer pc.Close()

	bestStarted := make(chan struct{})
	bestConn := &stubConn{}
	best := &stubProxy{name: "best", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		close(bestStarted)
		return bestConn, nil
	}}
	other := &stubProxy{name: "other", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		t.Error("other proxy should not be dialed when best wins immediately")
		return nil, errors.New("unexpected other dial")
	}}

	rt.SetBestProxy(key, "example.com", "best")

	start := time.Now()
	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, "example.com", []C.Proxy{best, other})
	if err != nil {
		t.Fatalf("serialTcpConn error: %v", err)
	}
	if conn == nil {
		t.Fatal("serialTcpConn returned nil connection")
	}
	elapsed := time.Since(start)
	if elapsed > smartDefaultDialWindow {
		t.Fatalf("best win took %v, want within %v", elapsed, smartDefaultDialWindow)
	}
	if bestConn.CloseCount() != 0 {
		t.Fatalf("winner conn closed %d times", bestConn.CloseCount())
	}
	_ = conn.Close()
}

func TestAdaptiveDialWindow(t *testing.T) {
	const (
		key    = "TARGET:example.com"
		domain = "example.com"
	)
	s, rt, pc := newBestRaceSmart()
	defer pc.Close()

	if got := s.adaptiveDialWindow(key, domain, "unsampled"); got != smartDefaultDialWindow {
		t.Fatalf("unsampled window = %v, want %v", got, smartDefaultDialWindow)
	}
	rt.UpdateLatency(key, domain, "normal", 100)
	if got := s.adaptiveDialWindow(key, domain, "normal"); got != 200*time.Millisecond {
		t.Fatalf("sampled window = %v, want 200ms", got)
	}
	rt.UpdateLatency(key, domain, "tiny", 1)
	if got := s.adaptiveDialWindow(key, domain, "tiny"); got != smartMinDialWindow {
		t.Fatalf("minimum-clamped window = %v, want %v", got, smartMinDialWindow)
	}
	rt.UpdateLatency(key, domain, "huge", 2000)
	if got := s.adaptiveDialWindow(key, domain, "huge"); got != smartMaxDialWindow {
		t.Fatalf("maximum-clamped window = %v, want %v", got, smartMaxDialWindow)
	}
}

func TestBestFirstRace_StaleBestYieldsToFasterFallback(t *testing.T) {
	const key = "TARGET:example.com"
	s, rt, pc := newBestRaceSmart()
	defer pc.Close()

	bestStarted := make(chan struct{})
	bestUnblock := make(chan struct{})
	best := blockingProxy("best", bestUnblock, &stubConn{}, bestStarted)

	secondStarted := make(chan struct{})
	second := &stubProxy{name: "second", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		close(secondStarted)
		return &stubConn{}, nil
	}}

	rt.UpdateLatency(key, "example.com", "best", 100)
	rt.SetBestProxy(key, "example.com", "best")
	wantWindow := 150 * time.Millisecond

	start := time.Now()
	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, "example.com", []C.Proxy{best, second})
	if err != nil {
		t.Fatalf("serialTcpConn error: %v", err)
	}
	if conn == nil {
		t.Fatal("serialTcpConn returned nil connection")
	}

	// second must not be launched before the exclusive window elapses.
	select {
	case <-secondStarted:
		if time.Since(start) < wantWindow {
			t.Fatal("second launched before exclusive window elapsed")
		}
	case <-time.After(2 * wantWindow):
		t.Fatal("second proxy never launched")
	}

	// second wins the race; best is unblocked later and drained as a loser.
	bestUnblock <- struct{}{}
	pc.wg.Wait()

	got, _ := rt.GetBestProxy(key, "example.com")
	if got != "second" {
		t.Fatalf("best = %q, want second", got)
	}
	_ = conn.Close()
}

func TestBestFirstRace_BestFailEarlyStartsFallbackImmediately(t *testing.T) {
	const key = "TARGET:example.com"
	s, rt, pc := newBestRaceSmart()
	defer pc.Close()

	bestStarted := make(chan struct{})
	best := &stubProxy{name: "best", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		close(bestStarted)
		return nil, syscall.ECONNREFUSED
	}}
	secondStarted := make(chan struct{})
	second := &stubProxy{name: "second", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		close(secondStarted)
		return &stubConn{}, nil
	}}

	rt.SetBestProxy(key, "example.com", "best")

	start := time.Now()
	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, "example.com", []C.Proxy{best, second})
	if err != nil {
		t.Fatalf("serialTcpConn error: %v", err)
	}
	if conn == nil {
		t.Fatal("serialTcpConn returned nil connection")
	}

	// best fails immediately → second must launch well before 600ms elapses.
	select {
	case <-secondStarted:
		elapsed := time.Since(start)
		if elapsed >= smartDefaultDialWindow {
			t.Fatalf("second launched after %v, want immediate (best failed early)", elapsed)
		}
	case <-time.After(smartDefaultDialWindow):
		t.Fatal("second proxy never launched after best failed early")
	}

	got, _ := rt.GetBestProxy(key, "example.com")
	if got != "second" {
		t.Fatalf("best = %q, want second", got)
	}
	_ = conn.Close()
}

// =========================================================================
// debug-log verification for the best-first race policy
// =========================================================================

// collectLogs subscribes to log events; waitForLog polls until a log with the
// given prefix arrives or the deadline elapses.  waitAbsentLog polls for the
// absence of a prefix (used to assert a proxy was never dialed).
func collectLogs() (waitForLog func(timeout time.Duration, parts ...string) bool, stop func()) {
	sub := log.Subscribe()
	var mu sync.Mutex
	var logs []string
	done := make(chan struct{})
	go func() {
		for ev := range sub {
			mu.Lock()
			logs = append(logs, ev.Payload)
			mu.Unlock()
		}
		close(done)
	}()
	contains := func(parts ...string) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, l := range logs {
			matches := true
			for _, part := range parts {
				if !strings.Contains(l, part) {
					matches = false
					break
				}
			}
			if matches {
				return true
			}
		}
		return false
	}
	waitForLog = func(timeout time.Duration, parts ...string) bool {
		deadline := time.After(timeout)
		for {
			if contains(parts...) {
				return true
			}
			select {
			case <-deadline:
				return false
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	stop = func() { log.UnSubscribe(sub) }
	return
}

// TestSmartPolicy_LogSequence_BestWinsWithinWindow verifies that when best
// succeeds inside its exclusive window, the other proxies are never dialed.
func TestSmartPolicy_LogSequence_BestWinsWithinWindow(t *testing.T) {
	const key = "TARGET:example.com"
	waitForLog, stop := collectLogs()
	defer stop()

	s, rt, pc := newBestRaceSmart()
	defer pc.Close()

	best := &stubProxy{name: "best", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		return &stubConn{}, nil
	}}
	other := &stubProxy{name: "other", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		t.Error("other should not be dialed")
		return nil, errors.New("unexpected")
	}}
	rt.SetBestProxy(key, "example.com", "best")

	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, "example.com", []C.Proxy{best, other})
	if err != nil || conn == nil {
		t.Fatalf("serialTcpConn = (%v, %v), want conn", conn, err)
	}
	_ = conn.Close()

	if !waitForLog(time.Second, "routed via best", ", Best)") {
		t.Fatal("best winner did not use the Best tag")
	}
	if waitForLog(50*time.Millisecond, "Best#") {
		t.Fatal("best winner used a numbered Best tag")
	}
	if waitForLog(200*time.Millisecond, "routed via other") {
		t.Fatal("other dialed despite best winning")
	}
}

// TestSmartPolicy_LogSequence_BestWinsAfterFallbackStarts verifies that the
// log describes the winning connection, not merely the phase currently active.
func TestSmartPolicy_LogSequence_BestWinsAfterFallbackStarts(t *testing.T) {
	const key = "TARGET:example.com"
	waitForLog, stop := collectLogs()
	defer stop()

	s, rt, pc := newBestRaceSmart()
	defer pc.Close()

	bestStarted := make(chan struct{})
	bestUnblock := make(chan struct{})
	best := blockingProxy("best", bestUnblock, &stubConn{}, bestStarted)
	secondStarted := make(chan struct{})
	secondUnblock := make(chan struct{})
	second := blockingProxy("second", secondUnblock, &stubConn{}, secondStarted)
	rt.SetBestProxy(key, "example.com", "best")

	result := make(chan struct {
		conn C.Conn
		err  error
	}, 1)
	go func() {
		conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, "example.com", []C.Proxy{best, second})
		result <- struct {
			conn C.Conn
			err  error
		}{conn: conn, err: err}
	}()

	select {
	case <-secondStarted:
		// The exclusive window elapsed and the fallback is now in flight.
	case <-time.After(2 * smartDefaultDialWindow):
		t.Fatal("fallback did not start after the best exclusive window")
	}
	close(bestUnblock)

	var got struct {
		conn C.Conn
		err  error
	}
	select {
	case got = <-result:
	case <-time.After(time.Second):
		t.Fatal("best did not win after fallback started")
	}
	if got.err != nil || got.conn == nil {
		t.Fatalf("serialTcpConn = (%v, %v), want best connection", got.conn, got.err)
	}
	close(secondUnblock)
	pc.wg.Wait()
	_ = got.conn.Close()

	if !waitForLog(time.Second, "routed via best", ", Best)") {
		t.Fatal("late best winner did not use the Best tag")
	}
	if waitForLog(50*time.Millisecond, "routed via best", "Best#") {
		t.Fatal("late best winner used a numbered Best tag")
	}
}

// TestSmartPolicy_LogSequence_StaleBestFallsBackToRace verifies that when best
// stays blocked past the window, the race launches the next proxy and the
// winner is recorded.
func TestSmartPolicy_LogSequence_StaleBestFallsBackToRace(t *testing.T) {
	const key = "TARGET:example.com"
	waitForLog, stop := collectLogs()
	defer stop()

	s, rt, pc := newBestRaceSmart()
	defer pc.Close()

	bestStarted := make(chan struct{})
	bestUnblock := make(chan struct{})
	best := blockingProxy("best", bestUnblock, &stubConn{}, bestStarted)
	second := &stubProxy{name: "second", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		return &stubConn{}, nil
	}}
	rt.SetBestProxy(key, "example.com", "best")

	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, "example.com", []C.Proxy{best, second})
	if err != nil || conn == nil {
		t.Fatalf("serialTcpConn = (%v, %v), want conn", conn, err)
	}
	bestUnblock <- struct{}{}
	pc.wg.Wait() // let the background drain settle
	_ = conn.Close()

	if !waitForLog(time.Second, "routed via second", ", Stagger#1)") {
		t.Fatal("fallback winner did not use the Stagger#1 tag")
	}
}

// TestSmartPolicy_LogSequence_BestFailsEarlyStartsFallbackImmediately verifies
// that a fast best failure launches the next proxy without waiting for the
// full window, and best is later drained as a loser.
func TestSmartPolicy_LogSequence_BestFailsEarlyStartsFallbackImmediately(t *testing.T) {
	const key = "TARGET:example.com"
	waitForLog, stop := collectLogs()
	defer stop()

	s, rt, pc := newBestRaceSmart()
	defer pc.Close()

	best := &stubProxy{name: "best", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		return nil, syscall.ECONNREFUSED
	}}
	second := &stubProxy{name: "second", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		return &stubConn{}, nil
	}}
	rt.SetBestProxy(key, "example.com", "best")

	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, "example.com", []C.Proxy{best, second})
	if err != nil || conn == nil {
		t.Fatalf("serialTcpConn = (%v, %v), want conn", conn, err)
	}
	pc.wg.Wait()
	_ = conn.Close()

	if !waitForLog(time.Second, "dial best failed") {
		t.Fatal("best failure not logged")
	}
	if !waitForLog(time.Second, "routed via second", ", Stagger#1)") {
		t.Fatal("fallback winner did not use the Stagger#1 tag")
	}
}

func TestStaggerTagUsesCandidateOrder(t *testing.T) {
	best := &stubProxy{name: "best"}
	first := &stubProxy{name: "first"}
	second := &stubProxy{name: "second"}
	ordered := []C.Proxy{best, first, second}

	if got := staggerTag(ordered, "best", "best"); got != "Best" {
		t.Fatalf("best tag = %q, want Best", got)
	}
	if got := staggerTag(ordered, "first", "best"); got != "Stagger#1" {
		t.Fatalf("first fallback tag = %q, want Stagger#1", got)
	}
	if got := staggerTag(ordered, "second", "best"); got != "Stagger#2" {
		t.Fatalf("second fallback tag = %q, want Stagger#2", got)
	}

	staggerOnly := []C.Proxy{first, second}
	if got := staggerTag(staggerOnly, "first", ""); got != "Stagger#1" {
		t.Fatalf("first stagger-only tag = %q, want Stagger#1", got)
	}
	if got := staggerTag(staggerOnly, "second", ""); got != "Stagger#2" {
		t.Fatalf("second stagger-only tag = %q, want Stagger#2", got)
	}
}

func TestRaceStaggeredRejectsNilConnectionWinner(t *testing.T) {
	proxy := &stubProxy{name: "nil-winner"}
	failed := false
	winner, conn, _, err := raceStaggered(context.Background(), []C.Proxy{proxy}, nil, 0,
		func(context.Context, C.Proxy) (C.Conn, int64, error) { return nil, 1, nil },
		func(string, int64) { t.Fatal("nil connection was recorded as connected") },
		func(C.Proxy, error) { failed = true },
		func(C.Proxy, int64) { t.Fatal("nil connection was selected as winner") },
	)
	if err != nil || winner != nil || conn != nil {
		t.Fatalf("race result = (%v, %v, %v), want no winner and no fatal error", winner, conn, err)
	}
	if !failed {
		t.Fatal("nil connection result did not enter failure handling")
	}
}

func TestRawDialPathsRejectNilConnections(t *testing.T) {
	rt := smart.NewRouteTable(10)
	s := &Smart{testUrl: "test", routeTable: rt}
	metadata := &C.Metadata{Host: "example.com"}
	key, domain := routeKey(metadata), routeDomain(metadata)

	tcpProxy := &stubProxy{name: "nil-tcp", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		return nil, nil
	}}
	if conn, _, err := s.dialTCP(context.Background(), tcpProxy, metadata, key); err == nil || conn != nil {
		t.Fatalf("dialTCP = (%v, %v), want nil connection and explicit error", conn, err)
	}

	udpProxy := &nilPacketProxy{stubProxy: &stubProxy{name: "nil-udp"}}
	if conn, err := s.dialUDPAndWrap(context.Background(), udpProxy, metadata, key, domain, true); err == nil || conn != nil {
		t.Fatalf("dialUDPAndWrap = (%v, %v), want nil connection and explicit error", conn, err)
	}
	if best, ok := rt.GetUDPBestProxyIfFresh(key, domain, time.Minute); ok {
		t.Fatalf("nil UDP connection was recorded as best %q", best)
	}
}

func TestUDPRouteDoesNotRetryFailedBestInFallback(t *testing.T) {
	best := &udpErrorProxy{stubProxy: &stubProxy{name: "best", delay: 1}}
	fallback := &udpErrorProxy{stubProxy: &stubProxy{name: "fallback", delay: 2}}
	base := NewGroupBase(GroupBaseOption{Name: "smart", Type: C.Smart})
	base.providerProxies = []C.Proxy{best, fallback}
	rt := smart.NewRouteTable(10)
	s := &Smart{GroupBase: base, testUrl: "test", routeTable: rt}
	metadata := &C.Metadata{Host: "example.com"}
	key, domain := routeKey(metadata), routeDomain(metadata)
	rt.SetUDPBestProxy(key, domain, best.Name(), true)

	conn, err := s.udpRoute(context.Background(), metadata)
	if err == nil || conn != nil {
		t.Fatalf("udpRoute = (%v, %v), want all-proxies-failed error", conn, err)
	}
	if best.calls != 1 {
		t.Fatalf("failed UDP best dialed %d times, want exactly once", best.calls)
	}
	if fallback.calls != 1 {
		t.Fatalf("fallback dialed %d times, want once", fallback.calls)
	}
}

func TestTCPRouteMissingManualSelectionReturnsError(t *testing.T) {
	available := &stubProxy{name: "automatic-fallback"}
	base := NewGroupBase(GroupBaseOption{Name: "smart", Type: C.Smart})
	base.providerProxies = []C.Proxy{available}
	s := &Smart{
		GroupBase:       base,
		selected:        "removed-fixed-proxy",
		testUrl:         "test",
		routeTable:      smart.NewRouteTable(10),
		raceCoordinator: newSmartRaceCoordinator(),
	}
	defer s.raceCoordinator.Close()

	conn, err := s.tcpRoute(context.Background(), &C.Metadata{Host: "example.com"})
	if err == nil || conn != nil {
		t.Fatalf("tcpRoute = (%v, %v), want explicit missing-selection error", conn, err)
	}
}
