package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel"
)

// stubProxy implements C.Proxy with minimal behavior for testing.
type stubProxy struct {
	name  string
	delay uint16
	dial  func(context.Context, *C.Metadata) (C.Conn, error)
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

func makeStubProxies(names ...string) []C.Proxy {
	proxies := make([]C.Proxy, len(names))
	for i, name := range names {
		proxies[i] = &stubProxy{name: name}
	}
	return proxies
}

// =========================================================================
// parallelDial tests
// =========================================================================

func TestParallelDial_ErrorsJoin_PreservesSentinel(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()

	proxies := makeStubProxies("p1", "p2", "p3")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		switch p.Name() {
		case "p1":
			return nil, 10, fmt.Errorf("network timeout")
		case "p2":
			return nil, 20, resolver.ErrIPNotFound
		case "p3":
			return nil, 30, fmt.Errorf("connection refused")
		}
		return nil, 0, nil
	}

	result, _, _ := pc.parallelDial(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, singleDial, nil,
	)

	if result.err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(result.err, resolver.ErrIPNotFound) {
		t.Fatalf("errors.Is(result.err, ErrIPNotFound) = false; err: %v", result.err)
	}
	if !tunnel.ShouldStopRetry(result.err) {
		t.Fatal("ShouldStopRetry returned false, expected true")
	}
}

func TestParallelDial_ErrorsJoin_WrappedSentinel(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()

	proxies := makeStubProxies("p1", "p2")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		return nil, 0, fmt.Errorf("resolve failed: %w", resolver.ErrIPNotFound)
	}

	result, _, _ := pc.parallelDial(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, singleDial, nil,
	)

	if result.err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(result.err, resolver.ErrIPNotFound) {
		t.Fatalf("errors.Is(result.err, wrapped ErrIPNotFound) = false; err: %v", result.err)
	}
}

func TestParallelDial_ErrorsJoin_NoSentinel(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()

	proxies := makeStubProxies("p1", "p2")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		return nil, 10, fmt.Errorf("network timeout")
	}

	result, _, _ := pc.parallelDial(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, singleDial, nil,
	)

	if result.err == nil {
		t.Fatal("expected error, got nil")
	}
	if tunnel.ShouldStopRetry(result.err) {
		t.Fatal("ShouldStopRetry returned true, expected false for node-level errors")
	}
}

func TestParallelDial_ReturnsDialResults_AllFailed(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()

	proxies := makeStubProxies("p1", "p2", "p3")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		return nil, 0, fmt.Errorf("fail: %s", p.Name())
	}

	_, _, dialResults := pc.parallelDial(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, singleDial, nil,
	)

	if len(dialResults) != 3 {
		t.Fatalf("expected 3 dial results, got %d", len(dialResults))
	}
	for _, dr := range dialResults {
		if dr.err == nil {
			t.Fatalf("expected all results to have errors, but %s succeeded", dr.proxy.Name())
		}
	}
}

func TestParallelDial_EmptyBatch(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()

	result, metrics, dialResults := pc.parallelDial(
		context.Background(), context.Background(), "TARGET:x",
		nil, &C.Metadata{}, nil, nil,
	)

	if result.err == nil {
		t.Fatal("expected error for empty batch")
	}
	if metrics != nil {
		t.Error("metrics should be nil for empty batch")
	}
	if dialResults != nil {
		t.Error("dialResults should be nil for empty batch")
	}
}

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

func routeFailedCount(t *testing.T, table *smart.RouteTable, key, proxy string) float64 {
	t.Helper()
	for _, row := range table.Snapshot("").Rows {
		if row.Key == key {
			return row.Proxies[proxy].Attributes.FailedCount
		}
	}
	t.Fatalf("route row %q not found", key)
	return 0
}

func waitForLatencies(t *testing.T, table *smart.RouteTable, key string, expected map[string]int64) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if hasLatencies(table, key, expected) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for latencies for key %s: want %v, snapshot=%+v", key, expected, table.Snapshot(""))
		case <-ticker.C:
		}
	}
}

func hasLatencies(table *smart.RouteTable, key string, expected map[string]int64) bool {
	for _, row := range table.Snapshot("").Rows {
		if row.Key != key {
			continue
		}
		for proxy, latency := range expected {
			record, ok := row.Proxies[proxy]
			if !ok || record.Attributes.Latency != latency {
				return false
			}
		}
		return true
	}
	return false
}

func waitForClosedConns(t *testing.T, conns []*stubConn, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		allClosed := true
		for _, conn := range conns {
			if conn == nil || conn.CloseCount() != want {
				allClosed = false
				break
			}
		}
		if allClosed {
			return
		}
		select {
		case <-deadline:
			counts := make([]int, len(conns))
			for i, conn := range conns {
				if conn != nil {
					counts[i] = conn.CloseCount()
				}
			}
			t.Fatalf("timed out waiting for close count %d, got %v", want, counts)
		case <-ticker.C:
		}
	}
}

func TestDiscover_FirstSuccessAllowsTopKLosersToRecordLatency(t *testing.T) {
	const key = "TARGET:example.com"
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(100)
	releaseLosers := make(chan struct{})
	canceled := make(chan string, topK-1)
	winnerConn := &stubConn{}
	loserConns := make([]*stubConn, topK-1)

	proxies := make([]C.Proxy, 0, topK)
	preRanked := make([]string, 0, topK)
	expected := map[string]int64{"p0": 5}
	for i := 0; i < topK; i++ {
		name := fmt.Sprintf("p%d", i)
		proxies = append(proxies, &stubProxy{name: name})
		preRanked = append(preRanked, name)
		if i > 0 {
			loserConns[i-1] = &stubConn{}
			expected[name] = int64(10 + i)
		}
	}

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		if p.Name() == "p0" {
			return winnerConn, 5, nil
		}
		select {
		case <-releaseLosers:
			var idx int
			if _, err := fmt.Sscanf(p.Name(), "p%d", &idx); err != nil {
				return nil, 0, err
			}
			return loserConns[idx-1], int64(10 + idx), nil
		case <-ctx.Done():
			canceled <- p.Name()
			return nil, 0, ctx.Err()
		}
	}

	start := time.Now()
	proxy, conn, connectTime, err := pc.Discover(
		context.Background(), key, proxies, &C.Metadata{}, preRanked, singleDial, rt,
	)
	if err != nil {
		t.Fatalf("Discover returned error: %v", err)
	}
	if proxy.Name() != "p0" {
		t.Fatalf("winner = %s, want p0", proxy.Name())
	}
	if conn != winnerConn {
		t.Fatal("Discover did not return the winner connection")
	}
	if connectTime != 5 {
		t.Fatalf("connectTime = %d, want 5", connectTime)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Discover waited for loser probes, elapsed=%s", elapsed)
	}

	close(releaseLosers)
	waitForLatencies(t, rt, key, expected)
	waitForClosedConns(t, loserConns, 1)
	if got := winnerConn.CloseCount(); got != 0 {
		t.Fatalf("winner connection close count = %d, want 0", got)
	}
	select {
	case name := <-canceled:
		t.Fatalf("loser %s was canceled before latency could be recorded", name)
	default:
	}
}

func TestDiscover_CloseCancelsBackgroundLosersAfterWinner(t *testing.T) {
	const key = "TARGET:example.com"
	pc := NewProbeCoordinator()
	rt := smart.NewRouteTable(100)
	canceled := make(chan string, topK-1)

	proxies := make([]C.Proxy, 0, topK)
	preRanked := make([]string, 0, topK)
	for i := 0; i < topK; i++ {
		name := fmt.Sprintf("p%d", i)
		proxies = append(proxies, &stubProxy{name: name})
		preRanked = append(preRanked, name)
	}

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		if p.Name() == "p0" {
			return &stubConn{}, 5, nil
		}
		<-ctx.Done()
		canceled <- p.Name()
		return nil, 0, ctx.Err()
	}

	proxy, conn, _, err := pc.Discover(
		context.Background(), key, proxies, &C.Metadata{}, preRanked, singleDial, rt,
	)
	if err != nil {
		t.Fatalf("Discover returned error: %v", err)
	}
	if proxy.Name() != "p0" || conn == nil {
		t.Fatalf("unexpected winner proxy=%v conn=%v", proxy.Name(), conn)
	}

	closed := make(chan error, 1)
	go func() {
		closed <- pc.Close()
	}()

	seen := make(map[string]struct{}, topK-1)
	for len(seen) < topK-1 {
		select {
		case name := <-canceled:
			seen[name] = struct{}{}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for loser cancellations, saw %d/%d", len(seen), topK-1)
		}
	}

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ProbeCoordinator.Close did not return")
	}
}

func TestStaggeredTCPFallback_FirstSuccessCancelsLosers(t *testing.T) {
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
	table.UpdateLatency(key, first.Name(), 10)
	table.SetBestProxy(key, first.Name())
	s := &Smart{testUrl: "test", routeTable: table}

	result := make(chan struct {
		conn C.Conn
		err  error
	}, 1)
	go func() {
		conn, err := s.staggeredTCPFallback(context.Background(), &C.Metadata{Host: "example.com"}, key,
			[]string{first.Name(), second.Name(), third.Name()}, map[string]C.Proxy{
				first.Name(): first, second.Name(): second, third.Name(): third,
			})
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
	if got := routeFailedCount(t, table, key, first.Name()); got != 0 {
		t.Fatalf("canceled first proxy failed count = %v, want 0", got)
	}
	if winner.CloseCount() != 0 {
		t.Fatalf("winner was closed %d times", winner.CloseCount())
	}
	_ = got.conn.Close()
}

func TestStaggeredTCPFallback_ClosesLateSuccessfulLoser(t *testing.T) {
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
		conn, err := s.staggeredTCPFallback(context.Background(), &C.Metadata{Host: "example.com"}, key,
			[]string{first.Name(), second.Name()}, map[string]C.Proxy{first.Name(): first, second.Name(): second})
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
	if best, ok := s.routeTable.GetBestProxy(key); !ok || best != second.Name() {
		t.Fatalf("best proxy = %q ok=%v, want selected winner %q", best, ok, second.Name())
	}
	if !s.routeTable.IsTCPProbed(key) {
		t.Fatal("selected winner did not mark route TCP-probed")
	}
	var firstUseCount, secondUseCount int64
	for _, row := range s.routeTable.Snapshot("").Rows {
		if row.Key == key {
			firstUseCount = row.Proxies[first.Name()].UseCount
			secondUseCount = row.Proxies[second.Name()].UseCount
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

func TestStaggeredTCPFallback_FatalErrorStopsScheduling(t *testing.T) {
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
	table.UpdateLatency(key, first.Name(), 10)
	table.SetBestProxy(key, first.Name())
	s := &Smart{testUrl: "test", routeTable: table}
	_, err := s.staggeredTCPFallback(context.Background(), &C.Metadata{Host: "example.com"}, key,
		[]string{first.Name(), second.Name()}, map[string]C.Proxy{first.Name(): first, second.Name(): second})
	if !errors.Is(err, resolver.ErrIPNotFound) || !tunnel.ShouldStopRetry(err) {
		t.Fatalf("fatal error = %v, want ErrIPNotFound", err)
	}
	select {
	case <-secondStarted:
		t.Fatal("second proxy started after fatal first result")
	case <-time.After(2 * smartTCPFallbackStagger):
	}
	if best, ok := table.GetBestProxy(key); !ok || best != first.Name() {
		t.Fatalf("fatal error cleared best proxy: best=%q ok=%v", best, ok)
	}
}

func TestStaggeredTCPFallback_ParentCancellationStopsScheduling(t *testing.T) {
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
	table.UpdateLatency(key, first.Name(), 10)
	table.SetBestProxy(key, first.Name())
	s := &Smart{testUrl: "test", routeTable: table}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := s.staggeredTCPFallback(ctx, &C.Metadata{Host: "example.com"}, key,
			[]string{first.Name(), second.Name()}, map[string]C.Proxy{first.Name(): first, second.Name(): second})
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
	if got := routeFailedCount(t, table, key, first.Name()); got != 0 {
		t.Fatalf("canceled first proxy failed count = %v, want 0", got)
	}
}

func TestParallelDial_ReturnsDialResults_Success(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()

	proxies := makeStubProxies("p1", "p2", "p3")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		switch p.Name() {
		case "p1":
			return nil, 10, fmt.Errorf("fail")
		case "p2":
			return &stubConn{}, 5, nil
		case "p3":
			time.Sleep(200 * time.Millisecond)
			return nil, 50, fmt.Errorf("slow fail")
		}
		return nil, 0, nil
	}

	result, _, dialResults := pc.parallelDial(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, singleDial, nil,
	)

	if result.err != nil {
		t.Fatalf("expected success, got err=%v", result.err)
	}
	if result.proxy.Name() != "p2" {
		t.Fatalf("expected winner p2, got %s", result.proxy.Name())
	}
	// The production contract is "first success returns immediately",
	// so the winner may be the only result in dialResults.  Verify
	// that the winner is present rather than asserting a minimum count.
	if len(dialResults) < 1 {
		t.Fatal("expected at least 1 dial result (the winner), got 0")
	}
	foundWinner := false
	for _, dr := range dialResults {
		if dr.err == nil && dr.proxy.Name() == "p2" {
			foundWinner = true
			break
		}
	}
	if !foundWinner {
		t.Error("p2 should be in dialResults as the winner")
	}
}

// =========================================================================
// probeBatch tests
// =========================================================================

func TestProbeBatch_FatalError_ReturnsImmediately(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(100)
	rt.UpdateLatency("TARGET:example.com", "p1", 10)

	proxies := makeStubProxies("p1", "p2")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		return nil, 0, resolver.ErrIPNotFound
	}

	result := pc.probeBatch(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1", "p2"},
		singleDial, rt,
	)

	if result.err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(result.err, resolver.ErrIPNotFound) {
		t.Fatalf("expected ErrIPNotFound, got %v", result.err)
	}
	if !tunnel.ShouldStopRetry(result.err) {
		t.Fatal("ShouldStopRetry returned false for fatal error")
	}
}

func TestProbeBatch_FatalError_SkipsMarkFailed(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(100)

	rt.UpdateLatency("TARGET:example.com", "p1", 10)
	rt.UpdateLatency("TARGET:example.com", "p2", 10)
	rt.SetBestProxy("TARGET:example.com", "p1")

	proxies := makeStubProxies("p1", "p2")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		return nil, 0, resolver.ErrIPNotFound
	}

	result := pc.probeBatch(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1", "p2"},
		singleDial, rt,
	)

	if result.err == nil {
		t.Fatal("expected error, got nil")
	}

	// MarkFailed should NOT have been called for fatal error.
	// Best proxy should still be set.
	bp, _ := rt.GetBestProxy("TARGET:example.com")
	if bp == "" {
		t.Error("best proxy was cleared — MarkFailed was incorrectly called for fatal error")
	}
}

func TestProbeBatch_NodeLevelError_MarksFailingProxy(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(100)

	rt.UpdateLatency("TARGET:example.com", "p1", 10)
	rt.UpdateLatency("TARGET:example.com", "p2", 10)
	rt.SetBestProxy("TARGET:example.com", "p1")

	proxies := makeStubProxies("p1", "p2")

	// p1 fails with node-level error; p2 fails with cancellation
	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		switch p.Name() {
		case "p1":
			return nil, 0, fmt.Errorf("connection refused")
		case "p2":
			return nil, 0, context.Canceled
		}
		return nil, 0, nil
	}

	_ = pc.probeBatch(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1", "p2"},
		singleDial, rt,
	)

	// p1 should have been marked failed → best proxy cleared
	// p2 should NOT be marked failed (cancellation → skip)
	// So best proxy should be empty (p1 was cleared)
	bp, _ := rt.GetBestProxy("TARGET:example.com")
	if bp == "p1" {
		t.Error("p1 should have been marked failed (node-level error), best proxy should be cleared")
	}
}

func TestProbeBatch_ContextCanceled_SkipsMarkFailed(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(100)

	rt.UpdateLatency("TARGET:example.com", "p1", 10)
	rt.SetBestProxy("TARGET:example.com", "p1")

	proxies := makeStubProxies("p1")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		return nil, 0, context.Canceled
	}

	_ = pc.probeBatch(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1"},
		singleDial, rt,
	)

	bp, ok := rt.GetBestProxy("TARGET:example.com")
	if !ok || bp != "p1" {
		t.Errorf("best proxy should still be 'p1' after context.Canceled, got %q ok=%v", bp, ok)
	}
}

func TestProbeBatch_MixedErrors_ReturnsFatal(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(100)

	rt.UpdateLatency("TARGET:example.com", "p1", 10)
	rt.UpdateLatency("TARGET:example.com", "p2", 10)

	proxies := makeStubProxies("p1", "p2")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		switch p.Name() {
		case "p1":
			return nil, 0, fmt.Errorf("connection refused")
		case "p2":
			return nil, 0, resolver.ErrIPVersion
		}
		return nil, 0, nil
	}

	result := pc.probeBatch(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1", "p2"},
		singleDial, rt,
	)

	if result.err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(result.err, resolver.ErrIPVersion) {
		t.Fatalf("expected ErrIPVersion (fatal), got %v", result.err)
	}
}

func TestProbeBatch_AllProxiesNodeLevel_NoSentinelDetected(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(100)

	rt.UpdateLatency("TARGET:example.com", "p1", 10)

	proxies := makeStubProxies("p1")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		return nil, 0, fmt.Errorf("connection refused")
	}

	result := pc.probeBatch(
		context.Background(), context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1"},
		singleDial, rt,
	)

	if result.err == nil {
		t.Fatal("expected error, got nil")
	}
	if tunnel.ShouldStopRetry(result.err) {
		t.Fatal("ShouldStopRetry returned true for node-level error, should be false")
	}
}
