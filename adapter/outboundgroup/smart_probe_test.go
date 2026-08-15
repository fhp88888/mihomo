package outboundgroup

import (
	"context"
	"errors"
	"fmt"
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
	name string
	delay uint16
	dial  func(context.Context, *C.Metadata) (C.Conn, error)
}

func (s *stubProxy) Name() string              { return s.name }
func (s *stubProxy) Type() C.AdapterType       { return C.Direct }
func (s *stubProxy) Addr() string              { return "" }
func (s *stubProxy) SupportUDP() bool          { return false }
func (s *stubProxy) ProxyInfo() C.ProxyInfo    { return C.ProxyInfo{} }
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
func (s *stubProxy) SupportUOT() bool                      { return false }
func (s *stubProxy) IsL3Protocol(metadata *C.Metadata) bool { return false }
func (s *stubProxy) Unwrap(metadata *C.Metadata, touch bool) C.Proxy { return nil }
func (s *stubProxy) Close() error                          { return nil }
func (s *stubProxy) Adapter() C.ProxyAdapter               { return s }
func (s *stubProxy) AliveForTestUrl(url string) bool       { return true }
func (s *stubProxy) DelayHistory() []C.DelayHistory        { return nil }
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
// probeBatch error-classification tests
//
// These cover the semantics that parallelDial used to own: fatal
// (target-level) errors must surface and not penalize the proxy, while
// node-level errors are MarkFailed.  parallelDial is gone; the classification
// now lives inside probeBatch's race onFail hook, so these tests drive
// probeBatch directly.
// =========================================================================

func TestProbeBatch_ErrorsJoin_PreservesSentinel(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(10)

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

	result := pc.probeBatch(
		context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1", "p2", "p3"},
		singleDial, rt,
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

func TestProbeBatch_ErrorsJoin_WrappedSentinel(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(10)

	proxies := makeStubProxies("p1", "p2")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		return nil, 0, fmt.Errorf("resolve failed: %w", resolver.ErrIPNotFound)
	}

	result := pc.probeBatch(
		context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1", "p2"},
		singleDial, rt,
	)

	if result.err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(result.err, resolver.ErrIPNotFound) {
		t.Fatalf("errors.Is(result.err, wrapped ErrIPNotFound) = false; err: %v", result.err)
	}
}

func TestProbeBatch_ErrorsJoin_NoSentinel(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(10)

	proxies := makeStubProxies("p1", "p2")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		return nil, 10, fmt.Errorf("network timeout")
	}

	result := pc.probeBatch(
		context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1", "p2"},
		singleDial, rt,
	)

	if result.err == nil {
		t.Fatal("expected error, got nil")
	}
	if tunnel.ShouldStopRetry(result.err) {
		t.Fatal("ShouldStopRetry returned true, expected false for node-level errors")
	}
}

func TestProbeBatch_AllFailed_NodeLevel(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(10)

	proxies := makeStubProxies("p1", "p2", "p3")

	singleDial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
		return nil, 0, fmt.Errorf("fail: %s", p.Name())
	}

	result := pc.probeBatch(
		context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1", "p2", "p3"},
		singleDial, rt,
	)

	if result.err == nil {
		t.Fatal("expected error when every proxy failed, got nil")
	}
}

func TestProbeBatch_EmptyBatch(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()

	result := pc.probeBatch(
		context.Background(), "TARGET:x",
		nil, &C.Metadata{}, nil,
		nil, nil,
	)

	if result.err == nil {
		t.Fatal("expected error for empty batch")
	}
}

// stubConn is a minimal C.Conn for testing successful dial results.
type stubConn struct {
	mu     sync.Mutex
	closes int
}

func (s *stubConn) Read(b []byte) (n int, err error)            { return 0, io.EOF }
func (s *stubConn) Write(b []byte) (n int, err error)           { return len(b), nil }
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
func (s *stubConn) LocalAddr() net.Addr                         { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0} }
func (s *stubConn) RemoteAddr() net.Addr                        { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443} }
func (s *stubConn) SetDeadline(t time.Time) error               { return nil }
func (s *stubConn) SetReadDeadline(t time.Time) error           { return nil }
func (s *stubConn) SetWriteDeadline(t time.Time) error          { return nil }
func (s *stubConn) ReadBuffer(buffer *buf.Buffer) error         { return io.EOF }
func (s *stubConn) WriteBuffer(buffer *buf.Buffer) error        { return nil }
func (s *stubConn) Upstream() any                               { return nil }
func (s *stubConn) NeedHandshake() bool                         { return false }
func (s *stubConn) ReaderReplaceable() bool                     { return false }
func (s *stubConn) WriterReplaceable() bool                     { return false }
func (s *stubConn) Chains() C.Chain                             { return nil }
func (s *stubConn) ProviderChains() C.Chain                     { return nil }
func (s *stubConn) AppendToChains(adapter C.ProxyAdapter)       {}
func (s *stubConn) RemoteDestination() string                   { return "" }

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
	table.UpdateLatency(key, first.Name(), 10)
	table.SetBestProxy(key, first.Name())
	s := &Smart{testUrl: "test", routeTable: table}

	result := make(chan struct {
		conn C.Conn
		err  error
	}, 1)
	go func() {
		conn, err := s.raceAndWrap(context.Background(), &C.Metadata{Host: "example.com"}, key,
			[]C.Proxy{first, second, third}, smartTCPFallbackStagger, nil, "Stagger")
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
		conn, err := s.raceAndWrap(context.Background(), &C.Metadata{Host: "example.com"}, key,
			[]C.Proxy{first, second}, smartTCPFallbackStagger, nil, "Stagger")
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
	table.UpdateLatency(key, first.Name(), 10)
	table.SetBestProxy(key, first.Name())
	s := &Smart{testUrl: "test", routeTable: table}
	_, err := s.raceAndWrap(context.Background(), &C.Metadata{Host: "example.com"}, key,
		[]C.Proxy{first, second}, smartTCPFallbackStagger, nil, "Stagger")
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
	table.UpdateLatency(key, first.Name(), 10)
	table.SetBestProxy(key, first.Name())
	s := &Smart{testUrl: "test", routeTable: table}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := s.raceAndWrap(ctx, &C.Metadata{Host: "example.com"}, key,
			[]C.Proxy{first, second}, smartTCPFallbackStagger, nil, "Stagger")
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

func TestProbeBatch_ReturnsWinner_Success(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(10)

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

	result := pc.probeBatch(
		context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, []string{"p1", "p2", "p3"},
		singleDial, rt,
	)

	if result.err != nil {
		t.Fatalf("expected success, got err=%v", result.err)
	}
	if result.proxy.Name() != "p2" {
		t.Fatalf("expected winner p2, got %s", result.proxy.Name())
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
		context.Background(), "TARGET:example.com",
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
		context.Background(), "TARGET:example.com",
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
		context.Background(), "TARGET:example.com",
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
		context.Background(), "TARGET:example.com",
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
		context.Background(), "TARGET:example.com",
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
		context.Background(), "TARGET:example.com",
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

// TestProbeBatch_KeepsLosersAliveAfterWinner verifies the discovery-path
// raceStaggered behavior: once a winner is found, in-flight loser dials are
// NOT immediately canceled — they run to their own dial timeout, and any that
// succeed get their connectTime sampled into the route table (EMA update) via
// onConnect, then their connection is closed by the background drain.  The
// winner's conn and its best-proxy routing state must be unaffected.
func TestProbeBatch_KeepsLosersAliveAfterWinner(t *testing.T) {
	const key = "TARGET:example.com"

	loserStarted := make(chan struct{})
	loserDone := make(chan struct{})
	loserCtxCanceled := make(chan struct{})
	loserConn := &stubConn{}
	winnerConn := &stubConn{}

	// Loser is preRanked first (dialed at t=0) but blocks until released, so
	// the winner (dialed after the 200ms stagger) wins the race while the
	// loser is still in flight.
	loser := &stubProxy{name: "loser", dial: func(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
		close(loserStarted)
		<-loserDone
		select {
		case <-ctx.Done():
			close(loserCtxCanceled)
			return nil, ctx.Err()
		default:
			return loserConn, nil
		}
	}}
	winner := &stubProxy{name: "winner", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		return winnerConn, nil
	}}

	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(100)
	// Seed the winner as best proxy, as discoverAndRoute would after a win.
	// The late loser's onConnect must not displace or clear it.
	rt.SetBestProxy(key, "winner")

	result := make(chan struct {
		conn C.Conn
		err  error
	}, 1)
	go func() {
		pr := pc.probeBatch(
			context.Background(), key,
			[]C.Proxy{loser, winner}, &C.Metadata{}, []string{"loser", "winner"},
			func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
				conn, err := p.DialContext(ctx, m)
				return conn, 42, err
			},
			rt,
		)
		result <- struct {
			conn C.Conn
			err  error
		}{pr.conn, pr.err}
	}()

	select {
	case <-loserStarted:
	case <-time.After(time.Second):
		t.Fatal("loser dial did not start")
	}

	// Winner fires after the stagger interval and wins while the loser is
	// still blocked.  probeBatch must return immediately with the winner.
	select {
	case r := <-result:
		if r.err != nil {
			t.Fatalf("probeBatch returned error: %v", r.err)
		}
		if r.conn == nil {
			t.Fatal("probeBatch returned nil connection")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("probeBatch did not return after winner succeeded")
	}

	// Now release the loser.  With the discovery path keeping losers alive,
	// its dial ctx is still live and it returns its successful conn.  Without
	// it, cancelRace would have fired and the loser would return canceled.
	close(loserDone)

	// Wait for the loser's conn to be drained and closed by the background
	// drain goroutine (after onConnect samples its connectTime).
	deadline := time.Now().Add(2 * time.Second)
	for loserConn.CloseCount() == 0 {
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case <-loserCtxCanceled:
		t.Fatal("loser dial was canceled after winner — loser should be kept alive on discovery path")
	default:
	}
	if loserConn.CloseCount() != 1 {
		t.Fatalf("loser conn closed %d times, want 1 (drained after sampling)", loserConn.CloseCount())
	}
	if winnerConn.CloseCount() != 0 {
		t.Fatalf("winner conn closed %d times, want 0", winnerConn.CloseCount())
	}

	// loser's connectTime (42ms) must have been recorded into the route table
	// via onConnect → UpdateLatency.
	var loserLat int64
	for _, row := range rt.Snapshot("").Rows {
		if row.Key == key {
			loserLat = row.Proxies["loser"].Attributes.Latency
		}
	}
	if loserLat != 42 {
		t.Fatalf("loser latency = %dms, want 42ms (sampled via onConnect)", loserLat)
	}

	// winner must remain the best proxy; the late loser must not displace it.
	if best, ok := rt.GetBestProxy(key); !ok || best != "winner" {
		t.Fatalf("best proxy = %q ok=%v, want winner", best, ok)
	}
}

// TestProbeBatch_KeepsLosersAliveThroughDiscover drives the full Discover
// path (leader + defer) to confirm the leader's deferred cleanup does NOT
// cancel leaderCtx early — otherwise the winner returning would abort the
// in-flight loser dial before the background drain samples it.  This is the
// regression guard for the Discover-side half of the keep-losers-alive fix.
func TestProbeBatch_KeepsLosersAliveThroughDiscover(t *testing.T) {
	const key = "TARGET:example.com"

	loserStarted := make(chan struct{})
	loserDone := make(chan struct{})
	loserCtxCanceled := make(chan struct{})
	loserConn := &stubConn{}
	winnerConn := &stubConn{}

	loser := &stubProxy{name: "loser", dial: func(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
		close(loserStarted)
		<-loserDone
		select {
		case <-ctx.Done():
			close(loserCtxCanceled)
			return nil, ctx.Err()
		default:
			return loserConn, nil
		}
	}}
	winner := &stubProxy{name: "winner", dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		return winnerConn, nil
	}}

	pc := NewProbeCoordinator()
	defer pc.Close()
	rt := smart.NewRouteTable(100)
	rt.SetBestProxy(key, "winner")

	result := make(chan struct {
		conn C.Conn
		err  error
	}, 1)
	go func() {
		_, c, _, e := pc.Discover(
			context.Background(), key,
			[]C.Proxy{loser, winner}, &C.Metadata{}, []string{"loser", "winner"},
			func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
				conn, err := p.DialContext(ctx, m)
				return conn, 42, err
			},
			rt,
		)
		result <- struct {
			conn C.Conn
			err  error
		}{c, e}
	}()

	select {
	case <-loserStarted:
	case <-time.After(time.Second):
		t.Fatal("loser dial did not start")
	}

	// Winner fires after the stagger and wins while the loser is still blocked.
	// Discover must return immediately with the winner (leader cleanup must not
	// wait on losers synchronously).
	select {
	case r := <-result:
		if r.err != nil {
			t.Fatalf("Discover returned error: %v", r.err)
		}
		if r.conn == nil {
			t.Fatal("Discover returned nil connection")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Discover did not return after winner succeeded")
	}

	// Now release the loser.  The winner-return must NOT have canceled it —
	// the leader's deferred cleanup must leave leaderCtx alive so the loser
	// completes and the background drain samples its connectTime.
	close(loserDone)

	deadline := time.Now().Add(2 * time.Second)
	for loserConn.CloseCount() == 0 {
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case <-loserCtxCanceled:
		t.Fatal("loser dial was canceled after winner — leader cleanup must keep loser alive")
	default:
	}
	if loserConn.CloseCount() != 1 {
		t.Fatalf("loser conn closed %d times, want 1 (drained after sampling)", loserConn.CloseCount())
	}
	if winnerConn.CloseCount() != 0 {
		t.Fatalf("winner conn closed %d times, want 0", winnerConn.CloseCount())
	}

	var loserLat int64
	for _, row := range rt.Snapshot("").Rows {
		if row.Key == key {
			loserLat = row.Proxies["loser"].Attributes.Latency
		}
	}
	if loserLat != 42 {
		t.Fatalf("loser latency = %dms, want 42ms (sampled via onConnect)", loserLat)
	}
}
// =========================================================================
// best-first race tests (serialTcpConn fast-path)
// =========================================================================

func newBestRaceSmart() (*Smart, *smart.RouteTable, *ProbeCoordinator) {
	rt := smart.NewRouteTable(10)
	pc := NewProbeCoordinator()
	return &Smart{testUrl: "test", routeTable: rt, probeCoordinator: pc}, rt, pc
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

	rt.SetBestProxy(key, "best")

	start := time.Now()
	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, []C.Proxy{best, other})
	if err != nil {
		t.Fatalf("serialTcpConn error: %v", err)
	}
	if conn == nil {
		t.Fatal("serialTcpConn returned nil connection")
	}
	elapsed := time.Since(start)
	if elapsed > smartBestExclusiveWindow {
		t.Fatalf("best win took %v, want within %v", elapsed, smartBestExclusiveWindow)
	}
	if bestConn.CloseCount() != 0 {
		t.Fatalf("winner conn closed %d times", bestConn.CloseCount())
	}
	_ = conn.Close()
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

	rt.SetBestProxy(key, "best")

	start := time.Now()
	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, []C.Proxy{best, second})
	if err != nil {
		t.Fatalf("serialTcpConn error: %v", err)
	}
	if conn == nil {
		t.Fatal("serialTcpConn returned nil connection")
	}

	// second must not be launched before the exclusive window elapses.
	select {
	case <-secondStarted:
		if time.Since(start) < smartBestExclusiveWindow {
			t.Fatal("second launched before exclusive window elapsed")
		}
	case <-time.After(2 * smartBestExclusiveWindow):
		t.Fatal("second proxy never launched")
	}

	// second wins the race; best is unblocked later and drained as a loser.
	bestUnblock <- struct{}{}
	pc.wg.Wait()

	got, _ := rt.GetBestProxy(key)
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

	rt.SetBestProxy(key, "best")

	start := time.Now()
	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, []C.Proxy{best, second})
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
		if elapsed >= smartBestExclusiveWindow {
			t.Fatalf("second launched after %v, want immediate (best failed early)", elapsed)
		}
	case <-time.After(smartBestExclusiveWindow):
		t.Fatal("second proxy never launched after best failed early")
	}

	got, _ := rt.GetBestProxy(key)
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
func collectLogs() (waitForLog func(prefix string, timeout time.Duration) bool, stop func()) {
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
	contains := func(prefix string) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, l := range logs {
			if strings.Contains(l, prefix) {
				return true
			}
		}
		return false
	}
	waitForLog = func(prefix string, timeout time.Duration) bool {
		deadline := time.After(timeout)
		for {
			if contains(prefix) {
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
	rt.SetBestProxy(key, "best")

	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, []C.Proxy{best, other})
	if err != nil || conn == nil {
		t.Fatalf("serialTcpConn = (%v, %v), want conn", conn, err)
	}
	_ = conn.Close()

	if !waitForLog("routed via best", time.Second) {
		t.Fatal("best not marked winner")
	}
	if waitForLog("routed via other", 200*time.Millisecond) {
		t.Fatal("other dialed despite best winning")
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
	rt.SetBestProxy(key, "best")

	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, []C.Proxy{best, second})
	if err != nil || conn == nil {
		t.Fatalf("serialTcpConn = (%v, %v), want conn", conn, err)
	}
	bestUnblock <- struct{}{}
	pc.wg.Wait() // let the background drain settle
	_ = conn.Close()

	if !waitForLog("routed via second", time.Second) {
		t.Fatal("second never dialed in fallback race")
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
	rt.SetBestProxy(key, "best")

	conn, err := s.serialTcpConn(context.Background(), &C.Metadata{Host: "example.com"}, key, []C.Proxy{best, second})
	if err != nil || conn == nil {
		t.Fatalf("serialTcpConn = (%v, %v), want conn", conn, err)
	}
	pc.wg.Wait()
	_ = conn.Close()

	if !waitForLog("dial best failed", time.Second) {
		t.Fatal("best failure not logged")
	}
	if !waitForLog("routed via second", time.Second) {
		t.Fatal("second not marked winner")
	}
}
