package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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
	name string
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
func (s *stubProxy) LastDelayForTestUrl(url string) uint16              { return 0 }
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
		context.Background(), "TARGET:example.com",
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
		context.Background(), "TARGET:example.com",
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
		context.Background(), "TARGET:example.com",
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
		context.Background(), "TARGET:example.com",
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
		context.Background(), "TARGET:x",
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
type stubConn struct{}

func (s *stubConn) Read(b []byte) (n int, err error)            { return 0, io.EOF }
func (s *stubConn) Write(b []byte) (n int, err error)           { return len(b), nil }
func (s *stubConn) Close() error                                { return nil }
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
		context.Background(), "TARGET:example.com",
		proxies, &C.Metadata{}, singleDial, nil,
	)

	if result.err != nil {
		t.Fatalf("expected success, got err=%v", result.err)
	}
	if result.proxy.Name() != "p2" {
		t.Fatalf("expected winner p2, got %s", result.proxy.Name())
	}
	if len(dialResults) < 2 {
		t.Fatalf("expected at least 2 dial results, got %d", len(dialResults))
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
