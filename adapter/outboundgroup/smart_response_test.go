package outboundgroup

import (
	"context"
	"encoding/pem"
	stdhttp "net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/http"
	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type responseTestProxy struct {
	*stubProxy
	status atomic.Int32
	probes atomic.Int32
	wait   func(context.Context)
}

func (p *responseTestProxy) StatusProbe(ctx context.Context, url string) (*smart.ProbeResult, error) {
	p.probes.Add(1)
	if p.wait != nil {
		p.wait(ctx)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return &smart.ProbeResult{StatusCode: int(p.status.Load()), Header: http.Header{}}, nil
}
func newResponseSmart(t *testing.T, nodes ...C.Proxy) *Smart {
	t.Helper()
	// Initialise the singleton in a temporary home before substituting the test
	// DB, so these integration tests never open the user's actual cache file.
	home := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	cachefile.GetSmartStore()
	C.SetHomeDir(home)
	testDir := t.TempDir()
	db, err := bbolt.Open(filepath.Join(testDir, "smart.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := smart.NewStore(db)
	hc := provider.NewHealthCheck(nodes, "test", 1000, 0, false, nil)
	pd, err := provider.NewCompatibleProvider("response", nodes, hc)
	if err != nil {
		t.Fatal(err)
	}
	s, _, pc := newBestRaceSmart()
	s.GroupBase = NewGroupBase(GroupBaseOption{Name: "response-smart", Type: C.Smart, Providers: []P.ProxyProvider{pd}, EmptyFallback: &stubProxy{name: "fallback"}})
	s.store = store
	s.configName = filepath.Base(filepath.Dir(testDir)) + "-" + filepath.Base(testDir)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	t.Cleanup(func() {
		s.responseMu.Lock()
		s.responseClosed = true
		s.responseMu.Unlock()
		s.cancel()
		s.responseWG.Wait()
		pc.Close()
		pd.Close()
		db.Close()
	})
	return s
}
func newResponseProxy(name string, status int) *responseTestProxy {
	p := &responseTestProxy{stubProxy: &stubProxy{name: name, dial: func(context.Context, *C.Metadata) (C.Conn, error) { return &stubConn{}, nil }}}
	p.status.Store(int32(status))
	return p
}
func probeResponse(t *testing.T, s *Smart, p C.Proxy, m *C.Metadata) {
	t.Helper()
	s.probeAfterClose(m, p)
	s.responseWG.Wait()
}

func TestSmartResponseRefusalChangesEligibilityNotQuality(t *testing.T) {
	bad := newResponseProxy("bad", 403)
	good := newResponseProxy("good", 200)
	s := newResponseSmart(t, bad, good)
	m := &C.Metadata{Host: "api.example.com", DstPort: 443, DstIPASN: "13335"}
	key, domain := routeKey(m), routeDomain(m)
	s.routeTable.UpdateLatency(key, domain, bad.Name(), 1)
	s.routeTable.IncrementUseCount(key, domain, bad.Name())
	s.routeTable.SetBestProxyAndTCPProbed(key, domain, bad.Name())
	probeResponse(t, s, bad, m)
	if bad.probes.Load() != 1 {
		t.Fatal("probe did not run")
	}
	if s.Unwrap(m, false).Name() != good.Name() {
		t.Fatal("cached best bypassed HTTP avoidance")
	}
	conn, err := s.tcpRoute(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	best, _ := s.routeTable.GetBestProxy(key, domain)
	if best != good.Name() {
		t.Fatalf("TCP selected refused node: %s", best)
	}
	if s.routeTable.ProxyFailedCount(key, domain, bad.Name()) != 0 {
		t.Fatal("HTTP refusal changed transport FailedCount")
	}
	nodes := s.responseCandidates(&C.Metadata{Host: "unrelated.example.com", DstIPASN: "13335"}, []C.Proxy{bad, good})
	if len(nodes) != 2 {
		t.Fatal("refusal leaked across same ASN")
	}
	if m.WildcardTarget != "" {
		t.Fatal("probe mutated original routing metadata")
	}
	s.selected = bad.Name()
	if s.Unwrap(m, false).Name() != bad.Name() {
		t.Fatal("manual selection was overridden")
	}
	conn, err = s.tcpRoute(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if len(s.responseCandidates(m, []C.Proxy{bad, good})) != 2 {
		t.Fatal("manual selection admission changed")
	}
	s.selected = ""
	// A confirmed reachable answer clears the separate avoidance state.
	clone := m.Clone()
	clone.WildcardTarget = domain
	clone.SmartTarget = domain
	s.applyNodeAnswer(clone, bad.Name(), smart.ClassifyResponse(200, nil, nil, time.Now()))
	if len(s.responseCandidates(m, []C.Proxy{bad, good})) != 2 {
		t.Fatal("successful recheck failed to restore eligibility")
	}
}

func TestSmartResponseCloseHookAndLimits(t *testing.T) {
	p := newResponseProxy("node", 429)
	s := newResponseSmart(t, p)
	m := &C.Metadata{Host: "tiny.example.com", DstPort: 443}
	rt := s.routeTable
	key, domain := routeKey(m), routeDomain(m)
	rt.UpdateLatency(key, domain, p.Name(), 10)
	c := s.wrapTCPConn(&stubConn{}, p, m, 10)
	tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, m, nil, 1, 1024, false)
	tracker.Close()
	s.responseWG.Wait()
	if p.probes.Load() != 1 {
		t.Fatal("close hook did not probe tiny HTTPS response")
	}
	if nodes := s.responseCandidates(m, []C.Proxy{p}); len(nodes) != 0 {
		t.Fatal("429 did not avoid node")
	}
	probeResponse(t, s, p, m)
	if p.probes.Load() != 1 {
		t.Fatal("repeat connection bypassed node throttle")
	}
	// A normal-size transfer and internal connections must not cause probes.
	for _, tc := range []struct {
		host     string
		download int64
		kind     C.Type
	}{
		{"large.example.com", 1 << 20, 0}, {"internal.example.com", 0, C.INNER},
	} {
		meta := &C.Metadata{Host: tc.host, DstPort: 443, Type: tc.kind}
		c := s.wrapTCPConn(&stubConn{}, p, meta, 10)
		tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, meta, nil, 1, tc.download, false)
		tracker.Close()
		s.responseWG.Wait()
	}
	if p.probes.Load() != 1 {
		t.Fatal("unnecessary response probe")
	}
	if conn, err := s.tcpRoute(context.Background(), m); err == nil || conn != nil {
		t.Fatal("all refused nodes silently admitted")
	}
}

func TestSmartResponseUDPAdmissionAndShutdown(t *testing.T) {
	bad := &udpErrorProxy{stubProxy: &stubProxy{name: "bad"}}
	good := &udpErrorProxy{stubProxy: &stubProxy{name: "good"}}
	s := newResponseSmart(t, bad, good)
	m := &C.Metadata{Host: "udp.example.com", DstPort: 443}
	clone := m.Clone()
	clone.WildcardTarget = routeDomain(m)
	s.applyNodeAnswer(clone, "bad", smart.ClassifyResponse(403, nil, nil, time.Now()))
	_, _ = s.udpRoute(context.Background(), m)
	if bad.calls != 0 || good.calls != 1 {
		t.Fatalf("UDP admission bypass: bad=%d good=%d", bad.calls, good.calls)
	}
	// A probe canceled during shutdown must never persist a fresh refusal.
	p := newResponseProxy("node", 403)
	started := make(chan struct{})
	p.wait = func(ctx context.Context) { close(started); <-ctx.Done() }
	other := newResponseSmart(t, p)
	meta := &C.Metadata{Host: "shutdown.example.com"}
	other.probeAfterClose(meta, p)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	if len(other.responseCandidates(meta, []C.Proxy{p})) != 1 {
		t.Fatal("shutdown recorded probe cancellation as a refusal")
	}
	other.responseMu.Lock()
	other.responseClosed = true
	other.responseMu.Unlock()
	other.probeAfterClose(&C.Metadata{Host: "after-close.example.com"}, p)
	if p.probes.Load() != 1 {
		t.Fatal("probe started after shutdown")
	}
}

// Exercise Alpha's real proxy HTTP/TLS transport, not only a fake StatusProber.
func TestSmartResponseRealTLSProbe(t *testing.T) {
	var status atomic.Int32
	server := httptest.NewTLSServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.URL.Path != "/robots.txt" {
			t.Errorf("unexpected probe path: %s", r.URL.Path)
		}
		switch status.Load() {
		case 429:
			w.Header().Set("Retry-After", "120")
		case 302:
			w.Header().Set("Location", "https://unrelated.invalid/")
		}
		w.WriteHeader(int(status.Load()))
		_, _ = w.Write([]byte("probe response"))
	}))
	defer server.Close()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := ca.AddCertificate(string(cert)); err != nil {
		t.Fatal(err)
	}
	defer ca.ResetCertificate()
	proxy := adapter.NewProxy(outbound.NewDirect())
	s := newResponseSmart(t, proxy)
	host := strings.TrimPrefix(server.URL, "https://")
	m := &C.Metadata{Host: host, WildcardTarget: host, SmartTarget: host}
	for _, tc := range []struct {
		status int
		action smart.VerdictAction
	}{
		{403, smart.VerdictRecord}, {429, smart.VerdictRecord}, {302, smart.VerdictIgnore}, {200, smart.VerdictReachable},
	} {
		status.Store(int32(tc.status))
		verdict := s.probeVerdict(proxy, host)
		if verdict.Action != tc.action {
			t.Fatalf("status=%d verdict=%+v", tc.status, verdict)
		}
		if tc.status == 429 && verdict.TTL != 2*time.Minute {
			t.Fatal("real Retry-After ignored")
		}
		s.applyNodeAnswer(m, proxy.Name(), verdict)
		nodes := s.responseCandidates(m, []C.Proxy{proxy})
		if tc.action == smart.VerdictRecord && len(nodes) != 0 {
			t.Fatal("real refusal not recorded")
		}
		if tc.action == smart.VerdictIgnore && len(nodes) != 0 {
			t.Fatal("cross-host redirect cleared avoidance")
		}
		if tc.action == smart.VerdictReachable && len(nodes) != 1 {
			t.Fatal("real success did not recover")
		}
	}
}

func TestSmartResponseDiscoveryFollowerRejectsOldWinner(t *testing.T) {
	var badDials atomic.Int32
	bad := newResponseProxy("old-winner", 403)
	bad.dial = func(context.Context, *C.Metadata) (C.Conn, error) { badDials.Add(1); return &stubConn{}, nil }
	good := newResponseProxy("eligible", 200)
	s := newResponseSmart(t, bad, good)
	m := &C.Metadata{Host: "follower.example.com"}
	key, domain := routeKey(m), routeDomain(m)
	s.routeTable.UpdateLatency(key, domain, bad.Name(), 10)
	// The leader selected this node before its HTTP refusal arrived.
	ds := &discoveryState{done: make(chan struct{}), proxy: bad, leaderCancel: func() {}}
	close(ds.done)
	s.probeCoordinator.discoveries[discoveryKey{routeKey: key, domain: domain}] = ds
	clone := m.Clone()
	clone.WildcardTarget = domain
	s.applyNodeAnswer(clone, bad.Name(), smart.ClassifyResponse(403, nil, nil, time.Now()))
	conn, err := s.tcpRoute(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	best, _ := s.routeTable.GetBestProxy(key, domain)
	if best != good.Name() || badDials.Load() != 0 {
		t.Fatalf("follower bypassed avoidance: best=%s bad dials=%d", best, badDials.Load())
	}
	if s.routeTable.ProxyFailedCount(key, domain, bad.Name()) != 0 {
		t.Fatal("eligibility rejection counted as transport failure")
	}
}
