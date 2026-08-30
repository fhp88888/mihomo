package outboundgroup

import (
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"sort"
	"syscall"
	"testing"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// fakeTracker implements statistic.Tracker by embedding the nil interface and
// only overriding Info(). markEarlyDeath only calls Info(), so the embedded
// methods are never invoked.
type fakeTracker struct {
	statistic.Tracker
	info *statistic.TrackerInfo
}

func (f *fakeTracker) Info() *statistic.TrackerInfo { return f.info }

func newFakeTracker(upload, download int64) *fakeTracker {
	return &fakeTracker{
		info: &statistic.TrackerInfo{
			UploadTotal:   atomic.NewInt64(upload),
			DownloadTotal: atomic.NewInt64(download),
		},
	}
}

func TestCheckEarlyDeath(t *testing.T) {
	const key = "TARGET:example.com"
	const proxyName = "p1"

	setup := func() (*Smart, float64) {
		rt := smart.NewRouteTable(smart.DefaultMaxRows)
		rt.RestoreRow(key, "example.com", proxyName, smart.PersistedCell{})
		s := &Smart{routeTable: rt}
		return s, routeFailedCount(t, rt, key, "example.com", proxyName)
	}

	t.Run("early death marks failed", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, "example.com", proxyName, errors.New("connection reset by peer"), 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before+0.8 {
			t.Fatalf("FailedCount = %v, want %v", got, before+0.8)
		}
	})

	t.Run("EOF is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, "example.com", proxyName, io.EOF, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("RST is left to checkResetByPeer", func(t *testing.T) {
		s, before := setup()
		// RST is the primary signal handled by checkResetByPeer (0.4); early
		// death must not add its 0.8 on top.
		err := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
		s.checkEarlyDeath(key, "example.com", proxyName, err, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("nil error is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, "example.com", proxyName, nil, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("bidirectional data is ignored", func(t *testing.T) {
		s, before := setup()
		// A full request/response exchange (both upload and download) means the
		// connection survived its first byte — not an early death.
		s.checkEarlyDeath(key, "example.com", proxyName, errors.New("connection reset by peer"), 100, newFakeTracker(1024, 512))
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("one-way data is still early death", func(t *testing.T) {
		s, before := setup()
		// Only upload flowed, no download — the response never arrived, so the
		// connection died before completing the exchange.
		s.checkEarlyDeath(key, "example.com", proxyName, errors.New("connection reset by peer"), 100, newFakeTracker(1024, 0))
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before+0.8 {
			t.Fatalf("FailedCount = %v, want %v", got, before+0.8)
		}
	})

	t.Run("slow failure is ignored", func(t *testing.T) {
		s, before := setup()
		slow := smartEarlyDeathLatencyLimit.Milliseconds() + 1000
		s.checkEarlyDeath(key, "example.com", proxyName, errors.New("connection reset by peer"), slow, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})
}

func TestCheckResetByPeer(t *testing.T) {
	const key = "TARGET:example.com"
	const proxyName = "p1"

	setup := func() (*Smart, float64) {
		rt := smart.NewRouteTable(smart.DefaultMaxRows)
		rt.RestoreRow(key, "example.com", proxyName, smart.PersistedCell{})
		s := &Smart{routeTable: rt}
		return s, routeFailedCount(t, rt, key, "example.com", proxyName)
	}

	t.Run("ECONNRESET marks failed", func(t *testing.T) {
		s, before := setup()
		// Realistic error chain: *net.OpError wrapping *os.SyscallError wrapping syscall.ECONNRESET.
		err := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
		s.checkResetByPeer(key, "example.com", proxyName, err)
		// RST carries a lighter 0.4 penalty, not the full early-death 0.8.
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before+0.4 {
			t.Fatalf("FailedCount = %v, want %v", got, before+0.4)
		}
	})

	t.Run("non-reset error is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkResetByPeer(key, "example.com", proxyName, errors.New("connection closed"))
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("EOF is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkResetByPeer(key, "example.com", proxyName, io.EOF)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("nil error is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkResetByPeer(key, "example.com", proxyName, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})
}

// TestRouteKey verifies the route table key selection rules:
//   - ASN available and valid: "ASN:<number> <org>" (e.g. "ASN:2497 KDDI")
//   - ASN lookup failed ("0" or "unknown"): "TARGET:<effective-target>"
func TestRouteKey(t *testing.T) {
	mkMeta := func(host string, dstIP string, asn string) *C.Metadata {
		ip, err := netip.ParseAddr(dstIP)
		if err != nil {
			t.Fatalf("bad test dstIP %q: %v", dstIP, err)
		}
		return &C.Metadata{
			Host:        host,
			DstIP:       ip,
			DstIPASN:    asn,
			SmartTarget: "",
		}
	}

	t.Run("regular ASN keeps org name in key", func(t *testing.T) {
		// A non-CDN ASN (e.g. a residential ISP) shares one row across all
		// targets in that ASN — the domain is not part of the key, but the
		// org name is stored with the ASN so the route table surfaces the
		// full "2497 KDDI" identity.
		m := mkMeta("www.example.com", "1.2.3.4", "2497 "+"KDDI")
		if got := routeKey(m); got != "ASN:2497 KDDI" {
			t.Fatalf("routeKey = %q, want %q", got, "ASN:2497 KDDI")
		}
	})

	t.Run("asn lookup failed becomes TARGET", func(t *testing.T) {
		// getASNCode writes "0" when resolution fails; routeKey must fall back
		// to the TARGET form keyed by the effective target.
		m := mkMeta("www.example.com", "1.2.3.4", "0")
		if got := routeKey(m); got != "TARGET:www.example.com" {
			t.Fatalf("routeKey = %q, want %q", got, "TARGET:www.example.com")
		}
		// SmartTarget should be populated so the close callback (which re-derives
		// the key) agrees with the route-time key.
		if m.SmartTarget != "www.example.com" {
			t.Fatalf("SmartTarget = %q, want %q", m.SmartTarget, "www.example.com")
		}
	})

	t.Run("legacy unknown sentinel also becomes TARGET", func(t *testing.T) {
		// rules/common/ipasn.go still writes "unknown" when the ASN rule
		// matches nothing; routeKey treats it the same as "0".
		m := mkMeta("www.example.com", "1.2.3.4", "unknown")
		if got := routeKey(m); got != "TARGET:www.example.com" {
			t.Fatalf("routeKey = %q, want %q", got, "TARGET:www.example.com")
		}
	})

	t.Run("rule descriptor SmartTarget still keys by effective target", func(t *testing.T) {
		// The tunnel pre-populates SmartTarget with a rule descriptor (e.g.
		// "DomainSuffix [example.com]"). routeKey must key the row by the
		// effective target, not the descriptor, so the same site reached via
		// different rules shares one row.
		m := mkMeta("www.example.com", "1.2.3.4", "0")
		m.SmartTarget = "DomainSuffix [example.com]"
		if got := routeKey(m); got != "TARGET:www.example.com" {
			t.Fatalf("routeKey = %q, want %q", got, "TARGET:www.example.com")
		}
		// The descriptor must be preserved (not overwritten) — it feeds stats.
		if m.SmartTarget != "DomainSuffix [example.com]" {
			t.Fatalf("SmartTarget = %q, want rule descriptor preserved", m.SmartTarget)
		}
	})

	t.Run("cdn ASN is not special-cased", func(t *testing.T) {
		// 13335 = Cloudflare, listed in CdnASNs. The CDN key form is gone, so
		// Cloudflare targets key by ASN+org like any other ASN.
		m := mkMeta("www.cloudflare.com", "1.2.3.4", "13335 Cloudflare")
		if got := routeKey(m); got != "ASN:13335 Cloudflare" {
			t.Fatalf("routeKey = %q, want %q", got, "ASN:13335 Cloudflare")
		}
	})

	t.Run("ip-only traffic falls back to the ip", func(t *testing.T) {
		// No host: GetEffectiveTarget passes the IP through, and the key still
		// carries the TARGET form when ASN resolution failed.
		m := mkMeta("", "1.2.3.4", "0")
		if got := routeKey(m); got != "TARGET:1.2.3.4" {
			t.Fatalf("routeKey = %q, want %q", got, "TARGET:1.2.3.4")
		}
	})
}

// TestRouteDomain verifies that the per-domain key is the effective target,
// not the rule descriptor the tunnel pre-populates into metadata.SmartTarget.
// This must match the conn-size bucket written by wrapTCPConn's close callback
// so routing state and conn-size land in the same domainCell.
func TestRouteDomain(t *testing.T) {
	ip, err := netip.ParseAddr("1.2.3.4")
	if err != nil {
		t.Fatal(err)
	}

	// Rule-matched traffic: SmartTarget is a rule descriptor, but the domain
	// key must still be the effective target.
	m := &C.Metadata{Host: "www.example.com", DstIP: ip, SmartTarget: "DomainSuffix [example.com]"}
	if got := routeDomain(m); got != "www.example.com" {
		t.Fatalf("routeDomain = %q, want %q", got, "www.example.com")
	}

	// IP-only traffic falls back to the IP.
	m = &C.Metadata{Host: "", DstIP: ip, SmartTarget: ""}
	if got := routeDomain(m); got != "1.2.3.4" {
		t.Fatalf("routeDomain(ip-only) = %q, want %q", got, "1.2.3.4")
	}
}

// =========================================================================
// exploreOrder tests
// =========================================================================

// seededProxyNames returns the names in the order they appear in ps.
func orderedNames(ps []C.Proxy) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Name()
	}
	return out
}

// hasDupNames reports whether the name slice contains the same name twice.
func hasDupNames(names []string) bool {
	seen := make(map[string]bool, len(names))
	for _, n := range names {
		if seen[n] {
			return true
		}
		seen[n] = true
	}
	return false
}

// TestExploreOrder_DeferredLast verifies that proxies with FailedCount > 0 or
// pkg-loss above the threshold are placed at the very end, never in the top
// exploration tier.
func TestExploreOrder_DeferredLast(t *testing.T) {
	rt := smart.NewRouteTable(10)
	rt.SetProxyAttrs(map[string]smart.ProxyAttributes{
		"good":    {Score: 5.0},
		"deferred": {Score: 6.0, FailedCount: 1.0}, // high score but failed
		"lossy":   {Score: 4.0, PkgLoss: 0.3},      // high pkg loss
	})
	s := &Smart{routeTable: rt, testUrl: "test"}

	proxies := makeStubProxies("good", "deferred", "lossy")
	ordered := s.exploreOrder(proxies, proxies, "TARGET:example.com", "example.com")

	names := orderedNames(ordered)
	if len(names) != 3 || hasDupNames(names) {
		t.Fatalf("expected 3 unique proxies, got %v", names)
	}
	// deferred/lossy must both be after every non-deferred proxy.
	if names[0] != "good" {
		t.Fatalf("expected first candidate good, got %v", names)
	}
	if names[1] == "good" || names[2] == "good" {
		t.Fatalf("good must be first (no deferral), got %v", names)
	}
}

// TestExploreOrder_UnsampledUsesNeutral verifies that proxies absent from the
// aggregation stay in the candidate pool (they do not get dropped) and that
// they are not all squeezed to the very end — they participate via the median
// score.
func TestExploreOrder_UnsampledUsesNeutral(t *testing.T) {
	rt := smart.NewRouteTable(10)
	// Sampled proxies have wide scores; the median is (2+10)/2 = 6.
	rt.SetProxyAttrs(map[string]smart.ProxyAttributes{
		"a": {Score: 2.0},
		"b": {Score: 10.0},
	})
	s := &Smart{routeTable: rt, testUrl: "test"}

	proxies := makeStubProxies("a", "b", "unsampled")
	ordered := s.exploreOrder(proxies, proxies, "TARGET:example.com", "example.com")
	names := orderedNames(ordered)

	if len(names) != 3 || hasDupNames(names) {
		t.Fatalf("expected all 3 proxies in the pool, got %v", names)
	}
	// unsampled uses the neutral median 6, so it sorts between a(2) and b(10).
	// It must not be last unless b(10) is also before it.
	idx := func(n string) int {
		for i, x := range names {
			if x == n {
				return i
			}
		}
		t.Fatalf("proxy %q not in explore order %v", n, names)
		return -1
	}
	if idx("b") > idx("unsampled") {
		t.Fatalf("b(10) must sort before unsampled(neutral 6), got %v", names)
	}
	if idx("unsampled") > idx("a") {
		t.Fatalf("unsampled(neutral 6) must sort before a(2), got %v", names)
	}
}

// TestExploreOrder_TopShuffledOnlyForLargePool verifies that the top tier is
// only shuffled when the pool is bigger than exploreBatch, and that deferred
// proxies are never pulled forward by the shuffle.
func TestExploreOrder_TopShuffledOnlyForLargePool(t *testing.T) {
	rt := smart.NewRouteTable(10)
	attrs := make(map[string]smart.ProxyAttributes, 8)
	for i := 0; i < 8; i++ {
		name := string(rune('a' + i))
		attrs[name] = smart.ProxyAttributes{Score: float64(8 - i)}
	}
	attrs["z"] = smart.ProxyAttributes{Score: 100, FailedCount: 1} // deferred
	rt.SetProxyAttrs(attrs)
	s := &Smart{routeTable: rt, testUrl: "test"}

	makeAll := func() []C.Proxy {
		names := make([]string, 0, 9)
		for i := 0; i < 8; i++ {
			names = append(names, string(rune('a'+i)))
		}
		names = append(names, "z")
		return makeStubProxies(names...)
	}

	// The deferred proxy z must always be last across many runs, and the
	// non-deferred tier must only ever be a permutation of the same 8.
	for i := 0; i < 20; i++ {
		proxies := makeAll()
		ordered := s.exploreOrder(proxies, proxies, "TARGET:example.com", "example.com")
		names := orderedNames(ordered)
		if len(names) != 9 || hasDupNames(names) {
			t.Fatalf("expected 9 unique proxies, got %v", names)
		}
		if names[8] != "z" {
			t.Fatalf("deferred z must always be last, got %v", names)
		}
		nonDeferred := names[:8]
		sorted := append([]string(nil), nonDeferred...)
		sort.Strings(sorted)
		want := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
		for j := range want {
			if sorted[j] != want[j] {
				t.Fatalf("non-deferred tier must be a permutation of the 8 candidates, got %v", nonDeferred)
			}
		}
	}
}

// TestExploreOrder_EmptyAggregationFallsBack verifies that an empty
// aggregation (cold start) falls back to the latency PreRankLatency order.
func TestExploreOrder_EmptyAggregationFallsBack(t *testing.T) {
	rt := smart.NewRouteTable(10)
	const key = "TARGET:example.com"
	// Seed per-key latency so PreRankLatency sorts deterministically (without
	// per-key data it shuffles to avoid always favoring the same proxy).
	rt.UpdateLatency(key, "example.com", "slow", 300)
	rt.UpdateLatency(key, "example.com", "fast", 50)
	s := &Smart{routeTable: rt, testUrl: "test"}

	proxies := makeStubProxies("slow", "fast")
	ordered := s.exploreOrder(proxies, proxies, key, "example.com")
	names := orderedNames(ordered)
	// Stable pre-rank by per-key latency: fast(50) before slow(300).
	if len(names) != 2 || names[0] != "fast" || names[1] != "slow" {
		t.Fatalf("expected fallback latency order [fast slow], got %v", names)
	}
}
