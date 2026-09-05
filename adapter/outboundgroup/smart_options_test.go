package outboundgroup

import (
	"fmt"
	C "github.com/metacubex/mihomo/constant"
	"math"
	"syscall"
	"testing"

	"github.com/metacubex/mihomo/component/smart"
)

func TestSmartPriorityRouting(t *testing.T) {
	const key, domain = "TARGET:example.com", "example.com"
	rt := smart.NewRouteTable(10)
	s := &Smart{routeTable: rt}
	applyPolicyPriority(s, `^preferred\d+$:3;bad:NaN;bad:Inf`)
	if got := s.getPriorityFactor("preferred1"); got != 3 {
		t.Fatalf("regex factor = %v", got)
	}
	if got := s.getPriorityFactor("bad"); got != 1 {
		t.Fatalf("invalid factor accepted: %v", got)
	}
	proxies := makeStubProxies("fast", "preferred1")
	rt.UpdateLatency(key, domain, "fast", 100)
	rt.UpdateLatency(key, domain, "preferred1", 200)
	// Score ranking must use policy before truncation, not reorder a filtered list.
	for _, ttfb := range []bool{false, true} {
		if ttfb {
			rt.UpdateTTFB(key, domain, "fast", 100)
			rt.UpdateTTFB(key, domain, "preferred1", 200)
		}
		got := s.rankCandidates(key, domain, proxies, nil)
		if got[0].Name() != "preferred1" {
			t.Fatalf("ttfb=%v ranking=%v", ttfb, orderedNames(got))
		}
	}
	if got := s.rankCandidates(key, domain, proxies, proxies[0]); got[0].Name() != "fast" {
		t.Fatal("fresh best must retain head start")
	}
	// UDP and discovery without aggregation share this preranking path.
	if got := rt.PreRankLatency(namesOf(proxies), nil, key, domain, s.getPriorityFactor); got[0] != "preferred1" {
		t.Fatalf("latency rank: %v", got)
	}
	if got := s.exploreOrder(proxies, proxies, key, domain); got[0].Name() != "preferred1" {
		t.Fatalf("discovery: %v", orderedNames(got))
	}
	rt.SetProxyAttrs(map[string]smart.ProxyAttributes{"fast": {Score: 2}, "preferred1": {Score: 1}})
	if got := s.exploreOrder(proxies, proxies, key, domain); got[0].Name() != "preferred1" {
		t.Fatalf("aggregate discovery: %v", orderedNames(got))
	}
	// A configured preference must survive cold-start randomization.
	for i := 0; i < 20; i++ {
		if got := rt.PreRankLatency(namesOf(proxies), nil, "TARGET:cold", "cold", s.getPriorityFactor); got[0] != "preferred1" {
			t.Fatalf("cold rank: %v", got)
		}
	}
}

func TestSmartPriorityBeforeTTFBCap(t *testing.T) {
	rt := smart.NewRouteTable(10)
	s := &Smart{routeTable: rt}
	applyPolicyPriority(s, "preferred:100")
	names := []string{"preferred"}
	rt.UpdateTTFB("key", "domain", "preferred", 1000)
	for i := 0; i < 8; i++ {
		n := fmt.Sprint(i)
		names = append(names, n)
		rt.UpdateTTFB("key", "domain", n, 100)
	}
	got := rt.RankByScore(names, nil, "key", "domain", s.getPriorityFactor)
	if len(got) != smart.MaxTTFBProxiesPerRank || got[0] != "preferred" {
		t.Fatalf("rank: %v", got)
	}
}

func TestSmartSampleRate(t *testing.T) {
	for _, rate := range []float64{0, 1} {
		s := &Smart{sampleRate: rate}
		for i := 0; i < 100; i++ {
			if !s.sampleConnection() {
				t.Fatalf("default/full rate %v dropped sample", rate)
			}
		}
	}
	s := &Smart{sampleRate: 0.25}
	sampled := 0
	for i := 0; i < 10000; i++ {
		if s.sampleConnection() {
			sampled++
		}
	}
	if sampled < 2000 || sampled > 3000 {
		t.Fatalf("25%% sampled %d/10000", sampled)
	}
	for _, rate := range []float64{-1, 1.1, math.NaN(), math.Inf(1)} {
		if _, err := NewSmart(GroupCommonOption{}, SmartOption{SampleRate: rate}, nil, nil); err == nil {
			t.Fatalf("accepted rate %v", rate)
		}
	}
}

// Drive the actual read/close callbacks: unselected connections must still
// penalize resets, while only selected connections contribute TTFB samples.
func TestSmartSampledConnectionFeedback(t *testing.T) {
	for _, rate := range []float64{1, math.SmallestNonzeroFloat64} {
		rt := smart.NewRouteTable(10)
		s := &Smart{routeTable: rt, sampleRate: rate}
		metadata := &C.Metadata{Host: "sample.example.com"}
		key, domain := routeKey(metadata), routeDomain(metadata)
		rt.UpdateLatency(key, domain, "proxy", 100)
		rt.IncrementUseCount(key, domain, "proxy")
		rt.SetBestProxyAndTCPProbed(key, domain, "proxy")
		conn := s.wrapTCPConn(&resetSampleConn{}, &stubProxy{name: "proxy"}, metadata, 100)
		_, _ = conn.Read(make([]byte, 1))
		_ = conn.Close()
		record := rt.Snapshot("test").Rows[0].Domains[0].Proxies["proxy"]
		if got := record.Attributes.TTFB > 0; got != (rate == 1) {
			t.Fatalf("rate=%v TTFB=%d", rate, record.Attributes.TTFB)
		}
		if record.Attributes.FailedCount != 0.4 {
			t.Fatalf("rate=%v lost failure: %v", rate, record.Attributes.FailedCount)
		}
		if record.UseCount != 1 || record.Attributes.Latency != 100 {
			t.Fatalf("sampling changed routing data: %+v", record)
		}
	}
}

type resetSampleConn struct{ stubConn }

func (c *resetSampleConn) Read(b []byte) (int, error) { return 0, syscall.ECONNRESET }
