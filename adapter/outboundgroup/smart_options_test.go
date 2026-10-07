package outboundgroup

import (
	"math"
	"syscall"
	"testing"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
)

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

func TestShouldRestoreRouteCellFiltersEvictedDomains(t *testing.T) {
	meta := map[string]smart.PersistedRow{
		"ASN:64512": {Domains: map[string]smart.PersistedDomain{
			"live.example.com": {},
		}},
	}
	liveRows := map[string]struct{}{"ASN:64512": {}}
	if !shouldRestoreRouteCell(liveRows, meta, "ASN:64512", "live.example.com") {
		t.Fatal("live domain was filtered during restore")
	}
	if shouldRestoreRouteCell(liveRows, meta, "ASN:64512", "evicted.example.com") {
		t.Fatal("evicted domain was restored from a stale route-cell record")
	}
	if shouldRestoreRouteCell(liveRows, meta, "ASN:evicted", "old.example.com") {
		t.Fatal("evicted row was restored from stale metadata")
	}
	if !shouldRestoreRouteCell(nil, meta, "TARGET:legacy.example.com", "legacy.example.com") {
		t.Fatal("legacy row without metadata should remain restorable")
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
