package smart

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

// testDomain is the per-domain key used by tests that exercise the
// domain-aware best/tcpProbed routing state.
const testDomain = "example.com"

func TestNewRouteTable(t *testing.T) {
	rt := NewRouteTable(100)
	if rt == nil {
		t.Fatal("NewRouteTable returned nil")
	}
	if rt.maxRows != 100 {
		t.Fatalf("expected maxRows=100, got %d", rt.maxRows)
	}
}

func TestSetAndGetBestProxy(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// Initially no best proxy
	if _, ok := rt.GetBestProxy(key, testDomain); ok {
		t.Fatal("expected no best proxy for new key")
	}

	rt.SetBestProxy(key, testDomain, "proxy-a")
	name, ok := rt.GetBestProxy(key, testDomain)
	if !ok {
		t.Fatal("expected best proxy after set")
	}
	if name != "proxy-a" {
		t.Fatalf("expected proxy-a, got %s", name)
	}
}

func TestTCPProbed(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	if rt.IsTCPProbed(key, testDomain) {
		t.Fatal("expected false for new key")
	}

	rt.SetTCPProbed(key, testDomain)
	if !rt.IsTCPProbed(key, testDomain) {
		t.Fatal("expected true after SetTCPProbed")
	}
}

func TestEMALatency(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	// First sample: writes directly (does not average with zero)
	rt.UpdateLatency(key, proxy, 100)
	snap := rt.Snapshot("test")
	if len(snap.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(snap.Rows))
	}
	rec := snap.Rows[0].Proxies[proxy]
	if rec.Attributes.Latency != 100 {
		t.Fatalf("expected first latency=100, got %d", rec.Attributes.Latency)
	}

	// Second sample: EMA = old*3/4 + new*1/4 = 100*3/4 + 40*1/4 = 85
	rt.UpdateLatency(key, proxy, 40)
	snap = rt.Snapshot("test")
	rec = snap.Rows[0].Proxies[proxy]
	expected := int64(float64(100)*3.0/4.0 + float64(40)/4.0)
	if rec.Attributes.Latency != expected {
		t.Fatalf("expected EMA latency=%d, got %d", expected, rec.Attributes.Latency)
	}
}

func TestEMAPkgLoss(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	rt.UpdatePkgLoss(key, proxy, 0.01)
	snap := rt.Snapshot("test")
	if snap.Rows[0].Proxies[proxy].Attributes.PkgLoss != 0.01 {
		t.Fatal("first pkg_loss should be 0.01")
	}

	rt.UpdatePkgLoss(key, proxy, 0.04)
	snap = rt.Snapshot("test")
	expected := 0.01*3.0/4.0 + 0.04/4.0
	got := snap.Rows[0].Proxies[proxy].Attributes.PkgLoss
	if got < expected-0.001 || got > expected+0.001 {
		t.Fatalf("expected EMA pkg_loss=%.4f, got %.4f", expected, got)
	}
}

func TestEMAJitter(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	// First TTFB sample: no baseline yet, jitter stays 0.
	rt.UpdateTTFB(key, proxy, 100)
	snap := rt.Snapshot("test")
	rec := snap.Rows[0].Proxies[proxy]
	if rec.Attributes.Jitter != 0 {
		t.Fatalf("expected jitter=0 on first sample, got %.2f", rec.Attributes.Jitter)
	}
	if rec.Attributes.TTFB != 100 {
		t.Fatalf("expected first ttfb=100, got %d", rec.Attributes.TTFB)
	}

	// Second sample: previous EMA ttfb was 100, new sample 40.
	// deviation = |40-100| = 60, first jitter sample writes directly.
	rt.UpdateTTFB(key, proxy, 40)
	snap = rt.Snapshot("test")
	rec = snap.Rows[0].Proxies[proxy]
	if rec.Attributes.Jitter != 60 {
		t.Fatalf("expected first jitter=60, got %.2f", rec.Attributes.Jitter)
	}
	// EMA ttfb = 100*3/4 + 40/4 = 85
	if rec.Attributes.TTFB != 85 {
		t.Fatalf("expected EMA ttfb=85, got %d", rec.Attributes.TTFB)
	}

	// Third sample: previous EMA ttfb was 85, new sample 100.
	// deviation = |100-85| = 15. Jitter EMA = 60*3/4 + 15/4 = 48.75.
	rt.UpdateTTFB(key, proxy, 100)
	snap = rt.Snapshot("test")
	rec = snap.Rows[0].Proxies[proxy]
	expectedJitter := 60*3.0/4.0 + 15.0/4.0
	if rec.Attributes.Jitter < expectedJitter-0.01 || rec.Attributes.Jitter > expectedJitter+0.01 {
		t.Fatalf("expected jitter=%.2f, got %.2f", expectedJitter, rec.Attributes.Jitter)
	}
}

func TestEMASpeed(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	rt.UpdateSpeed(key, proxy, 10485760)
	snap := rt.Snapshot("test")
	if snap.Rows[0].Proxies[proxy].Attributes.Speed != 10485760 {
		t.Fatal("first speed should be 10485760")
	}

	rt.UpdateSpeed(key, proxy, 20971520)
	snap = rt.Snapshot("test")
	expected := 10485760.0*3.0/4.0 + 20971520.0/4.0
	got := snap.Rows[0].Proxies[proxy].Attributes.Speed
	if got < expected-1 || got > expected+1 {
		t.Fatalf("expected EMA speed=%.0f, got %.0f", expected, got)
	}
}

func TestIncrementUseCount(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	rt.IncrementUseCount(key, proxy)
	rt.IncrementUseCount(key, proxy)
	snap := rt.Snapshot("test")
	if snap.Rows[0].Proxies[proxy].UseCount != 2 {
		t.Fatalf("expected use_count=2, got %d", snap.Rows[0].Proxies[proxy].UseCount)
	}
}

func TestCalculateScore(t *testing.T) {
	cases := []struct {
		latency     int64
		speed       float64
		failedCount float64
		jitter      float64
		expect      float64
	}{
		// latency-only: score = 100 / (max(latency, 50) + max(jitter, 10)).
		// jitter=0 still applies the 10ms floor to the denominator.
		{latency: 100, speed: 0, failedCount: 0, jitter: 0, expect: 100.0 / (100.0 + 10.0)}, // 100/110
		{latency: 50, speed: 0, failedCount: 0, jitter: 0, expect: 100.0 / (50.0 + 10.0)},   // max(50,50)=50
		{latency: 200, speed: 0, failedCount: 0, jitter: 0, expect: 100.0 / (200.0 + 10.0)}, // 100/210
		// speed contributes log1p(speed / 0.5MBps)
		{latency: 100, speed: 10485760, failedCount: 0, jitter: 0, expect: 100.0/(100.0+10.0) + math.Log1p(20)},
		{latency: 0, speed: 1048576, failedCount: 0, jitter: 0, expect: math.Log1p(2)}, // latency=0 → latency term skipped
		{latency: -10, speed: 0, failedCount: 0, jitter: 0, expect: 0},
		// failedCount penalty: 0.8^n multiplier
		{latency: 100, speed: 0, failedCount: 1, jitter: 0, expect: 100.0 / (100.0 + 10.0) * 0.8},
		{latency: 100, speed: 0, failedCount: 3, jitter: 0, expect: 100.0 / (100.0 + 10.0) * math.Pow(0.8, 3)},
		// jitter inflates the latency denominator: 100 / (max(latency,50) + max(jitter,10))
		{latency: 100, speed: 0, failedCount: 0, jitter: 10, expect: 100.0 / (100.0 + 10.0)},   // max(10,10)=10
		{latency: 100, speed: 0, failedCount: 0, jitter: 5, expect: 100.0 / (100.0 + 10.0)},    // max(5,10)=10, same floor
		{latency: 100, speed: 0, failedCount: 0, jitter: 50, expect: 100.0 / (100.0 + 50.0)},   // 100/150
		{latency: 100, speed: 0, failedCount: 0, jitter: 200, expect: 100.0 / (100.0 + 200.0)}, // 100/300
		// jitter combined with speed
		{latency: 100, speed: 1048576, failedCount: 0, jitter: 50, expect: 100.0/(100.0+50.0) + math.Log1p(2)},
		// latency=0 with jitter: latency term (and thus jitter) is skipped
		{latency: 0, speed: 1048576, failedCount: 0, jitter: 50, expect: math.Log1p(2)},
	}

	for _, tc := range cases {
		got := calculateScore(tc.latency, tc.speed, 0, tc.failedCount, tc.jitter, connSizeUnknown)
		if math.Abs(got-tc.expect) > 0.000001 {
			t.Fatalf("latency=%d speed=%.0f fail=%.1f jitter=%.1f: expected score %.6f, got %.6f", tc.latency, tc.speed, tc.failedCount, tc.jitter, tc.expect, got)
		}
	}
}

func TestCalculateScoreSkipsSpeedForSmallConnSize(t *testing.T) {
	// For a domain whose connections are smaller than 32kB, the speed term must
	// be skipped, leaving only the latency (+penalty) components.
	latencyOnly := 100.0 / (100.0 + 10.0) // latency=100, jitter=0 -> 100/110
	withSpeed := latencyOnly + math.Log1p(10485760.0/1024.0/1024.0/0.5)

	// connSize below the 32kB threshold: speed skipped.
	if got := calculateScore(100, 10485760, 0, 0, 0, 31.0); math.Abs(got-latencyOnly) > 0.000001 {
		t.Fatalf("small connSize: expected %.6f (speed skipped), got %.6f", latencyOnly, got)
	}
	// connSize exactly at the threshold (32kB): speed included.
	if got := calculateScore(100, 10485760, 0, 0, 0, 32.0); math.Abs(got-withSpeed) > 0.000001 {
		t.Fatalf("connSize at threshold: expected %.6f (speed included), got %.6f", withSpeed, got)
	}
	// connSize above the threshold: speed included.
	if got := calculateScore(100, 10485760, 0, 0, 0, 33.0); math.Abs(got-withSpeed) > 0.000001 {
		t.Fatalf("large connSize: expected %.6f (speed included), got %.6f", withSpeed, got)
	}
	// connSize unknown sentinel: speed included (domain-less callers).
	if got := calculateScore(100, 10485760, 0, 0, 0, connSizeUnknown); math.Abs(got-withSpeed) > 0.000001 {
		t.Fatalf("connSize unknown: expected %.6f (speed included), got %.6f", withSpeed, got)
	}
}

func TestRefreshScoresStoresNonEMA(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	rt.UpdateLatency(key, proxy, 100)
	rt.UpdateSpeed(key, proxy, 10485760)
	rt.RefreshScores(key, []string{proxy})
	snap := rt.Snapshot("test")
	got := snap.Rows[0].Proxies[proxy].Attributes.Score
	rec := snap.Rows[0].Proxies[proxy]
	atom := 100.0/(math.Max(float64(rec.Attributes.Latency), 50.0)+math.Max(rec.Attributes.Jitter, 10.0)) + math.Log1p(rec.Attributes.Speed/1024.0/1024.0/0.5)
	expected := atom // no per-proxy aggregation -> raw unblended score
	if math.Abs(got-expected) > 0.000001 {
		t.Fatalf("expected initial score %.6f, got %.6f", expected, got)
	}

	rt.UpdateLatency(key, proxy, 20)
	rt.UpdateSpeed(key, proxy, 20971520)
	rt.RefreshScores(key, []string{proxy})
	snap = rt.Snapshot("test")
	rec = snap.Rows[0].Proxies[proxy]
	atom = 100.0/(math.Max(float64(rec.Attributes.Latency), 50.0)+math.Max(rec.Attributes.Jitter, 10.0)) + math.Log1p(rec.Attributes.Speed/1024.0/1024.0/0.5)
	expected = atom
	if math.Abs(rec.Attributes.Score-expected) > 0.000001 {
		t.Fatalf("expected score from current latency/speed %.6f, got %.6f", expected, rec.Attributes.Score)
	}
}

func TestRankByScore(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	rt.UpdateLatency(key, "proxy-a", 100)
	rt.UpdateLatency(key, "proxy-b", 50)
	rt.UpdateLatency(key, "proxy-c", 200)
	rt.UpdateSpeed(key, "proxy-c", 10485760)
	// Give the domain a large connSize so the speed component is not skipped.
	rt.UpdateConnSize(key, testDomain, 2048)
	rt.RefreshScores(key, []string{"proxy-a", "proxy-b", "proxy-c"})

	proxies := []string{"proxy-a", "proxy-c", "proxy-b"}
	ranked := rt.RankByScore(proxies, nil, key, testDomain)
	// proxy-c=3.52 (200ms+10MiBps), proxy-b=1.67 (50ms), proxy-a=0.91 (100ms)
	expected := []string{"proxy-c", "proxy-b", "proxy-a"}
	for i := range expected {
		if ranked[i] != expected[i] {
			t.Fatalf("ranked[%d]: expected %s, got %s", i, expected[i], ranked[i])
		}
	}
}

func TestRankByScoreSkipsSpeedForSmallConnSize(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// proxy-a: fast latency (100ms), no speed.
	// proxy-c: slow latency (200ms) but huge speed — normally boosted above
	// proxy-a when the speed term counts.
	rt.UpdateLatency(key, "proxy-a", 100)
	rt.UpdateLatency(key, "proxy-c", 200)
	rt.UpdateSpeed(key, "proxy-c", 10485760)

	// Small connSize (< 32kB): speed is skipped, so proxy-a (faster latency)
	// ranks above proxy-c.
	rt.UpdateConnSize(key, "small.example.com", 10)
	ranked := rt.RankByScore([]string{"proxy-c", "proxy-a"}, nil, key, "small.example.com")
	if ranked[0] != "proxy-a" {
		t.Fatalf("small connSize: expected proxy-a first (speed skipped), got %v", ranked)
	}

	// Large connSize (>= 32kB): speed is included, so proxy-c's throughput
	// pushes it above proxy-a.
	rt.UpdateConnSize(key, "large.example.com", 2048)
	ranked = rt.RankByScore([]string{"proxy-c", "proxy-a"}, nil, key, "large.example.com")
	if ranked[0] != "proxy-c" {
		t.Fatalf("large connSize: expected proxy-c first (speed included), got %v", ranked)
	}
}

func TestRankByScoreDefaultConnSizeForUnseenDomain(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// proxy-a: fast latency (100ms), no speed.
	// proxy-c: slow latency (200ms) but huge speed — boosted above proxy-a when
	// the speed term counts.
	rt.UpdateLatency(key, "proxy-a", 100)
	rt.UpdateLatency(key, "proxy-c", 200)
	rt.UpdateSpeed(key, "proxy-c", 10485760)

	// A target with no domain record at all (no connSize sample, no cell)
	// must default to connSize 100 (> the 4kB threshold), so the speed term is
	// still counted and proxy-c outranks proxy-a.
	ranked := rt.RankByScore([]string{"proxy-c", "proxy-a"}, nil, key, "fresh.example.com")
	if ranked[0] != "proxy-c" {
		t.Fatalf("unseen domain: expected proxy-c first (default connSize keeps speed), got %v", ranked)
	}

	// Once a real (small) connSize is recorded, the speed term is skipped and
	// the ranking flips.
	rt.UpdateConnSize(key, "fresh.example.com", 1)
	ranked = rt.RankByScore([]string{"proxy-c", "proxy-a"}, nil, key, "fresh.example.com")
	if ranked[0] != "proxy-a" {
		t.Fatalf("small connSize: expected proxy-a first (speed skipped), got %v", ranked)
	}
}

func TestRankByScoreStableSort(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	rt.UpdateLatency(key, "proxy-a", 50)
	rt.UpdateLatency(key, "proxy-b", 50)
	rt.UpdateLatency(key, "proxy-c", 50)
	rt.RefreshScores(key, []string{"proxy-a", "proxy-b", "proxy-c"})

	proxies := []string{"proxy-c", "proxy-a", "proxy-b"}
	ranked := rt.RankByScore(proxies, nil, key, testDomain)
	for i := range proxies {
		if ranked[i] != proxies[i] {
			t.Fatalf("stable sort broken at [%d]: expected %s, got %s", i, proxies[i], ranked[i])
		}
	}
}

func TestRankByScoreWithHealthCheckFallback(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	rt.UpdateLatency(key, "proxy-a", 100)
	rt.RefreshScores(key, []string{"proxy-a"})

	healthCheck := func(name string) uint16 {
		switch name {
		case "proxy-b":
			return 50
		case "proxy-zero":
			return 0
		case "proxy-max":
			return 0xffff
		default:
			return 100
		}
	}

	proxies := []string{"proxy-zero", "proxy-a", "proxy-max", "proxy-b"}
	ranked := rt.RankByScore(proxies, healthCheck, key, testDomain)
	// proxy-b=1.67 (hc lat=50), proxy-a=0.91 (has sample, lat=100), proxy-zero=0, proxy-max=0
	expected := []string{"proxy-b", "proxy-a", "proxy-zero", "proxy-max"}
	for i := range expected {
		if ranked[i] != expected[i] {
			t.Fatalf("ranked[%d]: expected %s, got %s", i, expected[i], ranked[i])
		}
	}
}

func TestRankByScoreFailedOnlyProxyRanksLast(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// proxy-a has a latency sample; proxy-b has only failures (no sample).
	rt.UpdateLatency(key, "proxy-a", 100)
	rt.MarkFailed(key, "proxy-b", testDomain, 3.0)

	// A healthy-looking fallback latency must NOT rescue the failed-only proxy:
	// its failure penalty must be applied (score 0) so it ranks last.
	healthCheck := func(name string) uint16 { return 50 }

	ranked := rt.RankByScore([]string{"proxy-b", "proxy-a"}, healthCheck, key, testDomain)
	if ranked[0] != "proxy-a" {
		t.Fatalf("failed-only proxy ranked before sampled proxy: got %v", ranked)
	}
}

func TestRankByScorePrefersTTFBGroup(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// proxy-a: fast latency but slow TTFB.
	rt.UpdateLatency(key, "proxy-a", 100)
	rt.UpdateTTFB(key, "proxy-a", 80)
	// proxy-b: slow latency but fast TTFB.
	rt.UpdateLatency(key, "proxy-b", 200)
	rt.UpdateTTFB(key, "proxy-b", 50)

	// Once TTFB exists, ranking uses TTFB (not latency): proxy-b (50ms) beats
	// proxy-a (80ms) even though proxy-a's latency is lower.
	ranked := rt.RankByScore([]string{"proxy-a", "proxy-b"}, nil, key, testDomain)
	expected := []string{"proxy-b", "proxy-a"}
	for i := range expected {
		if ranked[i] != expected[i] {
			t.Fatalf("ranked[%d]: expected %s, got %s (full: %v)", i, expected[i], ranked[i], ranked)
		}
	}
}

func TestRankByScoreSkipsProxiesSlowerThanMinTTFB(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// proxy-a establishes minTTFB=50.
	rt.UpdateTTFB(key, "proxy-a", 50)
	// proxy-b has no TTFB and latency 200 -> 2*200 > 50 -> skipped.
	rt.UpdateLatency(key, "proxy-b", 200)
	// proxy-c has no TTFB and latency 20 -> 2*20 <= 50 -> kept.
	rt.UpdateLatency(key, "proxy-c", 20)
	// proxy-d has no TTFB and latency 40 -> 2*40 > 50 -> skipped (the current
	// prune threshold is latency > minTTFB/2, so even latency < minTTFB is cut).
	rt.UpdateLatency(key, "proxy-d", 40)

	ranked := rt.RankByScore([]string{"proxy-b", "proxy-c", "proxy-d", "proxy-a"}, nil, key, testDomain)

	if len(ranked) != 2 {
		t.Fatalf("expected 2 proxies (proxy-b, proxy-d skipped), got %d: %v", len(ranked), ranked)
	}
	// TTFB group (proxy-a) first, then the latency group (proxy-c).
	if ranked[0] != "proxy-a" || ranked[1] != "proxy-c" {
		t.Fatalf("unexpected order: %v", ranked)
	}
}

func TestRankByScoreLatencyGroupAfterTTFBGroup(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// proxy-a has TTFB=50; proxy-b has no TTFB but latency=20 (<= minTTFB*2,
	// so it survives the prune even though its raw latency is lower).
	rt.UpdateTTFB(key, "proxy-a", 50)
	rt.UpdateLatency(key, "proxy-b", 20)

	ranked := rt.RankByScore([]string{"proxy-b", "proxy-a"}, nil, key, testDomain)
	// TTFB group always comes first, even though proxy-b's raw latency is lower.
	expected := []string{"proxy-a", "proxy-b"}
	for i := range expected {
		if ranked[i] != expected[i] {
			t.Fatalf("ranked[%d]: expected %s, got %s (full: %v)", i, expected[i], ranked[i], ranked)
		}
	}
}

func TestRankByScoreMinTTFBFromWholeRow(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// proxy-b is NOT in the candidate list but establishes minTTFB=50.
	rt.UpdateTTFB(key, "proxy-b", 50)
	// proxy-a is in the list with TTFB=100.
	rt.UpdateTTFB(key, "proxy-a", 100)
	// proxy-c has no TTFB and latency=80, which is > 50 (whole-row min) -> skipped.
	rt.UpdateLatency(key, "proxy-c", 80)

	ranked := rt.RankByScore([]string{"proxy-a", "proxy-c"}, nil, key, testDomain)
	if len(ranked) != 1 || ranked[0] != "proxy-a" {
		t.Fatalf("expected only proxy-a (proxy-c skipped via whole-row minTTFB), got %v", ranked)
	}
}

func TestRankByScoreCapsTTFBGroup(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// Six proxies with a TTFB sample, clearly distinct first-byte times.
	// proxy-a is the fastest, proxy-f the slowest.
	for i, name := range []string{"proxy-a", "proxy-b", "proxy-c", "proxy-d", "proxy-e", "proxy-f"} {
		rt.UpdateTTFB(key, name, int64(50+10*i))
	}
	// A latency-only proxy with latency*2 <= minTTFB so it survives the prune.
	rt.UpdateLatency(key, "proxy-nottfb", 20)

	proxies := []string{"proxy-f", "proxy-e", "proxy-d", "proxy-c", "proxy-b", "proxy-a", "proxy-nottfb"}
	ranked := rt.RankByScore(proxies, nil, key, testDomain)

	if len(ranked) != MaxTTFBProxiesPerRank+1 {
		t.Fatalf("expected %d proxies (4 TTFB + 1 latency), got %d: %v", MaxTTFBProxiesPerRank+1, len(ranked), ranked)
	}
	// Only the 4 fastest TTFB proxies survive, score-descending.
	expected := []string{"proxy-a", "proxy-b", "proxy-c", "proxy-d", "proxy-nottfb"}
	for i := range expected {
		if ranked[i] != expected[i] {
			t.Fatalf("ranked[%d]: expected %s, got %s (full: %v)", i, expected[i], ranked[i], ranked)
		}
	}
}

func TestProxyFailedCount(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	if got := rt.ProxyFailedCount(key, "proxy-a"); got != 0 {
		t.Fatalf("ProxyFailedCount on absent proxy = %v, want 0", got)
	}

	// MarkFailed only records on an existing row (a proxy that failed was
	// being used, so its row already exists). Create it first.
	rt.UpdateLatency(key, "proxy-a", 100)
	rt.MarkFailed(key, "proxy-a", testDomain, 0.4)
	rt.MarkFailed(key, "proxy-a", testDomain, 0.8)
	if got := rt.ProxyFailedCount(key, "proxy-a"); math.Abs(got-1.2) > 0.001 {
		t.Fatalf("ProxyFailedCount = %v, want 1.2", got)
	}
	if got := rt.ProxyFailedCount("ASN:other", "proxy-a"); got != 0 {
		t.Fatalf("ProxyFailedCount on other key = %v, want 0", got)
	}
}

func TestSetBestProxyAndTCPProbed(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	rt.SetBestProxyAndTCPProbed(key, testDomain, "proxy-a")

	if best, ok := rt.GetBestProxy(key, testDomain); !ok || best != "proxy-a" {
		t.Fatalf("best = %q, %v; want proxy-a, true", best, ok)
	}
	if !rt.IsTCPProbed(key, testDomain) {
		t.Fatal("tcpProbed should be true after SetBestProxyAndTCPProbed")
	}
}

func TestSnapshotIncludesScore(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	rt.UpdateLatency(key, proxy, 100)
	rt.UpdateSpeed(key, proxy, 10485760)
	rt.RefreshScores(key, []string{proxy})
	snap := rt.Snapshot("test")
	got := snap.Rows[0].Proxies[proxy].Attributes.Score
	rec := snap.Rows[0].Proxies[proxy]
	atom := 100.0/(math.Max(float64(rec.Attributes.Latency), 50.0)+math.Max(rec.Attributes.Jitter, 10.0)) + math.Log1p(rec.Attributes.Speed/1024.0/1024.0/0.5)
	expected := atom // no per-proxy aggregation -> raw unblended score
	if math.Abs(got-expected) > 0.000001 {
		t.Fatalf("expected snapshot score %.6f, got %.6f", expected, got)
	}
}

func TestGetBestProxyIfFresh(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	rt.SetBestProxy(key, testDomain, "proxy-a")

	name, ok := rt.GetBestProxyIfFresh(key, testDomain, 20*time.Second)
	if !ok || name != "proxy-a" {
		t.Fatalf("expected fresh proxy-a, got %s ok=%v", name, ok)
	}

	rt.mu.Lock()
	rt.rows[key].domainTable[testDomain].lastUsed = time.Now().Add(-21 * time.Second).Unix()
	rt.mu.Unlock()

	if name, ok := rt.GetBestProxyIfFresh(key, testDomain, 20*time.Second); ok {
		t.Fatalf("expected stale best proxy to be unavailable, got %s", name)
	}
}

func TestPreRankLatency(t *testing.T) {
	rt := NewRouteTable(100)

	// Build known latencies: proxy-a avg=50, proxy-b avg=30, proxy-c avg=80
	rt.UpdateLatency("ASN:1", "proxy-a", 40)
	rt.UpdateLatency("ASN:2", "proxy-a", 60)
	rt.UpdateLatency("ASN:1", "proxy-b", 30)
	rt.UpdateLatency("ASN:2", "proxy-b", 30)
	rt.UpdateLatency("ASN:1", "proxy-c", 80)

	proxies := []string{"proxy-a", "proxy-c", "proxy-b"}
	ranked := rt.PreRankLatency(proxies, nil, "")

	// proxy-b (30) < proxy-a (50) < proxy-c (80)
	expected := []string{"proxy-b", "proxy-a", "proxy-c"}
	for i := range expected {
		if ranked[i] != expected[i] {
			t.Fatalf("pre-rank[%d]: expected %s, got %s", i, expected[i], ranked[i])
		}
	}
}

func TestPreRankStableSort(t *testing.T) {
	rt := NewRouteTable(100)

	// Same latency for all → preserves input order (stable sort)
	rt.UpdateLatency("ASN:1", "proxy-a", 50)
	rt.UpdateLatency("ASN:1", "proxy-b", 50)
	rt.UpdateLatency("ASN:1", "proxy-c", 50)

	proxies := []string{"proxy-c", "proxy-a", "proxy-b"}
	ranked := rt.PreRankLatency(proxies, nil, "")

	// All same latency, stable sort keeps input order
	for i := range proxies {
		if ranked[i] != proxies[i] {
			t.Fatalf("stable sort broken at [%d]: expected %s, got %s", i, proxies[i], ranked[i])
		}
	}
}

func TestPreRankWithHealthCheckFallback(t *testing.T) {
	rt := NewRouteTable(100)

	// proxy-d has no route table data, falls back to health check
	rt.UpdateLatency("ASN:1", "proxy-a", 50)
	rt.UpdateLatency("ASN:1", "proxy-b", 30)

	healthCheck := func(name string) uint16 {
		if name == "proxy-d" {
			return 20
		}
		return 100
	}

	proxies := []string{"proxy-a", "proxy-b", "proxy-d"}
	ranked := rt.PreRankLatency(proxies, healthCheck, "")

	// proxy-d (20 health) < proxy-b (30) < proxy-a (50)
	expected := []string{"proxy-d", "proxy-b", "proxy-a"}
	for i := range expected {
		if ranked[i] != expected[i] {
			t.Fatalf("pre-rank[%d]: expected %s, got %s (latencies: d=20hc, b=30, a=50)",
				i, expected[i], ranked[i])
		}
	}
}

func TestLRUEviction(t *testing.T) {
	rt := NewRouteTable(3) // small max for testing

	rt.SetBestProxy("ASN:1", testDomain, "p1")
	rt.SetBestProxy("ASN:2", testDomain, "p2")
	rt.SetBestProxy("ASN:3", testDomain, "p3")

	snap := rt.Snapshot("test")
	if snap.RowCount != 3 {
		t.Fatalf("expected 3 rows before eviction, got %d", snap.RowCount)
	}

	// Touch ASN:1 and ASN:2 to make ASN:3 LRU
	rt.TouchRow("ASN:1")
	rt.TouchRow("ASN:2")

	// Add a 4th row, should evict ASN:3 (least recently used)
	rt.SetBestProxy("ASN:4", testDomain, "p4")

	snap = rt.Snapshot("test")
	if snap.RowCount != 3 {
		t.Fatalf("expected 3 rows after eviction, got %d", snap.RowCount)
	}

	// ASN:3 should be gone
	if _, ok := rt.GetBestProxy("ASN:3", testDomain); ok {
		t.Fatal("ASN:3 should have been evicted")
	}

	// ASN:1, ASN:2, ASN:4 should remain
	for _, k := range []string{"ASN:1", "ASN:2", "ASN:4"} {
		if _, ok := rt.GetBestProxy(k, testDomain); !ok {
			t.Fatalf("%s should still be in table", k)
		}
	}
}

func TestSnapshotIsCopy(t *testing.T) {
	rt := NewRouteTable(100)
	rt.UpdateLatency("ASN:1", "proxy-a", 42)

	snap := rt.Snapshot("test")
	// Mutate snapshot
	snap.Rows[0].Proxies["proxy-a"] = ProxyRecord{Name: "hacked"}
	// Original should be unchanged
	snap2 := rt.Snapshot("test")
	if snap2.Rows[0].Proxies["proxy-a"].Name != "proxy-a" {
		t.Fatal("snapshot must be a deep copy — mutation should not affect original")
	}
}

func TestRemoveProxy(t *testing.T) {
	rt := NewRouteTable(100)
	rt.UpdateLatency("ASN:1", "proxy-a", 42)
	rt.UpdateLatency("ASN:1", "proxy-b", 30)
	rt.SetBestProxy("ASN:1", testDomain, "proxy-a")

	rt.RemoveProxy("proxy-a")

	snap := rt.Snapshot("test")
	proxies := snap.Rows[0].Proxies
	if _, ok := proxies["proxy-a"]; ok {
		t.Fatal("proxy-a should be removed")
	}
	if _, ok := proxies["proxy-b"]; !ok {
		t.Fatal("proxy-b should remain")
	}
	// best proxy should be cleared since it was removed
	if bp, _ := rt.GetBestProxy("ASN:1", testDomain); bp != "" {
		t.Fatalf("best proxy should be empty after removal, got %s", bp)
	}
}

func TestMarkFailed(t *testing.T) {
	rt := NewRouteTable(100)
	rt.UpdateLatency("ASN:1", "proxy-a", 42)
	rt.SetBestProxy("ASN:1", testDomain, "proxy-a")

	rt.MarkFailed("ASN:1", "proxy-a", testDomain, 1.0)

	// Best proxy should be cleared
	if bp, _ := rt.GetBestProxy("ASN:1", testDomain); bp != "" {
		t.Fatalf("best proxy should be empty after mark-failed, got %s", bp)
	}
}

func TestConcurrentSafety(t *testing.T) {
	rt := NewRouteTable(5000)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "ASN:" + string(rune('0'+id%10))
			rt.UpdateLatency(key, "proxy-a", int64(id))
			rt.UpdatePkgLoss(key, "proxy-a", 0.01)
			rt.UpdateSpeed(key, "proxy-a", 1000)
			rt.IncrementUseCount(key, "proxy-a")
			rt.SetBestProxy(key, testDomain, "proxy-a")
			rt.GetBestProxy(key, testDomain)
			rt.IsTCPProbed(key, testDomain)
			rt.PreRankLatency([]string{"proxy-a", "proxy-b"}, nil, "")
			rt.RefreshScores(key, []string{"proxy-a", "proxy-b"})
			rt.RankByScore([]string{"proxy-a", "proxy-b"}, nil, key, testDomain)
			rt.GetBestProxyIfFresh(key, testDomain, time.Second)
			rt.Snapshot("test")
		}(i)
	}

	wg.Wait()
	// No race detector complaints = pass
}

func TestSnapshotRowOrder(t *testing.T) {
	rt := NewRouteTable(100)
	rt.SetBestProxy("ASN:1", testDomain, "p1")
	rt.SetBestProxy("ASN:3", testDomain, "p3")
	rt.SetBestProxy("ASN:2", testDomain, "p2")
	time.Sleep(time.Second) // ensure distinct timestamp
	rt.TouchRow("ASN:2")    // make ASN:2 most recent

	snap := rt.Snapshot("test")
	// Should be sorted by LastUsed descending
	if snap.Rows[0].Key != "ASN:2" {
		t.Fatalf("expected ASN:2 first (most recent), got %s", snap.Rows[0].Key)
	}
}

func TestPerMetricHasSampleIndependent(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	// Update only latency — verify only latency flag is set
	rt.UpdateLatency(key, proxy, 100)
	rt.mu.RLock()
	cell := rt.rows[key].proxies[proxy]
	hasLat := cell.HasLatencySample
	hasSpd := cell.HasSpeedSample
	hasLoss := cell.HasPkgLossSample
	rt.mu.RUnlock()
	if !hasLat {
		t.Fatal("HasLatencySample should be true after UpdateLatency")
	}
	if hasSpd {
		t.Fatal("HasSpeedSample should be false after only latency update")
	}
	if hasLoss {
		t.Fatal("HasPkgLossSample should be false after only latency update")
	}

	// Update speed — verify both latency and speed flags now set
	rt.UpdateSpeed(key, proxy, 10485760)
	rt.mu.RLock()
	cell = rt.rows[key].proxies[proxy]
	hasSpd = cell.HasSpeedSample
	rt.mu.RUnlock()
	if !hasSpd {
		t.Fatal("HasSpeedSample should be true after UpdateSpeed")
	}
	if !rt.rows[key].proxies[proxy].HasLatencySample {
		t.Fatal("HasLatencySample should still be true")
	}

	// Update pkg_loss — verify all three flags now set
	rt.UpdatePkgLoss(key, proxy, 0.01)
	rt.mu.RLock()
	cell = rt.rows[key].proxies[proxy]
	hasLoss = cell.HasPkgLossSample
	rt.mu.RUnlock()
	if !hasLoss {
		t.Fatal("HasPkgLossSample should be true after UpdatePkgLoss")
	}
}

func TestEMASpeedIsCorrectWithPriorLatencySample(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	// Latency sample first (sets HasLatencySample, not HasSpeedSample)
	rt.UpdateLatency(key, proxy, 100)

	// Speed sample second — must be 100% of real speed, NOT 25% (EMA with 0)
	rt.UpdateSpeed(key, proxy, 10485760)
	snap := rt.Snapshot("test")
	got := snap.Rows[0].Proxies[proxy].Attributes.Speed
	if got != 10485760 {
		t.Fatalf("expected first speed=10485760 (100%% of value), got %.0f (%.1f%%)",
			got, got/10485760*100)
	}
}

func TestEMAPkgLossIsCorrectWithPriorLatencySample(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	// Latency sample first (sets HasLatencySample, not HasPkgLossSample)
	rt.UpdateLatency(key, proxy, 100)

	// PkgLoss sample second — must be 100% of real value, NOT 25%
	rt.UpdatePkgLoss(key, proxy, 0.08)
	snap := rt.Snapshot("test")
	got := snap.Rows[0].Proxies[proxy].Attributes.PkgLoss
	if got != 0.08 {
		t.Fatalf("expected first pkg_loss=0.08 (100%% of value), got %.4f", got)
	}
}

func TestPkgLossZeroUpdatesEMA(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	// Record initial loss
	rt.UpdatePkgLoss(key, proxy, 0.1)
	snap := rt.Snapshot("test")
	if snap.Rows[0].Proxies[proxy].Attributes.PkgLoss != 0.1 {
		t.Fatal("first pkg_loss should be 0.1")
	}

	// Update with 0% loss — EMA should decay toward 0
	rt.UpdatePkgLoss(key, proxy, 0.0)
	snap = rt.Snapshot("test")
	expected := 0.1*3.0/4.0 + 0.0/4.0 // = 0.075
	got := snap.Rows[0].Proxies[proxy].Attributes.PkgLoss
	if got < expected-0.001 || got > expected+0.001 {
		t.Fatalf("expected pkg_loss=%.4f after 0%% update, got %.4f", expected, got)
	}

	// Second 0% update — should decay further
	rt.UpdatePkgLoss(key, proxy, 0.0)
	snap = rt.Snapshot("test")
	expected = expected*3.0/4.0 + 0.0/4.0 // = 0.05625
	got = snap.Rows[0].Proxies[proxy].Attributes.PkgLoss
	if got < expected-0.001 || got > expected+0.001 {
		t.Fatalf("expected pkg_loss=%.4f after second 0%% update, got %.4f", expected, got)
	}
}

func TestBackwardCompatPersistedCell(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	// Simulate old-format PersistedCell (HasSample=true, no per-metric flags)
	pc := PersistedCell{
		Latency:     100,
		PkgLoss:     0.05,
		Speed:       10485760,
		UseCount:    5,
		FailedCount: 0,
		HasSample:   true,
		// HasLatencySample, HasPkgLossSample, HasSpeedSample are all false
		// (zero values from old-format JSON)
	}

	rt.RestoreRow(key, proxy, pc)

	rt.mu.RLock()
	cell := rt.rows[key].proxies[proxy]
	hasLat := cell.HasLatencySample
	hasLoss := cell.HasPkgLossSample
	hasSpd := cell.HasSpeedSample
	rt.mu.RUnlock()

	if !hasLat {
		t.Fatal("HasLatencySample should be inferred from old HasSample")
	}
	if !hasLoss {
		t.Fatal("HasPkgLossSample should be inferred from old HasSample")
	}
	if !hasSpd {
		t.Fatal("HasSpeedSample should be inferred from old HasSample")
	}
}

func TestIntegrationLatencySpeedLossFromSingleConnection(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	proxy := "proxy-a"

	// Simulate a full connection lifecycle:
	// 1. dialAndWrap / probeBatch writes connectTime once
	connectTime := int64(120)
	rt.UpdateLatency(key, proxy, connectTime)

	// 2. Speed sample from tracker (written on close)
	speed := 10485760.0 // 10 MiB/s
	rt.UpdateSpeed(key, proxy, speed)

	// 3. Loss rate from TCP stats (written on close, 0% loss — should still update)
	rt.UpdatePkgLoss(key, proxy, 0.0)

	// Verify all per-metric flags are set independently
	rt.mu.RLock()
	cell := rt.rows[key].proxies[proxy]
	if !cell.HasLatencySample {
		t.Fatal("HasLatencySample should be true after connectTime write")
	}
	if !cell.HasSpeedSample {
		t.Fatal("HasSpeedSample should be true after speed update")
	}
	if !cell.HasPkgLossSample {
		t.Fatal("HasPkgLossSample should be true after pkg_loss update")
	}
	rt.mu.RUnlock()

	// Verify final values
	snap := rt.Snapshot("test")
	rec := snap.Rows[0].Proxies[proxy]

	// Speed: first sample, no prior = raw value
	if rec.Attributes.Speed != speed {
		t.Fatalf("expected speed=%.0f, got %.0f", speed, rec.Attributes.Speed)
	}

	// PkgLoss: first sample, no prior = raw value (0.0)
	if rec.Attributes.PkgLoss != 0.0 {
		t.Fatalf("expected pkg_loss=0.0, got %.4f", rec.Attributes.PkgLoss)
	}

	// Latency: single connectTime write, first sample = raw value (no EMA blending)
	if rec.Attributes.Latency != connectTime {
		t.Fatalf("expected latency=%d (connectTime, single write), got %d", connectTime, rec.Attributes.Latency)
	}
}

func TestRowMetaDirtyTracking(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// Fresh row: not dirty, no state.
	if snap := rt.SnapshotAndClearDirtyRows(); len(snap) != 0 {
		t.Fatalf("expected no dirty rows on fresh table, got %d", len(snap))
	}

	// SetBestProxy marks the row dirty and snapshots the per-domain best proxy.
	rt.SetBestProxy(key, testDomain, "proxy-a")
	rt.SetTCPProbed(key, testDomain)
	snap := rt.SnapshotAndClearDirtyRows()
	if len(snap) != 1 {
		t.Fatalf("expected 1 dirty row, got %d", len(snap))
	}
	pr, ok := snap[key]
	if !ok {
		t.Fatalf("dirty snapshot missing key %q", key)
	}
	pd, ok := pr.Domains[testDomain]
	if !ok {
		t.Fatalf("dirty snapshot missing domain %q", testDomain)
	}
	if pd.BestProxy != "proxy-a" {
		t.Fatalf("BestProxy = %q, want %q", pd.BestProxy, "proxy-a")
	}
	if !pd.TCPProbed {
		t.Fatal("TCPProbed should be true")
	}

	// After snapshot-and-clear the row is clean again.
	if snap = rt.SnapshotAndClearDirtyRows(); len(snap) != 0 {
		t.Fatalf("expected no dirty rows after clear, got %d", len(snap))
	}

	// MarkFailed clears the routing state and re-marks the row dirty.
	rt.MarkFailed(key, "proxy-a", testDomain, 1.0)
	snap = rt.SnapshotAndClearDirtyRows()
	if len(snap) != 1 {
		t.Fatalf("expected 1 dirty row after MarkFailed, got %d", len(snap))
	}
	pr = snap[key]
	pd, ok = pr.Domains[testDomain]
	if !ok {
		t.Fatalf("dirty snapshot missing domain %q after MarkFailed", testDomain)
	}
	if pd.BestProxy != "" {
		t.Fatalf("BestProxy should be cleared by MarkFailed, got %q", pd.BestProxy)
	}
	if pd.TCPProbed {
		t.Fatal("TCPProbed should be cleared by MarkFailed")
	}
}

func TestRestoreRowMetaRoundtrip(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// A fresh restore must be clean and immediately serve the fast path.
	rt.RestoreRowMeta(key, PersistedRow{Domains: map[string]PersistedDomain{
		testDomain: {BestProxy: "proxy-a", TCPProbed: true},
	}})
	if snap := rt.SnapshotAndClearDirtyRows(); len(snap) != 0 {
		t.Fatalf("restored row should be clean, got %d dirty", len(snap))
	}
	if best, ok := rt.GetBestProxy(key, testDomain); !ok || best != "proxy-a" {
		t.Fatalf("GetBestProxy = %q, %v; want proxy-a, true", best, ok)
	}
	if !rt.IsTCPProbed(key, testDomain) {
		t.Fatal("IsTCPProbed should be true after restore")
	}

	// Restoring again with different state updates in place and stays clean.
	rt.RestoreRowMeta(key, PersistedRow{Domains: map[string]PersistedDomain{
		testDomain: {BestProxy: "proxy-b", TCPProbed: false},
	}})
	if best, ok := rt.GetBestProxy(key, testDomain); !ok || best != "proxy-b" {
		t.Fatalf("GetBestProxy after second restore = %q, %v; want proxy-b, true", best, ok)
	}
	if rt.IsTCPProbed(key, testDomain) {
		t.Fatal("IsTCPProbed should be false after second restore")
	}
	if snap := rt.SnapshotAndClearDirtyRows(); len(snap) != 0 {
		t.Fatalf("second restore should also be clean, got %d dirty", len(snap))
	}

	// MarkRowDirty marks a clean restored row for re-persist.
	rt.MarkRowDirty(key)
	if snap := rt.SnapshotAndClearDirtyRows(); len(snap) != 1 {
		t.Fatalf("expected 1 dirty row after MarkRowDirty, got %d", len(snap))
	}
}

func TestRowMetaRemoveProxyClearsBest(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	rt.SetBestProxy(key, testDomain, "proxy-a")
	rt.SetTCPProbed(key, testDomain)

	rt.RemoveProxy("proxy-a")
	snap := rt.SnapshotAndClearDirtyRows()
	if len(snap) != 1 {
		t.Fatalf("expected 1 dirty row after RemoveProxy, got %d", len(snap))
	}
	pr := snap[key]
	pd := pr.Domains[testDomain]
	if pd.BestProxy != "" {
		t.Fatalf("BestProxy should be cleared by RemoveProxy, got %q", pd.BestProxy)
	}
	if pd.TCPProbed {
		t.Fatal("TCPProbed should be cleared by RemoveProxy")
	}

	// Removing a proxy that was never best must not mark the row dirty.
	rt.SetBestProxy(key, testDomain, "proxy-b")
	rt.SnapshotAndClearDirtyRows()
	rt.RemoveProxy("proxy-c")
	if snap := rt.SnapshotAndClearDirtyRows(); len(snap) != 0 {
		t.Fatalf("RemoveProxy of non-best proxy marked row dirty")
	}
}

func TestPerDomainBestIndependent(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:13335"

	// Two domains under the same ASN row keep independent best proxies.
	rt.SetBestProxy(key, "site-a.example.com", "proxy-a")
	rt.SetBestProxy(key, "site-b.example.com", "proxy-b")

	if best, _ := rt.GetBestProxy(key, "site-a.example.com"); best != "proxy-a" {
		t.Fatalf("site-a best = %q, want proxy-a", best)
	}
	if best, _ := rt.GetBestProxy(key, "site-b.example.com"); best != "proxy-b" {
		t.Fatalf("site-b best = %q, want proxy-b", best)
	}

	// Marking proxy-a failed on site-a must only clear site-a, leaving site-b
	// (which still points at proxy-a) intact.
	rt.MarkFailed(key, "proxy-a", "site-a.example.com", 1.0)
	if _, ok := rt.GetBestProxy(key, "site-a.example.com"); ok {
		t.Fatal("site-a best should be cleared after proxy-a fails")
	}
	if best, _ := rt.GetBestProxy(key, "site-b.example.com"); best != "proxy-b" {
		t.Fatalf("site-b best = %q after proxy-a failure, want proxy-b", best)
	}
}

func TestMarkFailedClearsOnlyFailingDomain(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:13335"

	// Two domains point at proxy-a, one at proxy-b.
	rt.SetBestProxy(key, "site-a.example.com", "proxy-a")
	rt.SetBestProxy(key, "site-b.example.com", "proxy-a")
	rt.SetBestProxy(key, "site-c.example.com", "proxy-b")

	// A failure on site-a must clear only site-a.  site-b still points at
	// proxy-a (it did not observe the failure), and site-c is untouched.
	rt.MarkFailed(key, "proxy-a", "site-a.example.com", 1.0)

	if _, ok := rt.GetBestProxy(key, "site-a.example.com"); ok {
		t.Fatal("site-a should be cleared")
	}
	if best, _ := rt.GetBestProxy(key, "site-b.example.com"); best != "proxy-a" {
		t.Fatalf("site-b best = %q, want proxy-a (only site-a failed)", best)
	}
	if best, _ := rt.GetBestProxy(key, "site-c.example.com"); best != "proxy-b" {
		t.Fatalf("site-c best = %q, want proxy-b", best)
	}

	// The failure penalty is still proxy-wide: failedCount on proxy-a advances.
	rt.mu.RLock()
	fc := rt.rows[key].proxies["proxy-a"].FailedCount
	rt.mu.RUnlock()
	if fc != 1.0 {
		t.Fatalf("proxy-a failedCount = %v, want 1.0", fc)
	}
}

func TestDomainTableLRUEviction(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// Fill the domain table to capacity (normal ASN row).
	for i := 0; i < MaxDomainsPerNormalASRow; i++ {
		rt.SetBestProxy(key, fmt.Sprintf("domain-%d.example.com", i), "proxy-a")
	}

	rt.mu.RLock()
	count := len(rt.rows[key].domainTable)
	rt.mu.RUnlock()
	if count != MaxDomainsPerNormalASRow {
		t.Fatalf("expected %d domains, got %d", MaxDomainsPerNormalASRow, count)
	}

	// The first domain (least recently used) should be evicted on overflow.
	rt.SetBestProxy(key, "overflow.example.com", "proxy-b")

	rt.mu.RLock()
	_, firstGone := rt.rows[key].domainTable["domain-0.example.com"]
	_, overflowPresent := rt.rows[key].domainTable["overflow.example.com"]
	count = len(rt.rows[key].domainTable)
	rt.mu.RUnlock()

	if firstGone {
		t.Fatal("domain-0 should have been evicted (least recently used)")
	}
	if !overflowPresent {
		t.Fatal("overflow domain should be present")
	}
	if count != MaxDomainsPerNormalASRow {
		t.Fatalf("expected %d domains after overflow, got %d", MaxDomainsPerNormalASRow, count)
	}
}

func TestDomainTableCDNASLargerCapacity(t *testing.T) {
	rt := NewRouteTable(100)

	// A CDN ASN row (Cloudflare) gets MaxDomainsPerCDNASRow, not the smaller
	// normal-ASN limit.
	cdnKey := "ASN:13335 Cloudflare"
	for i := 0; i < MaxDomainsPerCDNASRow; i++ {
		rt.SetBestProxy(cdnKey, fmt.Sprintf("site-%d.example.com", i), "proxy-a")
	}
	rt.mu.RLock()
	cdnCount := len(rt.rows[cdnKey].domainTable)
	rt.mu.RUnlock()
	if cdnCount != MaxDomainsPerCDNASRow {
		t.Fatalf("CDN ASN row expected %d domains, got %d", MaxDomainsPerCDNASRow, cdnCount)
	}

	// A normal ASN row still caps at MaxDomainsPerNormalASRow.
	normalKey := "ASN:64512"
	for i := 0; i < MaxDomainsPerCDNASRow; i++ {
		rt.SetBestProxy(normalKey, fmt.Sprintf("site-%d.example.com", i), "proxy-a")
	}
	rt.mu.RLock()
	normalCount := len(rt.rows[normalKey].domainTable)
	rt.mu.RUnlock()
	if normalCount != MaxDomainsPerNormalASRow {
		t.Fatalf("normal ASN row expected %d domains, got %d", MaxDomainsPerNormalASRow, normalCount)
	}
}

func TestMaxDomainsForRow(t *testing.T) {
	cases := []struct {
		key  string
		want int
	}{
		{"ASN:13335 Cloudflare", MaxDomainsPerCDNASRow},
		{"ASN:13335", MaxDomainsPerCDNASRow},
		{"ASN:64512 Foo Corp", MaxDomainsPerNormalASRow},
		{"ASN:64512", MaxDomainsPerNormalASRow},
		{"TARGET:example.com", MaxDomainsPerNormalASRow},
		{"", MaxDomainsPerNormalASRow},
	}
	for _, tc := range cases {
		if got := maxDomainsForRow(tc.key); got != tc.want {
			t.Fatalf("maxDomainsForRow(%q) = %d, want %d", tc.key, got, tc.want)
		}
	}
}

func TestRestoreRowMetaLegacyTargetMigration(t *testing.T) {
	rt := NewRouteTable(100)
	key := "TARGET:example.com"

	// Legacy persisted rows carried row-level BestProxy/TCPProbed with no
	// domain map.  A TARGET row can reconstruct its domain from the key.
	rt.RestoreRowMeta(key, PersistedRow{BestProxy: "proxy-a", TCPProbed: true})

	if best, ok := rt.GetBestProxy(key, "example.com"); !ok || best != "proxy-a" {
		t.Fatalf("legacy TARGET migration GetBestProxy = %q, %v; want proxy-a, true", best, ok)
	}
	if !rt.IsTCPProbed(key, "example.com") {
		t.Fatal("legacy TARGET migration should restore tcpProbed")
	}
	if snap := rt.SnapshotAndClearDirtyRows(); len(snap) != 0 {
		t.Fatalf("legacy restore should be clean, got %d dirty", len(snap))
	}
}

func TestRestoreRowMetaLegacyASNDropped(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:13335"

	// Legacy ASN rows cannot reconstruct a domain from the key, so their old
	// row-level best is deliberately dropped and re-learned per-domain.
	rt.RestoreRowMeta(key, PersistedRow{BestProxy: "proxy-a", TCPProbed: true})

	if _, ok := rt.GetBestProxy(key, "example.com"); ok {
		t.Fatal("legacy ASN best should be dropped (no domain to map to)")
	}
	if rt.IsTCPProbed(key, "example.com") {
		t.Fatal("legacy ASN tcpProbed should be dropped")
	}
}

func TestRestoreRowMetaConnSizeRoundtrip(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"

	// UpdateConnSize alone must mark the row dirty so the connSize EMA is
	// persisted.  (Regression guard: connSize used to be silently dropped
	// because UpdateConnSize never set rowDirty.)
	rt.UpdateConnSize(key, testDomain, 2048)

	snap := rt.SnapshotAndClearDirtyRows()
	if len(snap) != 1 {
		t.Fatalf("expected 1 dirty row after UpdateConnSize, got %d", len(snap))
	}
	pd, ok := snap[key].Domains[testDomain]
	if !ok {
		t.Fatalf("snapshot missing domain %q", testDomain)
	}
	if pd.ConnSize != 2048 {
		t.Fatalf("ConnSize = %v, want 2048", pd.ConnSize)
	}
	if !pd.HasConnSizeSample {
		t.Fatal("HasConnSizeSample should be true")
	}

	// Restore into a fresh table and verify connSize is brought back.
	rt2 := NewRouteTable(100)
	rt2.RestoreRowMeta(key, snap[key])

	rt2.mu.RLock()
	cell := rt2.rows[key].domainTable[testDomain]
	gotConnSize := cell.connSize
	gotHas := cell.hasConnSizeSample
	rt2.mu.RUnlock()

	if gotConnSize != 2048 {
		t.Fatalf("restored connSize = %v, want 2048", gotConnSize)
	}
	if !gotHas {
		t.Fatal("restored hasConnSizeSample should be true")
	}
}
