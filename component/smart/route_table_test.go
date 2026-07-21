package smart

import (
	"math"
	"sync"
	"testing"
	"time"
)

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
	if _, ok := rt.GetBestProxy(key); ok {
		t.Fatal("expected no best proxy for new key")
	}

	rt.SetBestProxy(key, "proxy-a")
	name, ok := rt.GetBestProxy(key)
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

	if rt.IsTCPProbed(key) {
		t.Fatal("expected false for new key")
	}

	rt.SetTCPProbed(key)
	if !rt.IsTCPProbed(key) {
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
		latency int64
		speed   float64
		expect  float64
	}{
		{latency: 100, speed: 0, expect: 0.01},
		{latency: 50, speed: 0, expect: 0.02},
		{latency: 100, speed: 10485760, expect: 0.01 + math.Log1p(10)},
		{latency: 0, speed: 1048576, expect: math.Log1p(1)},
		{latency: -10, speed: 0, expect: 0},
	}

	for _, tc := range cases {
		got := calculateScore(tc.latency, tc.speed)
		if math.Abs(got-tc.expect) > 0.000001 {
			t.Fatalf("latency=%d speed=%.0f: expected score %.6f, got %.6f", tc.latency, tc.speed, tc.expect, got)
		}
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
	expected := 1.0/float64(rec.Attributes.Latency) + math.Log1p(rec.Attributes.Speed/1024.0/1024.0)
	if math.Abs(got-expected) > 0.000001 {
		t.Fatalf("expected initial score %.6f, got %.6f", expected, got)
	}

	rt.UpdateLatency(key, proxy, 20)
	rt.UpdateSpeed(key, proxy, 20971520)
	rt.RefreshScores(key, []string{proxy})
	snap = rt.Snapshot("test")
	rec = snap.Rows[0].Proxies[proxy]
	expected = 1.0/float64(rec.Attributes.Latency) + math.Log1p(rec.Attributes.Speed/1024.0/1024.0)
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
	rt.RefreshScores(key, []string{"proxy-a", "proxy-b", "proxy-c"})

	proxies := []string{"proxy-a", "proxy-c", "proxy-b"}
	ranked := rt.RankByScore(proxies, nil, key)
	expected := []string{"proxy-c", "proxy-b", "proxy-a"}
	for i := range expected {
		if ranked[i] != expected[i] {
			t.Fatalf("ranked[%d]: expected %s, got %s", i, expected[i], ranked[i])
		}
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
	ranked := rt.RankByScore(proxies, nil, key)
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
	ranked := rt.RankByScore(proxies, healthCheck, key)
	expected := []string{"proxy-b", "proxy-a", "proxy-zero", "proxy-max"}
	for i := range expected {
		if ranked[i] != expected[i] {
			t.Fatalf("ranked[%d]: expected %s, got %s", i, expected[i], ranked[i])
		}
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
	expected := 1.0/float64(rec.Attributes.Latency) + math.Log1p(rec.Attributes.Speed/1024.0/1024.0)
	if math.Abs(got-expected) > 0.000001 {
		t.Fatalf("expected snapshot score %.6f, got %.6f", expected, got)
	}
}

func TestGetBestProxyIfFresh(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:64512"
	rt.SetBestProxy(key, "proxy-a")

	name, ok := rt.GetBestProxyIfFresh(key, 20*time.Second)
	if !ok || name != "proxy-a" {
		t.Fatalf("expected fresh proxy-a, got %s ok=%v", name, ok)
	}

	rt.mu.Lock()
	rt.rows[key].lastUsed = time.Now().Add(-21 * time.Second).Unix()
	rt.mu.Unlock()

	if name, ok := rt.GetBestProxyIfFresh(key, 20*time.Second); ok {
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

	rt.SetBestProxy("ASN:1", "p1")
	rt.SetBestProxy("ASN:2", "p2")
	rt.SetBestProxy("ASN:3", "p3")

	snap := rt.Snapshot("test")
	if snap.RowCount != 3 {
		t.Fatalf("expected 3 rows before eviction, got %d", snap.RowCount)
	}

	// Touch ASN:1 and ASN:2 to make ASN:3 LRU
	rt.TouchRow("ASN:1")
	rt.TouchRow("ASN:2")

	// Add a 4th row, should evict ASN:3 (least recently used)
	rt.SetBestProxy("ASN:4", "p4")

	snap = rt.Snapshot("test")
	if snap.RowCount != 3 {
		t.Fatalf("expected 3 rows after eviction, got %d", snap.RowCount)
	}

	// ASN:3 should be gone
	if _, ok := rt.GetBestProxy("ASN:3"); ok {
		t.Fatal("ASN:3 should have been evicted")
	}

	// ASN:1, ASN:2, ASN:4 should remain
	for _, k := range []string{"ASN:1", "ASN:2", "ASN:4"} {
		if _, ok := rt.GetBestProxy(k); !ok {
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
	rt.SetBestProxy("ASN:1", "proxy-a")

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
	if bp, _ := rt.GetBestProxy("ASN:1"); bp != "" {
		t.Fatalf("best proxy should be empty after removal, got %s", bp)
	}
}

func TestMarkFailed(t *testing.T) {
	rt := NewRouteTable(100)
	rt.UpdateLatency("ASN:1", "proxy-a", 42)
	rt.SetBestProxy("ASN:1", "proxy-a")

	rt.MarkFailed("ASN:1", "proxy-a")

	// Best proxy should be cleared
	if bp, _ := rt.GetBestProxy("ASN:1"); bp != "" {
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
			rt.SetBestProxy(key, "proxy-a")
			rt.GetBestProxy(key)
			rt.IsTCPProbed(key)
			rt.PreRankLatency([]string{"proxy-a", "proxy-b"}, nil, "")
			rt.RefreshScores(key, []string{"proxy-a", "proxy-b"})
			rt.RankByScore([]string{"proxy-a", "proxy-b"}, nil, key)
			rt.GetBestProxyIfFresh(key, time.Second)
			rt.Snapshot("test")
		}(i)
	}

	wg.Wait()
	// No race detector complaints = pass
}

func TestSnapshotRowOrder(t *testing.T) {
	rt := NewRouteTable(100)
	rt.SetBestProxy("ASN:1", "p1")
	rt.SetBestProxy("ASN:3", "p3")
	rt.SetBestProxy("ASN:2", "p2")
	time.Sleep(time.Second) // ensure distinct timestamp
	rt.TouchRow("ASN:2")    // make ASN:2 most recent

	snap := rt.Snapshot("test")
	// Should be sorted by LastUsed descending
	if snap.Rows[0].Key != "ASN:2" {
		t.Fatalf("expected ASN:2 first (most recent), got %s", snap.Rows[0].Key)
	}
}
