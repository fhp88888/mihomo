package smart

import (
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

	// Second sample: EMA = old*2/3 + new*1/3 = 100*2/3 + 40*1/3 = 80
	rt.UpdateLatency(key, proxy, 40)
	snap = rt.Snapshot("test")
	rec = snap.Rows[0].Proxies[proxy]
	expected := int64(float64(100)*2.0/3.0 + float64(40)/3.0)
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
	first := snap.Rows[0].Proxies[proxy].Attributes.PkgLoss; if first < 0.009 || first > 0.011 {
		t.Fatalf("first pkg_loss should be ~0.01, got %.6f", first)
	}

	rt.UpdatePkgLoss(key, proxy, 0.04)
	snap = rt.Snapshot("test")
	expected := 0.01*2.0/3.0 + 0.04/3.0
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
	expected := 10485760.0*2.0/3.0 + 20971520.0/3.0
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
	rt.TouchRow("ASN:2") // make ASN:2 most recent

	snap := rt.Snapshot("test")
	// Should be sorted by LastUsed descending
	if snap.Rows[0].Key != "ASN:2" {
		t.Fatalf("expected ASN:2 first (most recent), got %s", snap.Rows[0].Key)
	}
}

func TestSecondaryRankColdStart(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:8075"

	rt.UpdateLatency(key, "proxy-fast", 89)
	rt.UpdateLatency(key, "proxy-mid", 163)
	rt.UpdateLatency(key, "proxy-slow", 275)

	topK := []string{"proxy-fast", "proxy-mid", "proxy-slow"}

	// No ASN data → cold start → rank = 1.33*lat. Order preserved.
	ranked := rt.SecondaryRank(key, "example.com", topK, nil)
	if ranked[0] != "proxy-fast" || ranked[1] != "proxy-mid" || ranked[2] != "proxy-slow" {
		t.Fatalf("cold start: expected latency order, got %v", ranked)
	}
}

func TestSecondaryRankBandwidthBoost(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:8075"

	rt.UpdateLatency(key, "proxy-low-lat", 89)
	rt.UpdateSpeed(key, "proxy-low-lat", 51804) // 50KB/s

	rt.UpdateLatency(key, "proxy-high-bw", 163)
	rt.UpdateSpeed(key, "proxy-high-bw", 189613) // 190KB/s

	// Large connection: avg 10MB
	rt.UpdateTargetConnSize(key, "bigfile.example.com", 10*1024*1024)

	ranked := rt.SecondaryRank(key, "bigfile.example.com", []string{"proxy-low-lat", "proxy-high-bw"}, nil)

	// 10MB/50KB = ~202s transmit vs 10MB/190KB = ~55s → high-bw wins
	if ranked[0] != "proxy-high-bw" {
		t.Fatalf("large file: expected proxy-high-bw first, got %s", ranked[0])
	}
}

func TestSecondaryRankLossPenalty(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:8075"

	rt.UpdateLatency(key, "proxy-clean", 100)
	rt.UpdatePkgLoss(key, "proxy-clean", 0.0)
	rt.UpdateLatency(key, "proxy-lossy", 100)
	rt.UpdatePkgLoss(key, "proxy-lossy", 0.15)

	ranked := rt.SecondaryRank(key, "example.com", []string{"proxy-clean", "proxy-lossy"}, nil)

	// clean: 133/1=133, lossy: 133/0.85=156.5 → clean wins
	if ranked[0] != "proxy-clean" {
		t.Fatalf("loss penalty: expected proxy-clean first, got %s", ranked[0])
	}
}

func TestSecondaryRankSmallConnection(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:8075"

	rt.UpdateLatency(key, "proxy-low-lat", 89)
	rt.UpdateSpeed(key, "proxy-low-lat", 51804)
	rt.UpdateLatency(key, "proxy-high-bw", 163)
	rt.UpdateSpeed(key, "proxy-high-bw", 189613)

	// Small connection: avg 2KB
	rt.UpdateTargetConnSize(key, "api.example.com", 2048)

	ranked := rt.SecondaryRank(key, "api.example.com", []string{"proxy-low-lat", "proxy-high-bw"}, nil)

	// 2KB/speed < 1ms for both → hit floor 0.33*lat → low-lat wins
	if ranked[0] != "proxy-low-lat" {
		t.Fatalf("small conn: expected proxy-low-lat first, got %s", ranked[0])
	}
}

func TestSecondaryRankEmptyAndFallback(t *testing.T) {
	rt := NewRouteTable(100)
	if len(rt.SecondaryRank("ASN:1", "x.com", nil, nil)) != 0 {
		t.Fatal("expected empty for nil input")
	}
	if len(rt.SecondaryRank("ASN:1", "x.com", []string{}, nil)) != 0 {
		t.Fatal("expected empty for empty input")
	}

	// Health check fallback: proxy-b has no cell, uses healthCheck=50
	rt.UpdateLatency("ASN:1", "proxy-a", 100)
	hc := func(name string) uint16 {
		if name == "proxy-b" {
			return 50
		}
		return 200
	}
	ranked := rt.SecondaryRank("ASN:1", "x.com", []string{"proxy-a", "proxy-b"}, hc)
	// proxy-b (50+16.5=66.5) < proxy-a (100+33=133)
	if ranked[0] != "proxy-b" {
		t.Fatalf("health check fallback: expected proxy-b first, got %s", ranked[0])
	}
}


