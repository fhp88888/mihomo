package smart

import (
	"math"
	"testing"
)

func TestAggregateByProxy(t *testing.T) {
	rt := NewRouteTable(100)
	keyA := "ASN:100|example.com"
	keyB := "ASN:100"

	// Row A: p1 use=2 lat=100, p2 use=1 lat=400 (total use=3)
	rt.UpdateLatency(keyA, "p1", 100)
	rt.UpdateLatency(keyA, "p2", 400)
	for i := 0; i < 2; i++ {
		rt.IncrementUseCount(keyA, "p1")
	}
	rt.IncrementUseCount(keyA, "p2")

	// Row B: p1 use=1 lat=250 (total use=1)
	rt.UpdateLatency(keyB, "p1", 250)
	rt.IncrementUseCount(keyB, "p1")

	// Unsampled proxy: has use count but no sample, must be excluded.
	for i := 0; i < 5; i++ {
		rt.IncrementUseCount(keyA, "p3")
	}

	agg := rt.AggregateByProxy()

	if agg.Count != 2 {
		t.Fatalf("expected Count=2, got %d", agg.Count)
	}
	if agg.UpdatedAt <= 0 {
		t.Fatalf("expected UpdatedAt > 0, got %d", agg.UpdatedAt)
	}
	if len(agg.Proxies) != 2 {
		t.Fatalf("expected 2 proxies, got %d: %+v", len(agg.Proxies), agg.Proxies)
	}

	// Order: p1 (use=3) before p2 (use=1)
	if agg.Proxies[0].Name != "p1" || agg.Proxies[1].Name != "p2" {
		t.Fatalf("expected order [p1 p2], got [%s %s]", agg.Proxies[0].Name, agg.Proxies[1].Name)
	}

	p1 := agg.Proxies[0]
	if p1.UseCount != 3 {
		t.Fatalf("expected p1 UseCount=3, got %d", p1.UseCount)
	}
	if p1.Rows != 2 {
		t.Fatalf("expected p1 Rows=2, got %d", p1.Rows)
	}
	// p1: rowA weight 2/3, rowB weight 1.0 → (100*2/3 + 250*1) / (2/3+1) = 190
	if p1.Attributes.Latency != 190 {
		t.Fatalf("expected p1 latency=190, got %d", p1.Attributes.Latency)
	}

	p2 := agg.Proxies[1]
	if p2.UseCount != 1 {
		t.Fatalf("expected p2 UseCount=1, got %d", p2.UseCount)
	}
	if p2.Rows != 1 {
		t.Fatalf("expected p2 Rows=1, got %d", p2.Rows)
	}
	// p2: rowA weight 1/3 → 400*(1/3)/(1/3) = 400
	if p2.Attributes.Latency != 400 {
		t.Fatalf("expected p2 latency=400, got %d", p2.Attributes.Latency)
	}

	// Unsampled p3 must not appear.
	for _, pr := range agg.Proxies {
		if pr.Name == "p3" {
			t.Fatalf("expected p3 to be excluded (no sample), but it appears: %+v", pr)
		}
	}
}

func TestAggregateByProxyEmpty(t *testing.T) {
	rt := NewRouteTable(100)

	agg := rt.AggregateByProxy()
	if agg.Count != 0 || len(agg.Proxies) != 0 {
		t.Fatalf("expected empty aggregation, got %+v", agg)
	}
	if agg.UpdatedAt <= 0 {
		t.Fatalf("expected UpdatedAt > 0 even for empty table, got %d", agg.UpdatedAt)
	}
}

// TestAggregateDoesNotBlendScore verifies that the aggregation pushes back into
// proxyAttrs for discovery ordering, but does NOT blend into per-target scores.
func TestAggregateDoesNotBlendScore(t *testing.T) {
	rt := NewRouteTable(100)
	key := "ASN:100|example.com"

	// Two rows on the same proxy; heavy use on the low-latency one.  Use
	// latencies well above the 100ms score floor so the values discriminate.
	rt.UpdateLatency(key, "p1", 150)
	for i := 0; i < 3; i++ {
		rt.IncrementUseCount(key, "p1")
	}
	rt.UpdateLatency("ASN:200", "p1", 450)
	rt.IncrementUseCount("ASN:200", "p1")

	// Before any aggregation: proxy-wise absent -> raw unblended score.
	rt.RefreshScores(key, []string{"p1"})
	snap := rt.Snapshot("test")
	pre := rowByKey(t, snap, key).Proxies["p1"].Attributes.Score
	atom := calculateScore(150, 0, 0, 0, 0)
	if math.Abs(pre-atom) > 0.000001 {
		t.Fatalf("expected pre-aggregation score atom=%.6f, got %.6f", atom, pre)
	}

	// Aggregate: p1 proxy-wise latency = (150*3/4 + 450*1/4) / (3/4+1/4) = 225.
	agg := rt.AggregateByProxy()
	if len(agg.Proxies) != 1 || agg.Proxies[0].Name != "p1" {
		t.Fatalf("expected single p1 aggregation, got %+v", agg.Proxies)
	}

	// After: latency stays raw (150, NOT blended toward the proxy-wise 225),
	// so the score is unchanged from the pre-aggregation atom.
	rt.RefreshScores(key, []string{"p1"})
	snap = rt.Snapshot("test")
	post := rowByKey(t, snap, key).Proxies["p1"].Attributes.Score
	if math.Abs(post-atom) > 0.000001 {
		t.Fatalf("expected latency-unblended score %.6f, got %.6f", atom, post)
	}
}

// rowByKey returns the row snapshot for key, so tests don't depend on the
// snapshot's LastUsed ordering (rows created in the same second tie, making
// Rows[0] non-deterministic).
func rowByKey(t *testing.T, snap TableSnapshot, key string) RowSnapshot {
	t.Helper()
	for _, r := range snap.Rows {
		if r.Key == key {
			return r
		}
	}
	t.Fatalf("row %q not found in snapshot (%d rows)", key, len(snap.Rows))
	return RowSnapshot{}
}

func TestBuildNodeRanking(t *testing.T) {
	proxies := []ProxyAggregate{
		{Name: "p1", UseCount: 10},
		{Name: "p2", UseCount: 5},
		{Name: "p3", UseCount: 3},
		{Name: "p4", UseCount: 1},
		{Name: "p5", UseCount: 0}, // never used → omitted
	}

	rank := BuildNodeRanking(proxies)

	if rank.LastUpdated <= 0 {
		t.Fatalf("expected LastUpdated > 0, got %d", rank.LastUpdated)
	}
	if len(rank.Result) != 4 {
		t.Fatalf("expected 4 used proxies (p5 omitted), got %d", len(rank.Result))
	}

	// Weight is normalized to the heaviest use (p1=10 → 100).
	if rank.Result[0].Name != "p1" || rank.Result[0].Weight != 100 {
		t.Fatalf("expected p1 weight 100, got %+v", rank.Result[0])
	}
	if rank.Result[1].Name != "p2" || rank.Result[1].Weight != 50 {
		t.Fatalf("expected p2 weight 50, got %+v", rank.Result[1])
	}

	// 4 positive proxies: mostUsedBound=max(1,floor(4*.2))=1, occasionalBound=1+floor(4*.5)=3.
	wantRanks := []string{RankMostUsed, RankOccasional, RankOccasional, RankRarelyUsed}
	for i, want := range wantRanks {
		if rank.Result[i].Rank != want {
			t.Fatalf("rank[%d] = %q, want %q", i, rank.Result[i].Rank, want)
		}
	}
}
