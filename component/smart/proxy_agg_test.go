package smart

import (
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
