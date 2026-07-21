package smart

import (
	"sync"
	"testing"
)

func TestASNSubTableUpdateAndGet(t *testing.T) {
	st := NewASNSubTable()

	st.Update("example.com", 10240)
	size, ok := st.GetAvgConnSize("example.com")
	if !ok {
		t.Fatal("expected entry to exist")
	}
	if size != 10240 {
		t.Fatalf("expected first size=10240, got %.0f", size)
	}

	// EMA: 10240*2/3 + 51200/3 = 23893.33
	st.Update("example.com", 51200)
	size, ok = st.GetAvgConnSize("example.com")
	if !ok {
		t.Fatal("expected entry after EMA update")
	}
	expected := 10240.0*2.0/3.0 + 51200.0/3.0
	if size < expected-0.01 || size > expected+0.01 {
		t.Fatalf("expected EMA size=%.2f, got %.2f", expected, size)
	}
}

func TestASNSubTableMissingAndEmpty(t *testing.T) {
	st := NewASNSubTable()
	if _, ok := st.GetAvgConnSize("nonexistent.com"); ok {
		t.Fatal("expected false for missing target")
	}
	st.Update("", 10240)
	if _, ok := st.GetAvgConnSize(""); ok {
		t.Fatal("expected false for empty target")
	}
	st.Update("x.com", 0)
	if _, ok := st.GetAvgConnSize("x.com"); ok {
		t.Fatal("expected false for zero-size update")
	}
}

func TestASNSubTableLRUEviction(t *testing.T) {
	st := NewASNSubTable()
	st.maxEntries = 3

	st.Update("a.com", 100)
	st.Update("b.com", 200)
	st.Update("c.com", 300)
	st.Update("a.com", 150) // a becomes MRU, order: b,c,a

	st.Update("d.com", 400) // evicts b (oldest)

	if _, ok := st.GetAvgConnSize("b.com"); ok {
		t.Fatal("b.com should have been evicted")
	}
	for _, name := range []string{"a.com", "c.com", "d.com"} {
		if _, ok := st.GetAvgConnSize(name); !ok {
			t.Fatalf("%s should still exist", name)
		}
	}
}

func TestASNSubTableConcurrent(t *testing.T) {
	st := NewASNSubTable()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			target := "host" + string(rune('a'+id%5)) + ".com"
			st.Update(target, float64(id*100))
			st.GetAvgConnSize(target)
		}(i)
	}
	wg.Wait()

	// Verify all 5 targets have entries with non-zero sizes
	for i := 0; i < 5; i++ {
		target := "host" + string(rune('a'+i)) + ".com"
		size, ok := st.GetAvgConnSize(target)
		if !ok {
			t.Errorf("expected %s to exist after concurrent updates", target)
		}
		if size <= 0 {
			t.Errorf("expected %s size > 0, got %.0f", target, size)
		}
	}

	// Verify non-existent target returns (0, false)
	size, ok := st.GetAvgConnSize("hostf.com")
	if ok {
		t.Error("expected hostf.com to not exist")
	}
	if size != 0 {
		t.Errorf("expected size 0 for missing target, got %.0f", size)
	}
}
