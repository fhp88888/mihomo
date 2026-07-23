package smart

import "sync"

const defaultMaxASNEntries = 20

// connSizeEntry is one per-target record in the ASN sub-table.
type connSizeEntry struct {
	target      string
	avgConnSize float32 // EMA, bidirectional bytes per connection lifecycle
}

// ASNSubTable tracks per-target average connection size (upload+download)
// for the targets visited under a given route-table row (ASN or TARGET key).
// Max 20 entries; LRU eviction on overflow. Memory-only, no persistence.
type ASNSubTable struct {
	mu         sync.RWMutex
	entries    []connSizeEntry
	maxEntries int
}

// NewASNSubTable creates an ASNSubTable with the default max capacity.
func NewASNSubTable() *ASNSubTable {
	return &ASNSubTable{
		entries:    make([]connSizeEntry, 0, defaultMaxASNEntries),
		maxEntries: defaultMaxASNEntries,
	}
}

// Update records or updates the EMA average connection size for a target.
// EMA weight: old * 2/3 + new * 1/3.
// If target is new and the table is full, the oldest entry (index 0) is evicted.
func (t *ASNSubTable) Update(target string, totalBytes float64) {
	if target == "" || totalBytes <= 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for i := range t.entries {
		if t.entries[i].target == target {
			t.entries[i].avgConnSize = t.entries[i].avgConnSize*2.0/3.0 + float32(totalBytes)/3.0
			// Move to end (most recently used)
			entry := t.entries[i]
			t.entries = append(t.entries[:i], t.entries[i+1:]...)
			t.entries = append(t.entries, entry)
			return
		}
	}

	// New entry: insert directly (first sample, no EMA needed)
	if len(t.entries) >= t.maxEntries {
		t.entries = t.entries[1:] // evict oldest (index 0)
	}
	t.entries = append(t.entries, connSizeEntry{
		target:      target,
		avgConnSize: float32(totalBytes),
	})
}

// GetAvgConnSize returns the EMA average connection size for the given target.
// Returns (0, false) if no record exists.
func (t *ASNSubTable) GetAvgConnSize(target string) (float32, bool) {
	if target == "" {
		return 0, false
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, e := range t.entries {
		if e.target == target {
			return e.avgConnSize, true
		}
	}
	return 0, false
}
