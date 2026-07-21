package smart

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

const DefaultMaxRows = 5000

// ProxyAttributes holds EMA-tracked connection quality metrics.
type ProxyAttributes struct {
	PkgLoss float64 `json:"pkg_loss"`
	Latency int64   `json:"latency"`
	Speed   float64 `json:"speed"`
}

// ProxyRecord is the per-proxy entry in a route table row.
type ProxyRecord struct {
	Name       string          `json:"name"`
	UseCount   int64           `json:"use_count"`
	Attributes ProxyAttributes `json:"attributes"`
}

// rowEntry is one row in the route table, keyed by ASN or TARGET.
type rowEntry struct {
	key       string
	bestProxy string
	tcpProbed bool
	lastUsed  int64
	proxies   map[string]*proxyCell
	asnSub    *ASNSubTable // per-row ASN-level sub-table for conn size tracking (memory-only)
}

type proxyCell struct {
	name      string
	useCount  int64
	latency   int64   // EMA, 0 means no sample yet
	pkgLoss   float64 // EMA
	speed     float64 // EMA
	hasSample bool
}

// RowSnapshot is a read-only copy of a row for the REST API.
type RowSnapshot struct {
	Key       string                  `json:"key"`
	BestProxy string                  `json:"best_proxy"`
	TCPProbed bool                    `json:"tcp_probed"`
	LastUsed  int64                   `json:"last_used"`
	Proxies   map[string]ProxyRecord  `json:"proxies"`
}

// TableSnapshot is a read-only copy of the full route table.
type TableSnapshot struct {
	Group    string        `json:"group"`
	RowCount int           `json:"row_count"`
	Rows     []RowSnapshot `json:"rows"`
}

// RouteTable is a concurrent-safe in-memory routing table.
// Each row is keyed by ASN:<number> or TARGET:<name>.
// Rows are LRU-evicted when the table exceeds maxRows.
type RouteTable struct {
	mu      sync.RWMutex
	rows    map[string]*rowEntry
	maxRows int
	// LRU order: index 0 = least recently used
	lruOrder []string
}

// NewRouteTable creates a new RouteTable with the given capacity.
func NewRouteTable(maxRows int) *RouteTable {
	if maxRows <= 0 {
		maxRows = DefaultMaxRows
	}
	return &RouteTable{
		rows:     make(map[string]*rowEntry),
		maxRows:  maxRows,
		lruOrder: make([]string, 0, maxRows),
	}
}

// getOrCreateRow returns the row for key, creating it if needed and handling LRU eviction.
// Must be called with mu held (write lock for creation, read lock for lookup-only).
func (rt *RouteTable) getOrCreateRow(key string) *rowEntry {
	row, ok := rt.rows[key]
	if ok {
		return row
	}

	// Evict if at capacity
	if len(rt.rows) >= rt.maxRows && len(rt.lruOrder) > 0 {
		evictKey := rt.lruOrder[0]
		rt.lruOrder = rt.lruOrder[1:]
		delete(rt.rows, evictKey)
	}

	row = &rowEntry{
		key:     key,
		lastUsed: time.Now().Unix(),
		proxies: make(map[string]*proxyCell),
	}
	rt.rows[key] = row
	rt.lruOrder = append(rt.lruOrder, key)
	return row
}

// touchLRU moves key to the end of the LRU list (most recently used).
// Must be called with mu held.
func (rt *RouteTable) touchLRU(key string) {
	for i, k := range rt.lruOrder {
		if k == key {
			rt.lruOrder = append(rt.lruOrder[:i], rt.lruOrder[i+1:]...)
			rt.lruOrder = append(rt.lruOrder, key)
			return
		}
	}
}

// GetBestProxy returns the current best proxy for a route key.
func (rt *RouteTable) GetBestProxy(key string) (string, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok || row.bestProxy == "" {
		return "", false
	}
	return row.bestProxy, true
}

// IsTCPProbed returns whether the row has completed TCP discovery.
func (rt *RouteTable) IsTCPProbed(key string) bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	return ok && row.tcpProbed
}

// SetBestProxy sets the best proxy for a route key.
func (rt *RouteTable) SetBestProxy(key, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	row.bestProxy = proxy
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// SetTCPProbed marks a row as having completed TCP discovery.
func (rt *RouteTable) SetTCPProbed(key string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	row.tcpProbed = true
	rt.touchLRU(key)
}

// getOrCreateCell returns the proxy cell for a row, creating it if needed.
// Must be called with mu held.
func (rt *RouteTable) getOrCreateCell(row *rowEntry, proxy string) *proxyCell {
	cell, ok := row.proxies[proxy]
	if !ok {
		cell = &proxyCell{name: proxy}
		row.proxies[proxy] = cell
	}
	return cell
}

// applyEMA applies exponential moving average with 1/3 weight for the new sample.
func applyEMA(old, new float64, hasSample bool) float64 {
	if !hasSample {
		return new
	}
	return old*2.0/3.0 + new/3.0
}

func applyEMAInt64(old, new int64, hasSample bool) int64 {
	if !hasSample {
		return new
	}
	return int64(float64(old)*2.0/3.0 + float64(new)/3.0)
}

// UpdateLatency updates the EMA latency for a (key, proxy) pair.
func (rt *RouteTable) UpdateLatency(key, proxy string, latency int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	cell.latency = applyEMAInt64(cell.latency, latency, cell.hasSample)
	cell.hasSample = true
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// UpdatePkgLoss updates the EMA packet loss for a (key, proxy) pair.
func (rt *RouteTable) UpdatePkgLoss(key, proxy string, loss float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	cell.pkgLoss = applyEMA(float64(cell.pkgLoss), loss, cell.hasSample)
	cell.hasSample = true
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// UpdateSpeed updates the EMA speed for a (key, proxy) pair.
func (rt *RouteTable) UpdateSpeed(key, proxy string, speed float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	cell.speed = applyEMA(float64(cell.speed), speed, cell.hasSample)
	cell.hasSample = true
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// UpdateTargetConnSize records the total bidirectional bytes for a target
// in the row's ASN sub-table (in-memory only, no persistence).
func (rt *RouteTable) UpdateTargetConnSize(key, target string, totalBytes float64) {
	if target == "" || totalBytes <= 0 {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	if row.asnSub == nil {
		row.asnSub = NewASNSubTable()
	}
	row.asnSub.Update(target, totalBytes)
}

// IncrementUseCount increments the use counter for a proxy within a row.
func (rt *RouteTable) IncrementUseCount(key, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	cell.useCount++
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// PreRankLatency sorts proxies by latency, preferring data from the given key's
// row when available.  When key is non-empty only the matching row is consulted;
// proxies without a sample in that row fall back to healthCheckLatency.  This
// prevents a positive-feedback loop where the winner of the first target's
// probe biases all subsequent probes via cross-row aggregation.
//
// When key is empty the old cross-row mean behaviour is preserved (used by
// Unwrap when no metadata is available).
//
// Sort is stable — equal latencies preserve input order.
func (rt *RouteTable) PreRankLatency(proxies []string, healthCheckLatency func(string) uint16, key string) []string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	meanLatency := make(map[string]float64, len(proxies))

	if key != "" {
		// Per-key: only use the specific row's data
		row, ok := rt.rows[key]
		hasKeyData := false
		for _, proxy := range proxies {
			if ok {
				if cell, cOk := row.proxies[proxy]; cOk && cell.hasSample {
					meanLatency[proxy] = float64(cell.latency)
					hasKeyData = true
					continue
				}
			}
			// Fall back to health check
			if healthCheckLatency != nil {
				meanLatency[proxy] = float64(healthCheckLatency(proxy))
			} else {
				meanLatency[proxy] = 1e9
			}
		}
		// When no proxy has per-key data, all latencies come from the
		// health-check fallback.  On localhost this means every proxy
		// ties at ~0 ms, so stable-sort would always put the same five
		// proxies in the first probe batch.  Shuffle to give every proxy
		// a fair chance on each new target.
		if !hasKeyData {
			result := make([]string, len(proxies))
			copy(result, proxies)
			rand.Shuffle(len(result), func(i, j int) {
				result[i], result[j] = result[j], result[i]
			})
			return result
		}
	} else {
		// Cross-row mean (legacy — used when no key is available)
		type sumCount struct {
			sum   int64
			count int
		}
		stats := make(map[string]*sumCount, len(proxies))
		for _, proxy := range proxies {
			stats[proxy] = &sumCount{}
		}

		for _, row := range rt.rows {
			for _, proxy := range proxies {
				if cell, cOk := row.proxies[proxy]; cOk && cell.hasSample {
					s := stats[proxy]
					s.sum += cell.latency
					s.count++
				}
			}
		}

		for proxy, sc := range stats {
			if sc.count > 0 {
				meanLatency[proxy] = float64(sc.sum) / float64(sc.count)
			} else if healthCheckLatency != nil {
				meanLatency[proxy] = float64(healthCheckLatency(proxy))
			} else {
				meanLatency[proxy] = 1e9
			}
		}
	}

	// Stable sort
	result := make([]string, len(proxies))
	copy(result, proxies)
	sort.SliceStable(result, func(i, j int) bool {
		return meanLatency[result[i]] < meanLatency[result[j]]
	})

	return result
}

// SecondaryRank re-ranks the top-K pre-ranked proxies using the predicted
// completion time formula:
//
//	Rank = (establish_time_ms + transmit_time_ms) / (1.0 - P_loss)
//
// This accounts for both bandwidth (via avg_conn_size / avg_speed) and
// packet loss, while still honoring latency for small connections.
func (rt *RouteTable) SecondaryRank(
	key string,
	target string,
	proxies []string,
	healthCheckLatency func(string) uint16,
) []string {
	if len(proxies) == 0 {
		return proxies
	}

	const epsilon = 1.0

	rt.mu.RLock()
	defer rt.mu.RUnlock()

	row, ok := rt.rows[key]
	var asnSub *ASNSubTable
	if ok && row.asnSub != nil {
		asnSub = row.asnSub
	}

	type rankEntry struct {
		name string
		rank float64
	}

	entries := make([]rankEntry, 0, len(proxies))

	for _, proxyName := range proxies {
		var establishTimeMs float64
		var avgSpeed float64
		var pLoss float64
		var hasCell bool

		if ok {
			if cell, cOk := row.proxies[proxyName]; cOk && cell.hasSample {
				establishTimeMs = float64(cell.latency)
				avgSpeed = cell.speed
				pLoss = cell.pkgLoss
				hasCell = true
			}
		}

		if !hasCell {
			if healthCheckLatency != nil {
				establishTimeMs = float64(healthCheckLatency(proxyName))
			} else {
				establishTimeMs = 1e9
			}
		}

		// Compute transmit_time_ms
		var transmitTimeMs float64
		avgConnSize, hasSize := float64(0), false
		if asnSub != nil {
			avgConnSize, hasSize = asnSub.GetAvgConnSize(target)
		}

		if hasSize && avgConnSize > 0 && avgSpeed > epsilon {
			actualTransmitMs := (avgConnSize / avgSpeed) * 1000.0
			floorTransmitMs := 0.33 * establishTimeMs
			if actualTransmitMs > floorTransmitMs {
				transmitTimeMs = actualTransmitMs
			} else {
				transmitTimeMs = floorTransmitMs
			}
		} else {
			transmitTimeMs = 0.33 * establishTimeMs
		}

		// Rank = (establish + transmit) / (1 - pLoss)
		lossPenalty := pLoss
		if lossPenalty > 0.99 {
			lossPenalty = 0.99
		}
		if lossPenalty < 0 {
			lossPenalty = 0
		}
		rank := (establishTimeMs + transmitTimeMs) / (1.0 - lossPenalty)

		entries = append(entries, rankEntry{name: proxyName, rank: rank})
	}

	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].rank != entries[j].rank {
			return entries[i].rank < entries[j].rank
		}
		return entries[i].name < entries[j].name
	})

	result := make([]string, len(entries))
	for i, e := range entries {
		result[i] = e.name
	}
	return result
}

// TouchRow updates the last-used timestamp for LRU tracking.
func (rt *RouteTable) TouchRow(key string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// MarkFailed removes the given proxy from the row's consideration and clears best_proxy.
func (rt *RouteTable) MarkFailed(key, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	if row.bestProxy == proxy {
		row.bestProxy = ""
	}
	row.tcpProbed = false
}

// RemoveProxy removes a proxy from all rows (e.g., when it leaves the provider).
func (rt *RouteTable) RemoveProxy(name string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, row := range rt.rows {
		delete(row.proxies, name)
		if row.bestProxy == name {
			row.bestProxy = ""
		}
	}
}

// Snapshot returns a read-only deep copy of the full table sorted by LastUsed descending.
func (rt *RouteTable) Snapshot(groupName string) TableSnapshot {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	rows := make([]RowSnapshot, 0, len(rt.rows))
	for _, row := range rt.rows {
		proxies := make(map[string]ProxyRecord, len(row.proxies))
		for _, cell := range row.proxies {
			proxies[cell.name] = ProxyRecord{
				Name:     cell.name,
				UseCount: cell.useCount,
				Attributes: ProxyAttributes{
					PkgLoss: cell.pkgLoss,
					Latency: cell.latency,
					Speed:   cell.speed,
				},
			}
		}
		rows = append(rows, RowSnapshot{
			Key:       row.key,
			BestProxy: row.bestProxy,
			TCPProbed: row.tcpProbed,
			LastUsed:  row.lastUsed,
			Proxies:   proxies,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].LastUsed > rows[j].LastUsed
	})

	return TableSnapshot{
		Group:    groupName,
		RowCount: len(rows),
		Rows:     rows,
	}
}

// DebugDumpRow returns a debug string of a single row's proxies map.
// Format: "proxy1(lat=30,use=5,loss=0.01,spd=1024) proxy2(lat=80,use=1,loss=0,spd=0) ..."
func (rt *RouteTable) DebugDumpRow(key string) string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	row, ok := rt.rows[key]
	if !ok || len(row.proxies) == 0 {
		return fmt.Sprintf("key=%s proxies=<empty>", key)
	}

	// Collect and sort by latency for readable output
	type entry struct {
		name    string
		latency int64
		use     int64
		loss    float64
		speed   float64
	}
	entries := make([]entry, 0, len(row.proxies))
	for _, cell := range row.proxies {
		entries = append(entries, entry{
			name:    cell.name,
			latency: cell.latency,
			use:     cell.useCount,
			loss:    cell.pkgLoss,
			speed:   cell.speed,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].latency != entries[j].latency {
			return entries[i].latency < entries[j].latency
		}
		return entries[i].name < entries[j].name
	})

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("key=%s best=%s tcpProbed=%v proxies=[",
		key, row.bestProxy, row.tcpProbed))
	for i, e := range entries {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(fmt.Sprintf("%s(lat=%d,use=%d,loss=%.3f,spd=%.0f)",
			e.name, e.latency, e.use, e.loss, e.speed))
	}
	sb.WriteString("]")
	return sb.String()
}
