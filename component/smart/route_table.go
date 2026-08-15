package smart

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/log"
)

const DefaultMaxRows = 5000

// ProxyAttributes holds tracked connection quality metrics.
type ProxyAttributes struct {
	PkgLoss     float64 `json:"pkg_loss"`
	Latency     int64   `json:"latency"`
	Speed       float64 `json:"speed"`
	Score       float64 `json:"score"`
	FailedCount float64 `json:"failed_count"`
	Jitter      float64 `json:"jitter"`
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
	// rowDirty is true when the row's routing state (bestProxy, tcpProbed)
	// has changed and the row snapshot has not been persisted yet.
	rowDirty bool
}

type proxyCell struct {
	Name             string
	UseCount         int64
	FailedCount      float64
	Latency          int64   // EMA, 0 means no sample yet
	PkgLoss          float64 // EMA
	Speed            float64 // EMA
	Jitter           float64 // EMA of |sample - previous latency EMA|, 0 means no sample yet
	Score            float64 // non-EMA score derived from latency, speed, pkgLoss and failedCount
	HasLatencySample bool
	HasPkgLossSample bool
	HasSpeedSample   bool
	HasJitterSample  bool
	Dirty            bool // true when cell has unsaved changes
}

// hasSample returns true when any metric has been sampled at least once.
func (c *proxyCell) hasSample() bool {
	return c.HasLatencySample || c.HasPkgLossSample || c.HasSpeedSample || c.HasJitterSample
}

// hasDirtyCell returns true when any cell in the row has unsaved changes.
// Must be called with mu held.
func hasDirtyCell(row *rowEntry) bool {
	for _, cell := range row.proxies {
		if cell.Dirty {
			return true
		}
	}
	return false
}

// RowSnapshot is a read-only copy of a row for the REST API.
type RowSnapshot struct {
	Key       string                 `json:"key"`
	BestProxy string                 `json:"best_proxy"`
	TCPProbed bool                   `json:"tcp_probed"`
	LastUsed  int64                  `json:"last_used"`
	Proxies   map[string]ProxyRecord `json:"proxies"`
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
	// proxyAttrs holds the per-proxy aggregation computed by AggregateByProxy.
	// It backs discovery ordering (exploreOrder) and the REST aggregation
	// snapshot.  It is NOT part of calculateScore, which uses only the
	// per-target (cell) view.  A missing proxy means "no aggregation yet".
	proxyAttrs map[string]ProxyAttributes
}

// NewRouteTable creates a new RouteTable with the given capacity.
func NewRouteTable(maxRows int) *RouteTable {
	if maxRows <= 0 {
		maxRows = DefaultMaxRows
	}
	return &RouteTable{
		rows:       make(map[string]*rowEntry),
		maxRows:    maxRows,
		lruOrder:   make([]string, 0, maxRows),
		proxyAttrs: make(map[string]ProxyAttributes),
	}
}

// getOrCreateRow returns the row for key, creating it if needed and handling LRU eviction.
// Must be called with mu held (write lock for creation, read lock for lookup-only).
func (rt *RouteTable) getOrCreateRow(key string) *rowEntry {
	row, ok := rt.rows[key]
	if ok {
		return row
	}

	// Evict if at capacity.  Prefer the least-recently-used row with no
	// unpersisted changes so we don't silently discard dirty metrics before the
	// periodic flush.  If every row is dirty, fall back to evicting the LRU row
	// anyway — unbounded growth is worse than dropping a dirty row.
	if len(rt.rows) >= rt.maxRows && len(rt.lruOrder) > 0 {
		evictIdx := -1
		for i, k := range rt.lruOrder {
			if r := rt.rows[k]; r != nil && !r.rowDirty && !hasDirtyCell(r) {
				evictIdx = i
				break
			}
		}
		if evictIdx < 0 {
			evictIdx = 0
		}
		evictKey := rt.lruOrder[evictIdx]
		rt.lruOrder = append(rt.lruOrder[:evictIdx], rt.lruOrder[evictIdx+1:]...)
		delete(rt.rows, evictKey)
	}

	row = &rowEntry{
		key:      key,
		lastUsed: time.Now().Unix(),
		proxies:  make(map[string]*proxyCell),
	}
	rt.rows[key] = row
	rt.lruOrder = append(rt.lruOrder, key)
	log.Debugln("[smart] LRU create key=%s size=%d capacity=%d", key, len(rt.rows), rt.maxRows)
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

// GetBestProxyIfFresh returns the current best proxy when the route row is younger than maxAge.
func (rt *RouteTable) GetBestProxyIfFresh(key string, maxAge time.Duration) (string, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok || row.bestProxy == "" {
		return "", false
	}
	if time.Since(time.Unix(row.lastUsed, 0)) >= maxAge {
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
	row.rowDirty = true
	rt.touchLRU(key)
}

// SetTCPProbed marks a row as having completed TCP discovery.
func (rt *RouteTable) SetTCPProbed(key string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	row.tcpProbed = true
	row.rowDirty = true
	rt.touchLRU(key)
}

// getOrCreateCell returns the proxy cell for a row, creating it if needed.
// Must be called with mu held.
func (rt *RouteTable) getOrCreateCell(row *rowEntry, proxy string) *proxyCell {
	cell, ok := row.proxies[proxy]
	if !ok {
		cell = &proxyCell{Name: proxy}
		row.proxies[proxy] = cell
	}
	return cell
}

// applyEMA applies exponential moving average with 1/4 weight for the new sample.
func applyEMA(old, new float64, hasSample bool) float64 {
	if !hasSample {
		return new
	}
	return old*3.0/4.0 + new/4.0
}

func applyEMAInt64(old, new int64, hasSample bool) int64 {
	if !hasSample {
		return new
	}
	return int64(float64(old)*3.0/4.0 + float64(new)/4.0)
}

func calculateScoreAtom(latency int64, speed float64, pkgLoss float64, failedCount float64, jitter float64) float64 {
	score := 0.0
	if latency > 0 {
		score = 100.0 / (math.Max(float64(latency), 100.0) +
			math.Max(jitter, 10.0))
	}
	if speed > 0 {
		// 500kb/s is a sensitive threshold to define what is good download speed or not.
		// so divided by 0.5
		score += math.Log1p(speed / 1024.0 / 1024.0 / 0.5)
	}
	score = score * (1 - math.Pow(pkgLoss, 0.7))
	if failedCount > 0 {
		score *= math.Pow(0.8, failedCount)
	}
	return score
}

func (rt *RouteTable) calculateScore(proxy string, latency int64, speed, pkgLoss, failedCount, jitter float64) float64 {
	return calculateScoreAtom(latency, speed, pkgLoss, failedCount, jitter)
}

// RefreshScores updates non-EMA scores for existing proxy samples in a route row.
func (rt *RouteTable) RefreshScores(key string, proxies []string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	for _, proxy := range proxies {
		cell, ok := row.proxies[proxy]
		if !ok || !cell.hasSample() {
			continue
		}
		cell.Score = rt.calculateScore(proxy, cell.Latency, cell.Speed, cell.PkgLoss, cell.FailedCount, cell.Jitter)
	}
}

// UpdateLatency updates the EMA latency for a (key, proxy) pair.
// Jitter is updated alongside using the same sample: the absolute deviation
// of this sample from the previous latency EMA, smoothed with EMA.
func (rt *RouteTable) UpdateLatency(key, proxy string, latency int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	oldLatency := cell.Latency
	cell.Latency = applyEMAInt64(cell.Latency, latency, cell.HasLatencySample)
	cell.HasLatencySample = true

	// Jitter = EMA of |sample - previous latency EMA|. On the first latency
	// sample there is no baseline yet (oldLatency is 0), so skip the jitter update.
	if oldLatency > 0 {
		deviation := float64(latency - oldLatency)
		if deviation < 0 {
			deviation = -deviation
		}
		cell.Jitter = applyEMA(cell.Jitter, deviation, cell.HasJitterSample)
		cell.HasJitterSample = true
	}

	cell.Dirty = true
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// UpdatePkgLoss updates the EMA packet loss for a (key, proxy) pair.
func (rt *RouteTable) UpdatePkgLoss(key, proxy string, loss float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	cell.PkgLoss = applyEMA(float64(cell.PkgLoss), loss, cell.HasPkgLossSample)
	cell.HasPkgLossSample = true
	cell.Dirty = true
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// UpdateSpeed updates the EMA speed for a (key, proxy) pair.
func (rt *RouteTable) UpdateSpeed(key, proxy string, speed float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	cell.Speed = applyEMA(float64(cell.Speed), speed, cell.HasSpeedSample)
	cell.HasSpeedSample = true
	cell.Dirty = true
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// IncrementUseCount increments the use counter for a proxy within a row.
// Also resets failedCount since a successful use means the proxy is working.
func (rt *RouteTable) IncrementUseCount(key, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	cell.UseCount++
	cell.FailedCount = 0
	cell.Dirty = true
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
				if cell, cOk := row.proxies[proxy]; cOk && cell.HasLatencySample {
					meanLatency[proxy] = float64(cell.Latency)
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
				if cell, cOk := row.proxies[proxy]; cOk && cell.HasLatencySample {
					s := stats[proxy]
					s.sum += cell.Latency
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

// RankByScore sorts proxies by score descending, falling back to latency-derived scores.
func (rt *RouteTable) RankByScore(proxies []string, healthCheckLatency func(string) uint16, key string) []string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	scores := make(map[string]float64, len(proxies))
	row, rowOK := rt.rows[key]
	for _, proxy := range proxies {
		if rowOK {
			if cell, ok := row.proxies[proxy]; ok && cell.hasSample() {
				if cell.Score > 0 {
					scores[proxy] = cell.Score
				} else {
					scores[proxy] = rt.calculateScore(proxy, cell.Latency, cell.Speed, cell.PkgLoss, cell.FailedCount, cell.Jitter)
				}
				continue
			}
		}
		if healthCheckLatency != nil {
			if hc := healthCheckLatency(proxy); hc != 0 && hc != 0xffff {
				scores[proxy] = rt.calculateScore(proxy, int64(hc), 0, 0, 0, 0)
			} else {
				scores[proxy] = 0
			}
		}
	}

	result := make([]string, len(proxies))
	copy(result, proxies)
	sort.SliceStable(result, func(i, j int) bool {
		return scores[result[i]] > scores[result[j]]
	})

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

// MarkFailed increments the failedCount for the given (key, proxy) pair,
// clearing bestProxy and tcpProbed.  Consecutive failures degrade the score
// via calculateScore's 0.8^n penalty.
//
// TODO: When failedCount exceeds a threshold (e.g. consecutive failures ≥
// maxFailedTimes), consider also calling store.UpdateHostStatus(...) to
// persist a TTL-based block via the HostStatus failure tracking system
// (see stats.go).  That gives cross-restart memory and binary exclusion
// while the score penalty handles in-memory gradual degradation.
func (rt *RouteTable) MarkFailed(key, proxy string, penalty float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	cell := rt.getOrCreateCell(row, proxy)
	cell.FailedCount = math.Min(cell.FailedCount+penalty, 10.0)
	cell.Dirty = true
	if row.bestProxy == proxy {
		row.bestProxy = ""
	}
	row.tcpProbed = false
	row.rowDirty = true
}

// DecayFailedCounts reduces every cell's FailedCount by 0.1 (floor 0) across all
// rows. Cells that change are marked dirty so they are persisted on the next cycle.
func (rt *RouteTable) DecayFailedCounts() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	dirtyCount := 0
	for _, row := range rt.rows {
		for _, cell := range row.proxies {
			if cell.FailedCount > 0 {
				cell.FailedCount = math.Max(0, cell.FailedCount-0.1)
				cell.Dirty = true
				dirtyCount++
			}
		}
	}
	return dirtyCount
}

// SnapshotAndClearDirty atomically snapshots all dirty cells and clears them
// in a single lock window. Returns a deep copy of each cell's persisted fields
// so the caller can serialize without holding the lock and without risk of
// data races with concurrent writers.
//
// The map key is "{routeKey}\x00{proxyName}".
func (rt *RouteTable) SnapshotAndClearDirty() map[string]PersistedCell {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	snapshot := make(map[string]PersistedCell)
	for key, row := range rt.rows {
		for _, cell := range row.proxies {
			if cell.Dirty {
				cellKey := key + "\x00" + cell.Name
				snapshot[cellKey] = PersistedCell{
					Latency:          cell.Latency,
					PkgLoss:          cell.PkgLoss,
					Speed:            cell.Speed,
					Jitter:           cell.Jitter,
					UseCount:         cell.UseCount,
					FailedCount:      cell.FailedCount,
					HasLatencySample: cell.HasLatencySample,
					HasPkgLossSample: cell.HasPkgLossSample,
					HasSpeedSample:   cell.HasSpeedSample,
					HasJitterSample:  cell.HasJitterSample,
				}
				cell.Dirty = false
			}
		}
	}
	return snapshot
}

// MarkDirty sets the dirty flag on a cell so it will be included in the next
// periodic persist cycle.  Unlike RestoreRow, it does not touch cell data,
// LRU order, or the Score field.
func (rt *RouteTable) MarkDirty(key, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	cell, ok := row.proxies[proxy]
	if !ok {
		return
	}
	cell.Dirty = true
}

// PersistedCell is the JSON-serializable form of a proxyCell meant for DB storage.
type PersistedCell struct {
	Latency          int64   `json:"latency"`
	PkgLoss          float64 `json:"pkg_loss"`
	Speed            float64 `json:"speed"`
	Jitter           float64 `json:"jitter"`
	UseCount         int64   `json:"use_count"`
	FailedCount      float64 `json:"failed_count"`
	HasSample        bool    `json:"has_sample"` // retained for backward compat with old persisted data
	HasLatencySample bool    `json:"has_latency_sample"`
	HasPkgLossSample bool    `json:"has_pkg_loss_sample"`
	HasSpeedSample   bool    `json:"has_speed_sample"`
	HasJitterSample  bool    `json:"has_jitter_sample"`
}

// RestoreRow restores a per-(key,proxy) cell from previously persisted data.
// The restored cell has dirty=false so it won't be flushed until modified.
func (rt *RouteTable) RestoreRow(key, proxy string, pc PersistedCell) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	cell.Latency = pc.Latency
	cell.PkgLoss = pc.PkgLoss
	cell.Speed = pc.Speed
	cell.Jitter = pc.Jitter
	cell.UseCount = pc.UseCount
	cell.FailedCount = pc.FailedCount
	cell.HasLatencySample = pc.HasLatencySample
	cell.HasPkgLossSample = pc.HasPkgLossSample
	cell.HasSpeedSample = pc.HasSpeedSample
	cell.HasJitterSample = pc.HasJitterSample
	// Backward compat: old persisted data only had HasSample.
	// Old code only wrote Speed/PkgLoss when > 0, so a zero value
	// for those fields means "no sample".  Infer each per-metric
	// flag from whether the stored value is non-zero.
	if pc.HasSample && !cell.HasLatencySample && !cell.HasPkgLossSample && !cell.HasSpeedSample && !cell.HasJitterSample {
		cell.HasLatencySample = pc.Latency > 0
		cell.HasPkgLossSample = pc.PkgLoss > 0
		cell.HasSpeedSample = pc.Speed > 0
	}
	cell.Score = rt.calculateScore(proxy, cell.Latency, cell.Speed, cell.PkgLoss, cell.FailedCount, cell.Jitter)
	cell.Dirty = false
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// PersistedRow is the JSON-serializable form of a rowEntry's routing state
// (the fields a fresh route would otherwise have to re-learn by discovery).
type PersistedRow struct {
	BestProxy string `json:"best_proxy"`
	TCPProbed bool   `json:"tcp_probed"`
}

// RestoreRowMeta restores a row's routing state (bestProxy, tcpProbed) from
// previously persisted data.  The restored row is marked clean so it won't be
// flushed until its state actually changes.
//
// lastUsed is deliberately reset to time.Now() (not the persisted last-used),
// because the row's route data is old: allowing the freshness window to start
// from now means the restored best proxy is eligible for the fast path until it
// either stays fresh or is displaced by MarkFailed.
func (rt *RouteTable) RestoreRowMeta(key string, pr PersistedRow) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	row.bestProxy = pr.BestProxy
	row.tcpProbed = pr.TCPProbed
	row.rowDirty = false
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// SnapshotAndClearDirtyRows atomically snapshots all rows whose routing state
// changed and clears their dirty flag in a single lock window.  Returns a deep
// copy of each row's persisted fields so the caller can serialize without
// holding the lock.  The map key is the route key.
func (rt *RouteTable) SnapshotAndClearDirtyRows() map[string]PersistedRow {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	snapshot := make(map[string]PersistedRow)
	for key, row := range rt.rows {
		if row.rowDirty {
			snapshot[key] = PersistedRow{
				BestProxy: row.bestProxy,
				TCPProbed: row.tcpProbed,
			}
			row.rowDirty = false
		}
	}
	return snapshot
}

// MarkRowDirty sets the dirty flag on a row so its routing state will be
// persisted on the next cycle.  Unlike RestoreRowMeta, it does not touch the
// row's state, LRU order, or lastUsed.
func (rt *RouteTable) MarkRowDirty(key string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	row.rowDirty = true
}

// RemoveProxy removes a proxy from all rows (e.g., when it leaves the provider).
func (rt *RouteTable) RemoveProxy(name string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, row := range rt.rows {
		delete(row.proxies, name)
		if row.bestProxy == name {
			row.bestProxy = ""
			row.tcpProbed = false
			row.rowDirty = true
		}
	}
	delete(rt.proxyAttrs, name)
}

// SetProxyAttrs stores the per-proxy aggregation backing the proxy-wise score
// component.  The aggregation is computed externally (AggregateByProxy) and
// pushed back so scoring stays group-local.  Proxies that vanish from the
// table are dropped so stale entries don't linger.
func (rt *RouteTable) SetProxyAttrs(attrs map[string]ProxyAttributes) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.proxyAttrs = attrs
}

// ProxyAttrsSnapshot returns a copy of the current per-proxy aggregation.
func (rt *RouteTable) ProxyAttrsSnapshot() map[string]ProxyAttributes {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	snap := make(map[string]ProxyAttributes, len(rt.proxyAttrs))
	for k, v := range rt.proxyAttrs {
		snap[k] = v
	}
	return snap
}

// Len returns the current number of rows in the table.
func (rt *RouteTable) Len() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return len(rt.rows)
}

// Snapshot returns a read-only deep copy of the full table sorted by LastUsed descending.
func (rt *RouteTable) Snapshot(groupName string) TableSnapshot {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	rows := make([]RowSnapshot, 0, len(rt.rows))
	for _, row := range rt.rows {
		proxies := make(map[string]ProxyRecord, len(row.proxies))
		for _, cell := range row.proxies {
			proxies[cell.Name] = ProxyRecord{
				Name:     cell.Name,
				UseCount: cell.UseCount,
				Attributes: ProxyAttributes{
					PkgLoss:     cell.PkgLoss,
					Latency:     cell.Latency,
					Speed:       cell.Speed,
					Score:       cell.Score,
					FailedCount: cell.FailedCount,
					Jitter:      cell.Jitter,
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
// Format: "proxy1(lat=30,use=5,fail=0,loss=0.01,spd=1024,latScore=...,speedScore=...,score=...) ..."
func (rt *RouteTable) DebugDumpRow(key string) string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	row, ok := rt.rows[key]
	if !ok || len(row.proxies) == 0 {
		return fmt.Sprintf("key=%s proxies=<empty>", key)
	}

	// Collect and sort by latency for readable output
	type entry struct {
		name         string
		latency      int64
		use          int64
		fail         float64
		loss         float64
		jitter       float64
		speed        float64
		latencyScore float64
		speedScore   float64
		score        float64
	}
	entries := make([]entry, 0, len(row.proxies))
	for _, cell := range row.proxies {
		entries = append(entries, entry{
			name:         cell.Name,
			latency:      cell.Latency,
			use:          cell.UseCount,
			fail:         cell.FailedCount,
			loss:         cell.PkgLoss,
			jitter:       cell.Jitter,
			speed:        cell.Speed,
			latencyScore: rt.calculateScore(cell.Name, cell.Latency, 0, 0, cell.FailedCount, cell.Jitter),
			speedScore:   rt.calculateScore(cell.Name, 0, cell.Speed, 0, cell.FailedCount, cell.Jitter),
			score:        cell.Score,
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
		sb.WriteString(fmt.Sprintf("%s(lat=%d,use=%d,fail=%.1f,loss=%.3f,jit=%.1f,spd=%.0f,[latScore=%.4f,speedScore=%.4f,score=%.4f])",
			e.name, e.latency, e.use, e.fail, e.loss, e.jitter, e.speed, e.latencyScore, e.speedScore, e.score))
	}
	sb.WriteString("]")
	return sb.String()
}
