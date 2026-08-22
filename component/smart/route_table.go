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

const DefaultMaxRows = 200

// MaxDomainsPerNormalASRow caps the per-row domain table (LRU) for a normal
// ASN or TARGET row: each tracks at most this many distinct effective domains
// by connection size.
const MaxDomainsPerNormalASRow = 40

// MaxDomainsPerCDNASRow caps the per-row domain table (LRU) for a CDN ASN row.
// A CDN ASN (see CdnASNs in common.go) fronts many distinct sites behind one
// ASN, so its row gets a larger domain table to avoid thrashing the LRU.
const MaxDomainsPerCDNASRow = 200

// MaxTTFBProxiesPerRank caps how many TTFB-group proxies RankByScore keeps.
// Even when many proxies have a TTFB sample, only the top-N by score survive;
// the rest of the candidate list is filled by the latency (non-TTFB) group.
const MaxTTFBProxiesPerRank = 6

// minConnSizeForSpeedKB gates the speed term in calculateScore: smaller
// connections transfer too little data for a reliable throughput reading.
const minConnSizeForSpeedKB = 4.0

// connSizeUnknown stands in for connSize when none is known (restore,
// aggregation, debug); it is large enough to always include the speed term.
const connSizeUnknown = 1e18

const maxFailedCount  = 10.0

// ProxyAttributes holds tracked connection quality metrics.
type ProxyAttributes struct {
	PkgLoss     float64 `json:"pkg_loss"`
	Latency     int64   `json:"latency"`
	TTFB        int64   `json:"ttfb"`
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
	key      string
	lastUsed int64
	proxies  map[string]*proxyCell
	// domainTable records per-domain routing state and connection sizes (kB)
	// observed under this CDN/ASN row, keyed by the connection's effective
	// target (see GetEffectiveTarget).  Bounded to maxDomainsForRow(key) entries
	// via LRU.  All domains in a row share the same set of proxyCell metrics;
	// only the best proxy selection (bestProxy/tcpProbed) is per-domain.
	domainTable map[string]*domainCell
	// domainOrder is the LRU order of domainTable keys: index 0 is the least
	// recently used and is evicted first when the table is full.
	domainOrder []string
	// rowDirty is true when any domain's routing state (bestProxy, tcpProbed)
	// has changed and the row snapshot has not been persisted yet.
	rowDirty bool
}

// domainCell is the per-domain entry in a row's domainTable.  bestProxy and
// tcpProbed hold this domain's routing decision, while connSize is an EMA of
// the connection size (kB) seen for the domain.
type domainCell struct {
	domainName string
	bestProxy  string
	tcpProbed  bool
	lastUsed   int64 // last time this domain's best was used; freshness window
	connSize   float64
	// hasConnSizeSample distinguishes "no sample yet" (connSize is the zero
	// placeholder) from a real sample so applyEMA starts from the sample value.
	hasConnSizeSample bool
}

type proxyCell struct {
	Name             string
	UseCount         int64
	FailedCount      float64
	Latency          int64   // EMA, 0 means no sample yet
	TTFB             int64   // EMA of time-to-first-byte (ms), 0 means no sample yet
	PkgLoss          float64 // EMA
	Speed            float64 // EMA
	Jitter           float64 // EMA of |sample - previous TTFB EMA|, 0 means no sample yet
	Score            float64 // non-EMA score derived from latency, speed, pkgLoss and failedCount
	HasLatencySample bool
	HasTTFBSample    bool
	HasPkgLossSample bool
	HasSpeedSample   bool
	HasJitterSample  bool
	Dirty            bool // true when cell has unsaved changes
}

func (c *proxyCell) hasSample() bool {
	return c.HasLatencySample || c.HasTTFBSample || c.HasPkgLossSample || c.HasSpeedSample || c.HasJitterSample
}

// hasData is true when the cell has any sample or a failure count, so a proxy
// that has only failed is still scored with the failure penalty rather than a
// neutral health-check score.
func (c *proxyCell) hasData() bool {
	return c.hasSample() || c.FailedCount > 0
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

// DomainSnapshot is a read-only copy of one domain's routing state for the REST API.
type DomainSnapshot struct {
	Name      string  `json:"name"`
	BestProxy string  `json:"best_proxy"`
	TCPProbed bool    `json:"tcp_probed"`
	LastUsed  int64   `json:"last_used"`
	ConnSize  float64 `json:"conn_size"`
}

// RowSnapshot is a read-only copy of a row for the REST API.
type RowSnapshot struct {
	Key       string                 `json:"key"`
	BestProxy string                 `json:"best_proxy"`
	TCPProbed bool                   `json:"tcp_probed"`
	LastUsed  int64                  `json:"last_used"`
	Proxies   map[string]ProxyRecord `json:"proxies"`
	Domains   []DomainSnapshot       `json:"domains"`
}

// TableSnapshot is a read-only copy of the full route table.
type TableSnapshot struct {
	Group    string        `json:"group"`
	RowCount int           `json:"row_count"`
	Rows     []RowSnapshot `json:"rows"`
}

// RouteTable is a concurrent-safe in-memory routing table.
// Each row is keyed by "ASN:<number> <org>" (e.g. "ASN:13335 Cloudflare")
// or "TARGET:<name>".
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

	// Evict the least-recently-used row, preferring one with no unpersisted
	// changes so dirty metrics aren't dropped before the periodic flush.
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
		key:         key,
		lastUsed:    time.Now().Unix(),
		proxies:     make(map[string]*proxyCell),
		domainTable: make(map[string]*domainCell),
	}
	rt.rows[key] = row
	rt.lruOrder = append(rt.lruOrder, key)
	log.Debugln("[smart] LRU create key=%s size=%d capacity=%d", key, len(rt.rows), rt.maxRows)
	return row
}

// moveToBack moves key to the end of list, returning the new slice.
func moveToBack(list []string, key string) []string {
	for i, k := range list {
		if k == key {
			return append(append(list[:i], list[i+1:]...), key)
		}
	}
	return list
}

// touchLRU moves key to the most-recently-used end of the LRU list.
// Must be called with mu held.
func (rt *RouteTable) touchLRU(key string) {
	rt.lruOrder = moveToBack(rt.lruOrder, key)
}

// maxDomainsForRow returns the per-row domain-table capacity.  CDN ASNs (see
// CdnASNs) get a larger table because one ASN fronts many distinct sites.
func maxDomainsForRow(key string) int {
	rest, ok := strings.CutPrefix(key, "ASN:")
	if !ok {
		return MaxDomainsPerNormalASRow
	}
	asn, _, _ := strings.Cut(rest, " ")
	if CdnASNs[asn] {
		return MaxDomainsPerCDNASRow
	}
	return MaxDomainsPerNormalASRow
}

// getOrCreateDomainCell returns the domain cell for a row, creating it if
// needed and handling the domain-table LRU eviction.  Must be called with mu
// held.  When the table is full the least-recently-used domain (domainOrder[0])
// is evicted first.
func (rt *RouteTable) getOrCreateDomainCell(row *rowEntry, domain string) *domainCell {
	if cell, ok := row.domainTable[domain]; ok {
		return cell
	}

	capacity := maxDomainsForRow(row.key)

	// Evict the least-recently-used domain when at capacity.
	if len(row.domainTable) >= capacity && len(row.domainOrder) > 0 {
		evictKey := row.domainOrder[0]
		row.domainOrder = row.domainOrder[1:]
		delete(row.domainTable, evictKey)
	}

	cell := &domainCell{domainName: domain, connSize: 100}
	row.domainTable[domain] = cell
	row.domainOrder = append(row.domainOrder, domain)
	log.Debugln("[smart] LRU create domain key=%s domain=%s size=%d capacity=%d", row.key, domain, len(row.domainTable), capacity)
	return cell
}

// touchDomainLRU moves domain to the most-recently-used end of the row's
// domainOrder.  Must be called with mu held.
func (rt *RouteTable) touchDomainLRU(row *rowEntry, domain string) {
	row.domainOrder = moveToBack(row.domainOrder, domain)
}

// GetBestProxy returns the current best proxy for a route key's domain.
func (rt *RouteTable) GetBestProxy(key, domain string) (string, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok {
		return "", false
	}
	cell, ok := row.domainTable[domain]
	if !ok || cell.bestProxy == "" {
		return "", false
	}
	return cell.bestProxy, true
}

// GetBestProxyIfFresh returns the current best proxy for a route key's domain
// when that domain's best is younger than maxAge.
func (rt *RouteTable) GetBestProxyIfFresh(key, domain string, maxAge time.Duration) (string, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok {
		return "", false
	}
	cell, ok := row.domainTable[domain]
	if !ok || cell.bestProxy == "" {
		return "", false
	}
	if time.Since(time.Unix(cell.lastUsed, 0)) >= maxAge {
		return "", false
	}
	return cell.bestProxy, true
}

// IsTCPProbed returns whether the route key's domain has completed TCP discovery.
func (rt *RouteTable) IsTCPProbed(key, domain string) bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok {
		return false
	}
	cell, ok := row.domainTable[domain]
	return ok && cell.tcpProbed
}

// setDomainState updates a domain's routing state.  Caller holds mu.
func (rt *RouteTable) setDomainState(key, domain, proxy string, setBest, tcpProbed bool) {
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateDomainCell(row, domain)
	if setBest {
		cell.bestProxy = proxy
	}
	cell.tcpProbed = tcpProbed
	cell.lastUsed = time.Now().Unix()
	row.lastUsed = time.Now().Unix()
	row.rowDirty = true
	rt.touchDomainLRU(row, domain)
	rt.touchLRU(key)
}

// SetBestProxy sets the best proxy for a route key's domain.
func (rt *RouteTable) SetBestProxy(key, domain, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.setDomainState(key, domain, proxy, true, false)
}

// SetTCPProbed marks a route key's domain as having completed TCP discovery.
func (rt *RouteTable) SetTCPProbed(key, domain string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.setDomainState(key, domain, "", false, true)
}

// SetBestProxyAndTCPProbed sets the domain's best proxy and TCP-probed flag
// atomically, so a MarkFailed interleaving cannot leave bestProxy empty with
// tcpProbed set.
func (rt *RouteTable) SetBestProxyAndTCPProbed(key, domain, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.setDomainState(key, domain, proxy, true, true)
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

// applyEMA applies an exponential moving average weighted 3:1 toward the old value.
func applyEMA(old, new float64, hasSample bool) float64 {
	if !hasSample {
		return new
	}
	return old*3.0/4.0 + new/4.0
}

func applyEMAInt64(old, new int64, hasSample bool) int64 {
	return int64(applyEMA(float64(old), float64(new), hasSample))
}

// calculateScore computes a proxy score whose response-time term is responseTime.
// Pass latency for the legacy latency-derived score, or TTFB to score by
// time-to-first-byte instead.  All other dimensions (speed, pkgLoss,
// failedCount, jitter) apply identically.
func calculateScore(responseTime int64, speed float64, pkgLoss float64, failedCount float64, jitter float64, connSizeKB float64) float64 {
	score := 0.0
	if responseTime > 0 {
		score = 100.0 / (math.Max(float64(responseTime), 50.0) +
			math.Max(jitter, 10.0))
	}
	if speed > 0 && connSizeKB >= minConnSizeForSpeedKB {
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
		if !ok || !cell.hasData() {
			continue
		}
		cell.Score = calculateScore(cell.Latency, cell.Speed, cell.PkgLoss, cell.FailedCount, cell.Jitter, connSizeUnknown)
	}
}

// UpdateLatency updates the EMA latency for a (key, proxy) pair.
func (rt *RouteTable) UpdateLatency(key, proxy string, latency int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	cell.Latency = applyEMAInt64(cell.Latency, latency, cell.HasLatencySample)
	cell.HasLatencySample = true

	cell.Dirty = true
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// UpdateTTFB updates the EMA time-to-first-byte for a (key, proxy) pair.
// TTFB is measured end-to-end from dial start: it includes connecting to the
// proxy server and the protocol handshake, then the time to the first byte
// read from the target.  Jitter is updated alongside using the same sample:
// the absolute deviation of this sample from the previous TTFB EMA, smoothed
// with EMA.
func (rt *RouteTable) UpdateTTFB(key, proxy string, ttfb int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateCell(row, proxy)
	oldTTFB := cell.TTFB
	cell.TTFB = applyEMAInt64(cell.TTFB, ttfb, cell.HasTTFBSample)
	cell.HasTTFBSample = true

	// Jitter = EMA of |sample - previous TTFB EMA|. On the first TTFB
	// sample there is no baseline yet (oldTTFB is 0), so skip the jitter update.
	if oldTTFB > 0 {
		deviation := float64(ttfb - oldTTFB)
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

// UpdateConnSize updates the EMA connection size (kB) for a domain, evicting
// the least-recently-used domain when the row's LRU is full.
func (rt *RouteTable) UpdateConnSize(key, domain string, sizeKB float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateDomainCell(row, domain)
	rt.touchDomainLRU(row, domain)

	cell.connSize = applyEMA(cell.connSize, sizeKB, cell.hasConnSizeSample)
	cell.hasConnSizeSample = true
	row.rowDirty = true
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

// PreRankLatency sorts proxies by latency.  When key is non-empty only that
// row's samples are used (falling back to healthCheckLatency), preventing the
// first target's winner from biasing later probes via cross-row aggregation.
// key == "" preserves the legacy cross-row mean.  Sort is stable.
func (rt *RouteTable) PreRankLatency(proxies []string, healthCheckLatency func(string) uint16, key string) []string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	meanLatency := make(map[string]float64, len(proxies))

	fallbackLatency := func(proxy string) float64 {
		if healthCheckLatency != nil {
			return float64(healthCheckLatency(proxy))
		}
		return 1e9
	}

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
			meanLatency[proxy] = fallbackLatency(proxy)
		}
		// With no per-key data every proxy ties at the health-check fallback
		// (~0 ms on localhost), so shuffle to give each a fair first-batch slot.
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
			} else {
				meanLatency[proxy] = fallbackLatency(proxy)
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

// RankByScore sorts proxies for a route key's domain.  Once the key has any
// TTFB sample it switches to TTFB-first ranking:
//   - proxies with a TTFB sample are ranked first, scored with TTFB replacing
//     latency as the response-time term; at most MaxTTFBProxiesPerRank of them
//     survive (the top-scored ones), so a crowded TTFB group cannot crowd out
//     the latency-ranked fallbacks;
//   - proxies without a TTFB sample whose latency already exceeds the key's
//     minimum TTFB (a known-faster first-byte makes them hopeless) are skipped;
//     the rest are ranked after the TTFB group by latency-derived score.
//
// With no TTFB sample at all (cold start) it falls back to latency-derived
// scores as before.
func (rt *RouteTable) RankByScore(proxies []string, healthCheckLatency func(string) uint16, key, domain string) []string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	row, rowOK := rt.rows[key]

	// Per-domain connSize gates the speed component.  A domain without a cell
	// yet has connSize 0, which also skips the speed term.
	var connSizeKB float64
	if rowOK {
		if dc, ok := row.domainTable[domain]; ok {
			connSizeKB = dc.connSize
		}
	}

	// minTTFB is the smallest TTFB EMA among proxies with a sample in this row.
	var minTTFB int64
	hasTTFB := false
	if rowOK {
		for _, cell := range row.proxies {
			if cell.HasTTFBSample {
				if !hasTTFB || cell.TTFB < minTTFB {
					minTTFB = cell.TTFB
					hasTTFB = true
				}
			}
		}
	}

	type candidate struct {
		name  string
		score float64
		ttfb  bool
	}
	cands := make([]candidate, 0, len(proxies))

	for _, proxy := range proxies {
		var cell *proxyCell
		if rowOK {
			if c, ok := row.proxies[proxy]; ok {
				cell = c
			}
		}

		// TTFB group: rank by a score whose response-time term is TTFB.
		if cell != nil && cell.HasTTFBSample {
			cands = append(cands, candidate{
				name:  proxy,
				score: calculateScore(cell.TTFB, cell.Speed, cell.PkgLoss, cell.FailedCount, cell.Jitter, connSizeKB),
				ttfb:  true,
			})
			continue
		}

		// No TTFB sample: resolve a latency — cell EMA when the cell has any
		// data (a failed-only cell keeps latency 0 so its failure penalty still
		// drives it to the back), otherwise the health-check fallback.
		var latency int64
		var speed, pkgLoss, failedCount, jitter float64
		if cell != nil && cell.hasData() {
			latency = cell.Latency
			speed = cell.Speed
			pkgLoss = cell.PkgLoss
			failedCount = cell.FailedCount
			jitter = cell.Jitter
		} else if healthCheckLatency != nil {
			if hc := healthCheckLatency(proxy); hc != 0 && hc != 0xffff {
				latency = int64(hc)
			}
		}

		// Once any proxy has a TTFB sample, a proxy whose latency already
		// exceeds the known minimum first-byte time cannot win — skip it.
		if hasTTFB && latency > minTTFB {
			continue
		}

		cands = append(cands, candidate{
			name:  proxy,
			score: calculateScore(latency, speed, pkgLoss, failedCount, jitter, connSizeKB),
			ttfb:  false,
		})
	}

	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].ttfb != cands[j].ttfb {
			return cands[i].ttfb // TTFB group first
		}
		return cands[i].score > cands[j].score
	})

	// Only the top MaxTTFBProxiesPerRank TTFB proxies survive.  The sort put
	// the TTFB group first (score-descending), so skipping past the first N
	// ttfb candidates keeps exactly the highest-scored TTFB proxies.
	result := make([]string, 0, len(cands))
	ttfbKept := 0
	for _, c := range cands {
		if c.ttfb {
			if ttfbKept >= MaxTTFBProxiesPerRank {
				continue
			}
			ttfbKept++
		}
		result = append(result, c.name)
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

// ProxyFailedCount returns the failedCount recorded for a proxy within a
// specific route key's row, or 0 when the row or proxy has no record.  It backs
// discovery ordering (exploreOrder) so a proxy that has only failed for this
// key — and thus has no sample and is absent from the UseCount-weighted
// proxyAttrs aggregation — is still deferred to the end of the probe batch.
func (rt *RouteTable) ProxyFailedCount(key, proxy string) float64 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok {
		return 0
	}
	cell, ok := row.proxies[proxy]
	if !ok {
		return 0
	}
	return cell.FailedCount
}

// MarkFailed penalizes a proxy and, when the given domain's best points at it,
// clears that domain's best/tcpProbed so it re-discovers.  Clearing is
// domain-scoped: the proxy-wide FailedCount already deprioritizes it elsewhere.
func (rt *RouteTable) MarkFailed(key, proxy, domain string, penalty float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	cell := rt.getOrCreateCell(row, proxy)
	cell.FailedCount = math.Min(cell.FailedCount+penalty, maxFailedCount)
	cell.Dirty = true

	if domain == "" {
		return
	}
	dc, ok := row.domainTable[domain]
	if !ok || dc.bestProxy != proxy {
		return
	}
	dc.bestProxy = ""
	dc.tcpProbed = false
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
					TTFB:             cell.TTFB,
					PkgLoss:          cell.PkgLoss,
					Speed:            cell.Speed,
					Jitter:           cell.Jitter,
					UseCount:         cell.UseCount,
					FailedCount:      cell.FailedCount,
					HasLatencySample: cell.HasLatencySample,
					HasTTFBSample:    cell.HasTTFBSample,
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
	TTFB             int64   `json:"ttfb"`
	PkgLoss          float64 `json:"pkg_loss"`
	Speed            float64 `json:"speed"`
	Jitter           float64 `json:"jitter"`
	UseCount         int64   `json:"use_count"`
	FailedCount      float64 `json:"failed_count"`
	HasSample        bool    `json:"has_sample"` // retained for backward compat with old persisted data
	HasLatencySample bool    `json:"has_latency_sample"`
	HasTTFBSample    bool    `json:"has_ttfb_sample"`
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
	cell.TTFB = pc.TTFB
	cell.PkgLoss = pc.PkgLoss
	cell.Speed = pc.Speed
	cell.Jitter = pc.Jitter
	cell.UseCount = pc.UseCount
	cell.FailedCount = pc.FailedCount
	cell.HasLatencySample = pc.HasLatencySample
	cell.HasTTFBSample = pc.HasTTFBSample
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
	cell.Score = calculateScore(cell.Latency, cell.Speed, cell.PkgLoss, cell.FailedCount, cell.Jitter, connSizeUnknown)
	cell.Dirty = false
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// PersistedDomain is the JSON-serializable form of a domainCell's routing
// state (bestProxy, tcpProbed) plus its connection-size EMA so the connSize
// gating in calculateScore survives a restart.
type PersistedDomain struct {
	BestProxy         string  `json:"best_proxy"`
	TCPProbed         bool    `json:"tcp_probed"`
	ConnSize          float64 `json:"conn_size"`
	HasConnSizeSample bool    `json:"has_conn_size_sample"`
}

// PersistedRow is the JSON-serializable form of a rowEntry's routing state:
// the per-domain best/tcpProbed a fresh route would otherwise have to
// re-learn by discovery.  BestProxy/TCPProbed are retained only for backward
// compatibility with rows persisted before per-domain routing was introduced.
type PersistedRow struct {
	Domains   map[string]PersistedDomain `json:"domains,omitempty"`
	BestProxy string                     `json:"best_proxy,omitempty"`
	TCPProbed bool                       `json:"tcp_probed,omitempty"`
}

// RestoreRowMeta restores a row's per-domain routing state (bestProxy,
// tcpProbed) from previously persisted data.  The restored row is marked clean
// so it won't be flushed until its state actually changes.
//
// lastUsed is deliberately reset to time.Now() (not the persisted last-used),
// because the row's route data is old: allowing the freshness window to start
// from now means the restored best proxy is eligible for the fast path until it
// either stays fresh or is displaced by MarkFailed.
func (rt *RouteTable) RestoreRowMeta(key string, pr PersistedRow) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	now := time.Now().Unix()

	if len(pr.Domains) > 0 {
		for domain, pd := range pr.Domains {
			cell := rt.getOrCreateDomainCell(row, domain)
			cell.bestProxy = pd.BestProxy
			cell.tcpProbed = pd.TCPProbed
			cell.connSize = pd.ConnSize
			cell.hasConnSizeSample = pd.HasConnSizeSample
			cell.lastUsed = now
		}
	} else if pr.BestProxy != "" {
		// Legacy row-level best: only a TARGET row can reconstruct its domain
		// from the key ("TARGET:<domain>").  ASN rows drop their old best and
		// re-learn per-domain on the next discovery.
		if domain, ok := strings.CutPrefix(key, "TARGET:"); ok {
			cell := rt.getOrCreateDomainCell(row, domain)
			cell.bestProxy = pr.BestProxy
			cell.tcpProbed = pr.TCPProbed
			cell.lastUsed = now
		}
	}

	row.rowDirty = false
	row.lastUsed = now
	rt.touchLRU(key)
}

// SnapshotAndClearDirtyRows atomically snapshots all rows whose routing state
// changed and clears their dirty flag in a single lock window.  Returns a deep
// copy of each row's per-domain persisted fields so the caller can serialize
// without holding the lock.  The map key is the route key.
func (rt *RouteTable) SnapshotAndClearDirtyRows() map[string]PersistedRow {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	snapshot := make(map[string]PersistedRow)
	for key, row := range rt.rows {
		if row.rowDirty {
			domains := make(map[string]PersistedDomain, len(row.domainTable))
			for domain, cell := range row.domainTable {
				domains[domain] = PersistedDomain{
					BestProxy:         cell.bestProxy,
					TCPProbed:         cell.tcpProbed,
					ConnSize:          cell.connSize,
					HasConnSizeSample: cell.hasConnSizeSample,
				}
			}
			snapshot[key] = PersistedRow{Domains: domains}
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
		for _, dc := range row.domainTable {
			if dc.bestProxy == name {
				dc.bestProxy = ""
				dc.tcpProbed = false
				row.rowDirty = true
			}
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
					TTFB:        cell.TTFB,
					Speed:       cell.Speed,
					Score:       cell.Score,
					FailedCount: cell.FailedCount,
					Jitter:      cell.Jitter,
				},
			}
		}

		// BestProxy/TCPProbed at the row level reflect the most-recently-used
		// domain that has a best, kept for display compatibility.  The
		// authoritative per-domain state lives in Domains.
		domains := make([]DomainSnapshot, 0, len(row.domainTable))
		var bestBestProxy string
		var bestTCPProbed bool
		var bestLastUsed int64 = -1
		for _, dc := range row.domainTable {
			domains = append(domains, DomainSnapshot{
				Name:      dc.domainName,
				BestProxy: dc.bestProxy,
				TCPProbed: dc.tcpProbed,
				LastUsed:  dc.lastUsed,
				ConnSize:  dc.connSize,
			})
			if dc.bestProxy != "" && dc.lastUsed > bestLastUsed {
				bestLastUsed = dc.lastUsed
				bestBestProxy = dc.bestProxy
				bestTCPProbed = dc.tcpProbed
			}
		}
		sort.Slice(domains, func(i, j int) bool {
			return domains[i].LastUsed > domains[j].LastUsed
		})

		rows = append(rows, RowSnapshot{
			Key:       row.key,
			BestProxy: bestBestProxy,
			TCPProbed: bestTCPProbed,
			LastUsed:  row.lastUsed,
			Proxies:   proxies,
			Domains:   domains,
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
		ttfb         int64
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
			ttfb:         cell.TTFB,
			use:          cell.UseCount,
			fail:         cell.FailedCount,
			loss:         cell.PkgLoss,
			jitter:       cell.Jitter,
			speed:        cell.Speed,
			latencyScore: calculateScore(cell.Latency, 0, 0, cell.FailedCount, cell.Jitter, connSizeUnknown),
			speedScore:   calculateScore(0, cell.Speed, 0, cell.FailedCount, cell.Jitter, connSizeUnknown),
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
	domainNames := make([]string, 0, len(row.domainTable))
	for d := range row.domainTable {
		domainNames = append(domainNames, d)
	}
	sort.Strings(domainNames)
	sb.WriteString(fmt.Sprintf("key=%s domains=[", key))
	for i, d := range domainNames {
		dc := row.domainTable[d]
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(fmt.Sprintf("%s(best=%s,tcpProbed=%v)", d, dc.bestProxy, dc.tcpProbed))
	}
	sb.WriteString("] proxies=[")
	for i, e := range entries {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(fmt.Sprintf("%s(lat=%d,ttfb=%d,use=%d,fail=%.1f,loss=%.3f,jit=%.1f,spd=%.0f,[latScore=%.4f,speedScore=%.4f,score=%.4f])",
			e.name, e.latency, e.ttfb, e.use, e.fail, e.loss, e.jitter, e.speed, e.latencyScore, e.speedScore, e.score))
	}
	sb.WriteString("]")
	return sb.String()
}
