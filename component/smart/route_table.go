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
	"golang.org/x/net/publicsuffix"
)

const DefaultMaxRows = 200

const RouteTableMetaKey = "__table__"

// MaxDomainsPerNormalASRow caps the per-row domain table (LRU) for a normal
// ASN or TARGET row: each tracks at most this many distinct hostnames or IPs
// by connection size.
const MaxDomainsPerNormalASRow = 50

// MaxDomainsPerCDNASRow caps the per-row domain table (LRU) for a CDN ASN row.
// A CDN ASN (see CdnASNs in common.go) fronts many distinct sites behind one
// ASN, so its row gets a larger domain table to avoid thrashing the LRU.
const MaxDomainsPerCDNASRow = 300

// MaxTTFBProxiesPerRank caps how many TTFB-group proxies RankByScore keeps.
// Even when many proxies have a TTFB sample, only the top-N by score survive;
// the rest of the candidate list is filled by the latency (non-TTFB) group.
const MaxTTFBProxiesPerRank = 6

// MaxProxyCellsPerDomain bounds detailed, domain-specific observations.
// Ranking is score-based, but storage eviction is recency-based so an early
// high score cannot occupy a slot forever and starve new proxies of samples.
const MaxProxyCellsPerDomain = 12

// MaxExploreRoutes bounds exploration cadence counters whose routes may
// otherwise outlive the bounded row/domain tables indefinitely.
const MaxExploreRoutes = 1024

// minConnSizeForSpeedKB gates the speed term in calculateScore: smaller
// connections transfer too little data for a reliable throughput reading.
const minConnSizeForSpeedKB = 4.0

// connSizeUnknown stands in for connSize when none is known (restore,
// aggregation, debug); it is large enough to always include the speed term.
const connSizeUnknown = 1e18

const maxFailedCount = 10.0

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

// TTFBPrior summarizes end-to-end TTFB observations for one proxy. Mean and
// StdDev are in milliseconds. Samples counts independent route/domain cells,
// not raw requests, because each cell already stores an EMA.
type TTFBPrior struct {
	Mean    float64
	StdDev  float64
	Samples int
}

// ExplorationRisk is the observed request-level cost of trying a challenger.
// Positive regret means the served request was slower than the incumbent's
// historical TTFB; negative regret means exploration improved it.
type ExplorationRisk struct {
	Mean    float64
	StdDev  float64
	Samples int64
}

// ProxyRecord is the per-proxy entry in a route table row.
type ProxyRecord struct {
	Name       string          `json:"name"`
	UseCount   int64           `json:"use_count"`
	Attributes ProxyAttributes `json:"attributes"`
}

// rowEntry is one row in the route table, keyed by ASN or TARGET.  Proxy
// quality metrics (latency, TTFB, speed, pkg loss, jitter, failed count) live
// per-domain inside domainCell, not on the row: they are strongly
// target-dependent (TTFB especially), so sharing them across every domain
// behind an ASN would blend unrelated sites' behavior into one noisy signal.
type rowEntry struct {
	key         string
	lastUsed    int64
	domainTable map[string]*domainCell
	domainOrder []string
	rowDirty    bool
}

// domainCell is the per-domain entry in a row's domainTable.  bestProxy and
// tcpProbed hold this domain's routing decision, connSize is an EMA of the
// connection size (kB) seen for the domain, and proxies holds this domain's
// own view of every proxy's quality metrics — nothing here is shared with
// sibling domains in the same row.
type domainCell struct {
	domainName        string
	tcpBestProxy      string
	udpBestProxy      string
	tcpProbed         bool
	lastUsed          int64 // domain LRU/activity timestamp
	firstUsed         int64 // first observation in this process
	tcpEvaluatedAt    int64 // last time TCP best was evaluated, not merely reused
	udpEvaluatedAt    int64 // last time UDP best was evaluated, not merely reused
	connSize          float64
	hasConnSizeSample bool
	proxies           map[string]*proxyCell
	// Observation LRU; current TCP and UDP bests are protected on eviction.
	proxyOrder []string
}

// ShouldExplore advances this route's request counter and returns true once
// every N known-route requests. Sibling observations inform risk pruning, but
// must not consume this route's opportunities to gather its own TTFB samples.
func (rt *RouteTable) ShouldExplore(key, domain string, every uint64) bool {
	if every == 0 {
		return false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	route := domain
	if key != "" {
		route = key + "\x00" + domain
	}
	if _, exists := rt.exploreCounts[route]; !exists && len(rt.exploreCounts) >= MaxExploreRoutes {
		evict := rt.exploreOrder[0]
		rt.exploreOrder = rt.exploreOrder[1:]
		delete(rt.exploreCounts, evict)
	}
	if _, exists := rt.exploreCounts[route]; exists {
		rt.exploreOrder = moveToBack(rt.exploreOrder, route)
	} else {
		rt.exploreOrder = append(rt.exploreOrder, route)
	}
	rt.exploreCounts[route]++
	return rt.exploreCounts[route]%every == 0
}

type proxyCell struct {
	Name                  string
	UseCount              int64
	FailedCount           float64
	Latency               int64   // EMA, 0 means no sample yet
	TTFB                  int64   // EMA of time-to-first-byte (ms), 0 means no sample yet
	PkgLoss               float64 // EMA
	Speed                 float64 // EMA
	Jitter                float64 // EMA of |sample - previous TTFB EMA|, 0 means no sample yet
	Score                 float64 // non-EMA score derived from latency, speed, pkgLoss and failedCount
	HasLatencySample      bool
	HasTTFBSample         bool
	HasPkgLossSample      bool
	HasSpeedSample        bool
	HasJitterSample       bool
	ExplorationRegretMean float64
	ExplorationRegretM2   float64
	ExplorationSamples    int64
	Dirty                 bool // true when cell has unsaved changes
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

// hasDirtyDomain returns true when any proxy cell in the domain has unsaved
// changes.  Must be called with mu held.
func hasDirtyDomain(dc *domainCell) bool {
	for _, cell := range dc.proxies {
		if cell.Dirty {
			return true
		}
	}
	return false
}

// hasDirtyCell returns true when any domain's proxy cell in the row has
// unsaved changes.  Must be called with mu held.
func hasDirtyCell(row *rowEntry) bool {
	for _, dc := range row.domainTable {
		if hasDirtyDomain(dc) {
			return true
		}
	}
	return false
}

// DomainSnapshot is a read-only copy of one domain's routing state for the REST API.
type DomainSnapshot struct {
	Name         string                 `json:"name"`
	BestProxy    string                 `json:"best_proxy"` // TCP best; retained for API compatibility
	UDPBestProxy string                 `json:"udp_best_proxy"`
	TCPProbed    bool                   `json:"tcp_probed"`
	LastUsed     int64                  `json:"last_used"`
	ConnSize     float64                `json:"conn_size"`
	Proxies      map[string]ProxyRecord `json:"proxies"`
}

// RowSnapshot is a read-only copy of a row for the REST API.
type RowSnapshot struct {
	Key       string           `json:"key"`
	BestProxy string           `json:"best_proxy"`
	TCPProbed bool             `json:"tcp_probed"`
	LastUsed  int64            `json:"last_used"`
	Domains   []DomainSnapshot `json:"domains"`
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
	proxyAttrs    map[string]ProxyAttributes
	exploreCounts map[string]uint64
	exploreOrder  []string
	tableDirty    bool
}

// NewRouteTable creates a new RouteTable with the given capacity.
func NewRouteTable(maxRows int) *RouteTable {
	if maxRows <= 0 {
		maxRows = DefaultMaxRows
	}
	return &RouteTable{
		rows:          make(map[string]*rowEntry),
		maxRows:       maxRows,
		lruOrder:      make([]string, 0, maxRows),
		proxyAttrs:    make(map[string]ProxyAttributes),
		exploreCounts: make(map[string]uint64),
		exploreOrder:  make([]string, 0, MaxExploreRoutes),
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
		rt.tableDirty = true
	}

	row = &rowEntry{
		key:         key,
		lastUsed:    time.Now().Unix(),
		domainTable: make(map[string]*domainCell),
	}
	rt.rows[key] = row
	rt.lruOrder = append(rt.lruOrder, key)
	rt.tableDirty = true
	log.Debugln("[Smart] LRU create key=%s size=%d capacity=%d", key, len(rt.rows), rt.maxRows)
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
// held.  When the table is full, the least-recently-used domain with no
// unpersisted proxy-metric changes is evicted first, mirroring the row-level
// eviction preference in getOrCreateRow — otherwise domain churn under a busy
// CDN ASN row would silently drop unpersisted latency/TTFB/speed samples.
func (rt *RouteTable) getOrCreateDomainCell(row *rowEntry, domain string) *domainCell {
	if cell, ok := row.domainTable[domain]; ok {
		return cell
	}

	capacity := maxDomainsForRow(row.key)

	if len(row.domainTable) >= capacity && len(row.domainOrder) > 0 {
		evictIdx := -1
		for i, d := range row.domainOrder {
			if dc := row.domainTable[d]; dc != nil && !hasDirtyDomain(dc) {
				evictIdx = i
				break
			}
		}
		if evictIdx < 0 {
			evictIdx = 0
		}
		evictKey := row.domainOrder[evictIdx]
		row.domainOrder = append(row.domainOrder[:evictIdx], row.domainOrder[evictIdx+1:]...)
		delete(row.domainTable, evictKey)
		row.rowDirty = true
	}

	now := time.Now().Unix()
	cell := &domainCell{
		domainName: domain,
		connSize:   100,
		firstUsed:  now,
		lastUsed:   now,
		proxies:    make(map[string]*proxyCell),
		proxyOrder: make([]string, 0, MaxProxyCellsPerDomain),
	}
	row.domainTable[domain] = cell
	row.domainOrder = append(row.domainOrder, domain)
	row.rowDirty = true
	log.Debugln("[Smart] LRU create domain key=%s domain=%s size=%d capacity=%d", row.key, domain, len(row.domainTable), capacity)
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
	if !ok || cell.tcpBestProxy == "" {
		return "", false
	}
	return cell.tcpBestProxy, true
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
	if !ok || cell.tcpBestProxy == "" {
		return "", false
	}
	if time.Since(time.Unix(cell.tcpEvaluatedAt, 0)) >= maxAge {
		return "", false
	}
	return cell.tcpBestProxy, true
}

// GetUDPBestProxyIfFresh returns the current UDP best for a route key's domain
// when it was used within maxAge.
func (rt *RouteTable) GetUDPBestProxyIfFresh(key, domain string, maxAge time.Duration) (string, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok {
		return "", false
	}
	cell, ok := row.domainTable[domain]
	if !ok || cell.udpBestProxy == "" {
		return "", false
	}
	if time.Since(time.Unix(cell.udpEvaluatedAt, 0)) >= maxAge {
		return "", false
	}
	return cell.udpBestProxy, true
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
func (rt *RouteTable) setDomainState(key, domain, proxy string, setBest, tcpProbed, evaluated bool) {
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateDomainCell(row, domain)
	if setBest {
		cell.tcpBestProxy = proxy
	}
	cell.tcpProbed = tcpProbed
	cell.lastUsed = time.Now().Unix()
	if evaluated {
		cell.tcpEvaluatedAt = cell.lastUsed
	}
	row.lastUsed = time.Now().Unix()
	row.rowDirty = true
	rt.touchDomainLRU(row, domain)
	rt.touchLRU(key)
}

// SetBestProxy sets the best proxy for a route key's domain.
func (rt *RouteTable) SetBestProxy(key, domain, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.setDomainState(key, domain, proxy, true, false, true)
}

// SetUDPBestProxy updates UDP routing state without modifying TCP best/probed.
func (rt *RouteTable) SetUDPBestProxy(key, domain, proxy string, evaluated bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	cell := rt.getOrCreateDomainCell(row, domain)
	cell.udpBestProxy = proxy
	cell.lastUsed = time.Now().Unix()
	if evaluated {
		cell.udpEvaluatedAt = cell.lastUsed
	}
	row.lastUsed = cell.lastUsed
	row.rowDirty = true
	rt.touchDomainLRU(row, domain)
	rt.touchLRU(key)
}

// SetTCPProbed marks a route key's domain as having completed TCP discovery.
func (rt *RouteTable) SetTCPProbed(key, domain string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.setDomainState(key, domain, "", false, true, true)
}

// SetTCPProbedPreserveEvaluation records a successful exploration dial without
// replacing the best or refreshing its evaluation time. Another in-flight
// request may already have promoted a new best since this dial began.
func (rt *RouteTable) SetTCPProbedPreserveEvaluation(key, domain string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.setDomainState(key, domain, "", false, true, false)
}

// PromoteTCPBestIfCurrent prevents a late dial or exploration result from
// replacing a best that changed after the request captured its incumbent.
func (rt *RouteTable) PromoteTCPBestIfCurrent(key, domain, incumbent, challenger string) bool {
	return rt.promoteTCPBestIfCurrent(key, domain, incumbent, challenger, false)
}

// PromoteTCPBestAfterFallback also accepts an empty best: a failed dial may
// have cleared the incumbent before the fallback connected.
func (rt *RouteTable) PromoteTCPBestAfterFallback(key, domain, incumbent, fallback string) bool {
	return rt.promoteTCPBestIfCurrent(key, domain, incumbent, fallback, true)
}

func (rt *RouteTable) promoteTCPBestIfCurrent(key, domain, incumbent, challenger string, allowEmpty bool) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return false
	}
	cell, ok := row.domainTable[domain]
	if !ok || cell.tcpBestProxy != incumbent && (!allowEmpty || cell.tcpBestProxy != "") {
		return false
	}
	rt.setDomainState(key, domain, challenger, true, true, true)
	return true
}

// SetBestProxyAndTCPProbed sets the domain's best proxy and TCP-probed flag
// atomically, so a MarkFailed interleaving cannot leave bestProxy empty with
// tcpProbed set.
func (rt *RouteTable) SetBestProxyAndTCPProbed(key, domain, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.setDomainState(key, domain, proxy, true, true, true)
}

// SetBestProxyAndTCPProbedPreserveEvaluation records a successful reuse of the
// current TCP best without extending its evaluation freshness window.
func (rt *RouteTable) SetBestProxyAndTCPProbedPreserveEvaluation(key, domain, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.setDomainState(key, domain, proxy, true, true, false)
}

// getOrCreateCell returns the proxy cell within a domain, creating it if needed.
// Must be called with mu held.
func (rt *RouteTable) getOrCreateCell(dc *domainCell, proxy string) *proxyCell {
	cell, ok := dc.proxies[proxy]
	if ok {
		dc.proxyOrder = moveToBack(dc.proxyOrder, proxy)
		return cell
	}

	if len(dc.proxies) >= MaxProxyCellsPerDomain {
		evictIdx := -1
		// Prefer a clean LRU cell. Best proxies are routing state rather than
		// disposable observations and must remain resident.
		for i, name := range dc.proxyOrder {
			if name == dc.tcpBestProxy || name == dc.udpBestProxy {
				continue
			}
			if existing := dc.proxies[name]; existing != nil && !existing.Dirty {
				evictIdx = i
				break
			}
		}
		// Preserve the hard bound even when every cell changed since the last
		// persistence pass; the oldest non-best observation is least valuable.
		if evictIdx < 0 {
			for i, name := range dc.proxyOrder {
				if name != dc.tcpBestProxy && name != dc.udpBestProxy {
					evictIdx = i
					break
				}
			}
		}
		if evictIdx >= 0 {
			evict := dc.proxyOrder[evictIdx]
			dc.proxyOrder = append(dc.proxyOrder[:evictIdx], dc.proxyOrder[evictIdx+1:]...)
			delete(dc.proxies, evict)
		}
	}

	cell = &proxyCell{Name: proxy}
	dc.proxies[proxy] = cell
	dc.proxyOrder = append(dc.proxyOrder, proxy)
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

// RefreshScores updates non-EMA scores for existing proxy samples in a route
// key's domain.
func (rt *RouteTable) RefreshScores(key, domain string, proxies []string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	dc, ok := row.domainTable[domain]
	if !ok {
		return
	}
	for _, proxy := range proxies {
		cell, ok := dc.proxies[proxy]
		if !ok || !cell.hasData() {
			continue
		}
		cell.Score = calculateScore(cell.Latency, cell.Speed, cell.PkgLoss, cell.FailedCount, cell.Jitter, connSizeUnknown)
	}
}

// UpdateLatency updates the EMA latency for a (key, domain, proxy) triple.
func (rt *RouteTable) UpdateLatency(key, domain, proxy string, latency int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	dc := rt.getOrCreateDomainCell(row, domain)
	cell := rt.getOrCreateCell(dc, proxy)
	cell.Latency = applyEMAInt64(cell.Latency, latency, cell.HasLatencySample)
	cell.HasLatencySample = true

	cell.Dirty = true
	rt.touchDomainLRU(row, domain)
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// ProxyDialLatency returns the historical TCP dial-latency EMA for one
// (route key, domain, proxy) tuple. The boolean is false until at least one
// successful dial has contributed a latency sample.
func (rt *RouteTable) ProxyDialLatency(key, domain, proxy string) (int64, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok {
		return 0, false
	}
	dc, ok := row.domainTable[domain]
	if !ok {
		return 0, false
	}
	cell, ok := dc.proxies[proxy]
	if !ok || !cell.HasLatencySample {
		return 0, false
	}
	return cell.Latency, true
}

// ProxyHasTTFBSample reports whether a proxy has an end-to-end first-byte
// sample for this route key and domain.
func (rt *RouteTable) ProxyHasTTFBSample(key, domain, proxy string) bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok {
		return false
	}
	dc, ok := row.domainTable[domain]
	if !ok {
		return false
	}
	cell, ok := dc.proxies[proxy]
	return ok && cell.HasTTFBSample
}

// RouteFamily returns the registrable-domain root of the domain suffix tree.
// A leading fake-IP wildcard describes a deeper hostname and does not change
// that root. Public suffixes alone never form a family.
func RouteFamily(domain string) string {
	host := strings.TrimPrefix(strings.ToLower(strings.TrimSuffix(domain, ".")), "*.")
	if root, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
		return root
	}
	parts := strings.Split(host, ".")
	if len(parts) >= 2 {
		return strings.Join(parts[len(parts)-2:], ".")
	}
	return host
}

// DomainTreeSimilarity measures proximity in a reversed, public-suffix-aware
// domain tree. Siblings under one registrable domain are related; an ancestor
// and descendant sharing a longer private suffix are closer. A public suffix
// by itself contributes no similarity.
func DomainTreeSimilarity(a, b string) float64 {
	labels := func(domain string) (string, []string) {
		host := strings.TrimPrefix(strings.ToLower(strings.TrimSuffix(domain, ".")), "*.")
		root := RouteFamily(host)
		if root == "" || !strings.HasSuffix(host, root) {
			return root, nil
		}
		rootLabels := strings.Split(root, ".")
		public, _ := publicsuffix.PublicSuffix(root)
		publicLabels := 0
		if public != "" {
			publicLabels = len(strings.Split(public, "."))
		}
		privateCount := len(rootLabels) - publicLabels
		if privateCount < 1 {
			privateCount = 1
		}
		privateRoot := rootLabels[:privateCount]
		sub := strings.TrimSuffix(host, "."+root)
		parts := append([]string{}, privateRoot...)
		if sub != host && sub != "" {
			subLabels := strings.Split(sub, ".")
			for i := len(subLabels) - 1; i >= 0; i-- {
				parts = append(parts, subLabels[i])
			}
		}
		return root, parts
	}
	rootA, aParts := labels(a)
	rootB, bParts := labels(b)
	if rootA == "" || rootA != rootB {
		return 0
	}
	shared := 0
	for shared < len(aParts) && shared < len(bParts) && aParts[shared] == bParts[shared] {
		shared++
	}
	if shared == 0 {
		return 0
	}
	distance := len(aParts) + len(bParts) - 2*shared
	return 1 / float64(1+distance)
}

func performanceSignatureSimilarity(a, b *domainCell) float64 {
	x, y := make([]float64, 0), make([]float64, 0)
	for proxy, ac := range a.proxies {
		bc, ok := b.proxies[proxy]
		if !ok || !ac.HasTTFBSample || !bc.HasTTFBSample {
			continue
		}
		x, y = append(x, float64(ac.TTFB)), append(y, float64(bc.TTFB))
	}
	if len(x) < 3 {
		return 1
	}
	mx, my := 0.0, 0.0
	for i := range x {
		mx, my = mx+x[i], my+y[i]
	}
	mx, my = mx/float64(len(x)), my/float64(len(y))
	cov, vx, vy := 0.0, 0.0, 0.0
	for i := range x {
		dx, dy := x[i]-mx, y[i]-my
		cov, vx, vy = cov+dx*dy, vx+dx*dx, vy+dy*dy
	}
	if vx == 0 || vy == 0 {
		return 1
	}
	correlation := cov / math.Sqrt(vx*vy)
	return math.Max(0, math.Min(1, (correlation+1)/2))
}

func summarizeValues(values []float64, floorFraction float64) (TTFBPrior, bool) {
	if len(values) == 0 {
		return TTFBPrior{}, false
	}
	mean := 0.0
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))
	variance := 0.0
	for _, value := range values {
		delta := value - mean
		variance += delta * delta
	}
	variance /= float64(len(values))
	stddev := math.Sqrt(variance)
	if floor := mean * floorFraction; stddev < floor {
		stddev = floor
	}
	return TTFBPrior{Mean: mean, StdDev: stddev, Samples: len(values)}, true
}

// SimilarTTFBPrior combines the nearest domains under the same registrable
// root. Domain-tree proximity is the prior weight; once both domains have at
// least three common proxy observations, their performance-signature
// correlation becomes a posterior correction. ASN is deliberately excluded.
func (rt *RouteTable) SimilarTTFBPrior(key, domain, proxy string) (TTFBPrior, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	type neighbor struct{ value, weight float64 }
	neighbors := make([]neighbor, 0)
	var current *domainCell
	if row, ok := rt.rows[key]; ok {
		current = row.domainTable[domain]
	}
	for _, row := range rt.rows {
		for name, dc := range row.domainTable {
			if name == domain {
				continue
			}
			treeSimilarity := DomainTreeSimilarity(domain, name)
			if treeSimilarity == 0 {
				continue
			}
			// Keep most of the registrable-domain prior so a shallow tree does
			// not discard useful sibling evidence. Tree distance and the
			// posterior signature refine that prior instead of replacing it.
			weight := .75 + .25*treeSimilarity
			if current != nil {
				weight *= .75 + .25*performanceSignatureSimilarity(current, dc)
			}
			if cell, ok := dc.proxies[proxy]; ok && cell.HasTTFBSample && weight > 0 {
				neighbors = append(neighbors, neighbor{float64(cell.TTFB), weight})
			}
		}
	}
	if len(neighbors) == 0 {
		return TTFBPrior{}, false
	}
	sort.Slice(neighbors, func(i, j int) bool { return neighbors[i].weight > neighbors[j].weight })
	if len(neighbors) > 8 {
		neighbors = neighbors[:8]
	}
	total, mean := 0.0, 0.0
	for _, n := range neighbors {
		total, mean = total+n.weight, mean+n.value*n.weight
	}
	mean /= total
	variance := 0.0
	for _, n := range neighbors {
		variance += n.weight * (n.value - mean) * (n.value - mean)
	}
	variance /= total
	stddev := math.Sqrt(variance)
	avgWeight := total / float64(len(neighbors))
	if floor := mean * (.20 + .20*(1-avgWeight)); stddev < floor {
		stddev = floor
	}
	return TTFBPrior{Mean: mean, StdDev: stddev, Samples: len(neighbors)}, true
}

// RouteTTFBProxyCount returns how many proxies have a TTFB observation on
// this exact route. Sibling observations must not count as local coverage.
func (rt *RouteTable) RouteTTFBProxyCount(key, domain string) int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok {
		return 0
	}
	dc, ok := row.domainTable[domain]
	if !ok {
		return 0
	}
	count := 0
	for _, cell := range dc.proxies {
		if cell.HasTTFBSample {
			count++
		}
	}
	return count
}

// RouteFamilyTTFBProxyCount reports distinct proxies observed across a route
// family. It does not measure the coverage of an individual route.
func (rt *RouteTable) RouteFamilyTTFBProxyCount(domain string) int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	family := RouteFamily(domain)
	seen := make(map[string]struct{})
	for _, row := range rt.rows {
		for name, dc := range row.domainTable {
			if RouteFamily(name) != family {
				continue
			}
			for proxy, cell := range dc.proxies {
				if cell.HasTTFBSample {
					seen[proxy] = struct{}{}
				}
			}
		}
	}
	return len(seen)
}

// ExpectedRouteFutureRequests estimates demand for this exact route when
// deciding whether to accelerate its own initial exploration.
func (rt *RouteTable) ExpectedRouteFutureRequests(key, domain string, horizon time.Duration) float64 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok {
		return 1
	}
	dc, ok := row.domainTable[domain]
	if !ok {
		return 1
	}
	uses := int64(0)
	for _, cell := range dc.proxies {
		uses += cell.UseCount
	}
	if uses <= 1 || dc.firstUsed == 0 {
		return 1
	}
	elapsed := math.Max(time.Since(time.Unix(dc.firstUsed, 0)).Seconds(), 1)
	estimate := float64(uses) / elapsed * horizon.Seconds()
	return math.Max(1, math.Min(estimate, 32))
}

// ExpectedFutureRequests estimates near-term demand for sibling routes from
// their observed request rate. It is used only to budget risk pruning.
func (rt *RouteTable) ExpectedFutureRequests(domain string, horizon time.Duration) float64 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	family := RouteFamily(domain)
	uses, first := int64(0), int64(0)
	for _, row := range rt.rows {
		for name, dc := range row.domainTable {
			if RouteFamily(name) != family {
				continue
			}
			if first == 0 || dc.firstUsed < first {
				first = dc.firstUsed
			}
			for _, cell := range dc.proxies {
				uses += cell.UseCount
			}
		}
	}
	if uses <= 1 || first == 0 {
		return 1
	}
	elapsed := math.Max(time.Since(time.Unix(first, 0)).Seconds(), 1)
	estimate := float64(uses) / elapsed * horizon.Seconds()
	return math.Max(1, math.Min(estimate, 32))
}

func (rt *RouteTable) UpdateExplorationRegret(key, domain, challenger string, regret float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	dc := rt.getOrCreateDomainCell(row, domain)
	cell := rt.getOrCreateCell(dc, challenger)
	cell.ExplorationSamples++
	delta := regret - cell.ExplorationRegretMean
	cell.ExplorationRegretMean += delta / float64(cell.ExplorationSamples)
	cell.ExplorationRegretM2 += delta * (regret - cell.ExplorationRegretMean)
}

func (rt *RouteTable) ExplorationRisk(domain, challenger string) (ExplorationRisk, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	family := RouteFamily(domain)
	count, mean, m2 := int64(0), 0.0, 0.0
	for _, row := range rt.rows {
		for name, dc := range row.domainTable {
			if RouteFamily(name) != family {
				continue
			}
			cell, ok := dc.proxies[challenger]
			if !ok || cell.ExplorationSamples == 0 {
				continue
			}
			// Merge each cell's Welford accumulator without losing its within-cell variance.
			n := cell.ExplorationSamples
			delta := cell.ExplorationRegretMean - mean
			newCount := count + n
			mean += delta * float64(n) / float64(newCount)
			m2 += cell.ExplorationRegretM2 + delta*delta*float64(count*n)/float64(newCount)
			count = newCount
		}
	}
	if count == 0 {
		return ExplorationRisk{}, false
	}
	variance := 0.0
	if count > 1 {
		variance = m2 / float64(count-1)
	}
	return ExplorationRisk{Mean: mean, StdDev: math.Sqrt(variance), Samples: count}, true
}

// ProxyTTFBPrior returns a proxy's current-domain observation when available;
// otherwise it pools observations from other route/domain cells as a prior.
// The current cell is deliberately preferred so mature per-target knowledge
// is never diluted by unrelated destinations.
func (rt *RouteTable) ProxyTTFBPrior(key, domain, proxy string) (TTFBPrior, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	if row, ok := rt.rows[key]; ok {
		if dc, ok := row.domainTable[domain]; ok {
			if cell, ok := dc.proxies[proxy]; ok && cell.HasTTFBSample {
				stddev := cell.Jitter
				if !cell.HasJitterSample {
					stddev = float64(cell.TTFB) * 0.15
				}
				return TTFBPrior{Mean: float64(cell.TTFB), StdDev: stddev, Samples: 1}, true
			}
		}
	}

	values := make([]float64, 0)
	for rowKey, row := range rt.rows {
		for domainName, dc := range row.domainTable {
			if rowKey == key && domainName == domain {
				continue
			}
			if cell, ok := dc.proxies[proxy]; ok && cell.HasTTFBSample {
				values = append(values, float64(cell.TTFB))
			}
		}
	}
	if len(values) == 0 {
		return TTFBPrior{}, false
	}
	var mean float64
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))
	var variance float64
	for _, value := range values {
		delta := value - mean
		variance += delta * delta
	}
	variance /= float64(len(values))
	// A single aggregate cell has no between-domain variance. Preserve enough
	// uncertainty to allow a plausibly better proxy to be explored.
	stddev := math.Sqrt(variance)
	if floor := mean * 0.15; stddev < floor {
		stddev = floor
	}
	return TTFBPrior{Mean: mean, StdDev: stddev, Samples: len(values)}, true
}

// UpdateTTFB updates the EMA time-to-first-byte for a (key, domain, proxy)
// triple.  TTFB is measured end-to-end from dial start: it includes
// connecting to the proxy server and the protocol handshake, then the time to
// the first byte read from the target.  Jitter is updated alongside using the
// same sample: the absolute deviation of this sample from the previous TTFB
// EMA, smoothed with EMA.
func (rt *RouteTable) UpdateTTFB(key, domain, proxy string, ttfb int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	dc := rt.getOrCreateDomainCell(row, domain)
	cell := rt.getOrCreateCell(dc, proxy)
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
	rt.touchDomainLRU(row, domain)
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// UpdatePkgLoss updates the EMA packet loss for a (key, domain, proxy) triple.
func (rt *RouteTable) UpdatePkgLoss(key, domain, proxy string, loss float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	dc := rt.getOrCreateDomainCell(row, domain)
	cell := rt.getOrCreateCell(dc, proxy)
	cell.PkgLoss = applyEMA(float64(cell.PkgLoss), loss, cell.HasPkgLossSample)
	cell.HasPkgLossSample = true
	cell.Dirty = true
	rt.touchDomainLRU(row, domain)
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// UpdateSpeed updates the EMA speed for a (key, domain, proxy) triple.
func (rt *RouteTable) UpdateSpeed(key, domain, proxy string, speed float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	dc := rt.getOrCreateDomainCell(row, domain)
	cell := rt.getOrCreateCell(dc, proxy)
	cell.Speed = applyEMA(float64(cell.Speed), speed, cell.HasSpeedSample)
	cell.HasSpeedSample = true
	cell.Dirty = true
	rt.touchDomainLRU(row, domain)
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

// IncrementUseCount increments the use counter for a proxy within a route
// key's domain.  Also resets failedCount since a successful use means the
// proxy is working.
func (rt *RouteTable) IncrementUseCount(key, domain, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	dc := rt.getOrCreateDomainCell(row, domain)
	cell := rt.getOrCreateCell(dc, proxy)
	cell.UseCount++
	cell.FailedCount = 0
	cell.Dirty = true
	rt.touchDomainLRU(row, domain)
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// PreRankLatency sorts proxies by latency.  When key is non-empty only that
// key's domain samples are used (falling back to healthCheckLatency),
// preventing the first target's winner from biasing later probes via
// cross-domain aggregation.  key == "" preserves the legacy cross-row,
// cross-domain mean.  Sort is stable.
func (rt *RouteTable) PreRankLatency(proxies []string, healthCheckLatency func(string) uint16, key, domain string) []string {
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
		// Per-(key,domain): only use that specific domain's data
		row, ok := rt.rows[key]
		var dc *domainCell
		if ok {
			dc = row.domainTable[domain]
		}
		hasKeyData := false
		for _, proxy := range proxies {
			if dc != nil {
				if cell, cOk := dc.proxies[proxy]; cOk && cell.HasLatencySample {
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
		// Cross-row, cross-domain mean (legacy — used when no key is available)
		type sumCount struct {
			sum   int64
			count int
		}
		stats := make(map[string]*sumCount, len(proxies))
		for _, proxy := range proxies {
			stats[proxy] = &sumCount{}
		}

		for _, row := range rt.rows {
			for _, dc := range row.domainTable {
				for _, proxy := range proxies {
					if cell, cOk := dc.proxies[proxy]; cOk && cell.HasLatencySample {
						s := stats[proxy]
						s.sum += cell.Latency
						s.count++
					}
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

// RankByScore sorts proxies for a route key's domain. Once the domain has
// any TTFB sample it normally switches to TTFB-first ranking:
//   - proxies with a TTFB sample are ranked first, scored with TTFB replacing
//     latency as the response-time term; at most MaxTTFBProxiesPerRank of them
//     survive (the top-scored ones), so a crowded TTFB group cannot crowd out
//     the latency-ranked fallbacks;
//   - proxies without a TTFB sample whose measured dial latency already exceeds
//     the domain's minimum TTFB are skipped; the rest are ranked after the TTFB
//     group. URL-test delay is only a ranking fallback, never a pruning bound.
//
// With no TTFB sample at all (cold start for this domain) it falls back to
// latency-derived scores as before.
func (rt *RouteTable) RankByScore(proxies []string, urlTestDelay func(string) uint16, key, domain string) []string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	row, rowOK := rt.rows[key]
	var dc *domainCell
	if rowOK {
		dc = row.domainTable[domain]
	}

	// connSize gates the speed component.  A domain without a cell yet has
	// connSize 0, which also skips the speed term.
	var connSizeKB float64
	if dc != nil {
		connSizeKB = dc.connSize
	}

	// minTTFB is the smallest TTFB EMA among proxies with a sample in this domain.
	var minTTFB int64
	hasTTFB := false
	if dc != nil {
		for _, cell := range dc.proxies {
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
		if dc != nil {
			if c, ok := dc.proxies[proxy]; ok {
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

		// No TTFB sample: use the dial-latency EMA when the cell has data,
		// otherwise use URL-test only to order cold candidates. A failed-only
		// cell keeps rankingDelay 0 so its failure penalty drives it back.
		var rankingDelay int64
		var speed, pkgLoss, failedCount, jitter float64
		if cell != nil && cell.hasData() {
			rankingDelay = cell.Latency
			speed = cell.Speed
			pkgLoss = cell.PkgLoss
			failedCount = cell.FailedCount
			jitter = cell.Jitter
		} else if urlTestDelay != nil {
			if delay := urlTestDelay(proxy); delay != 0 && delay != 0xffff {
				rankingDelay = int64(delay)
			}
		}

		// Only a measured dial latency can bound this proxy's first-byte time.
		// The URL-test fallback includes a request to another target and
		// must never be used as a pruning bound.
		if hasTTFB && cell != nil && cell.HasLatencySample && cell.Latency > minTTFB {
			continue
		}

		cands = append(cands, candidate{
			name:  proxy,
			score: calculateScore(rankingDelay, speed, pkgLoss, failedCount, jitter, connSizeKB),
			ttfb:  false,
		})
	}

	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].ttfb != cands[j].ttfb {
			return cands[i].ttfb // TTFB group first
		}
		return cands[i].score > cands[j].score
	})

	// Only the top MaxTTFBProxiesPerRank TTFB proxies survive. The sort put
	// the TTFB group first (score-descending), so skipping past the first N
	// TTFB candidates keeps exactly the highest-scored TTFB proxies.
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
// specific route key's domain, or 0 when the row, domain or proxy has no
// record.  It backs discovery ordering (exploreOrder) so a proxy that has
// only failed for this (key, domain) — and thus has no sample and is absent
// from the UseCount-weighted proxyAttrs aggregation — is still deferred to
// the end of the probe batch.
func (rt *RouteTable) ProxyFailedCount(key, domain, proxy string) float64 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	row, ok := rt.rows[key]
	if !ok {
		return 0
	}
	dc, ok := row.domainTable[domain]
	if !ok {
		return 0
	}
	cell, ok := dc.proxies[proxy]
	if !ok {
		return 0
	}
	return cell.FailedCount
}

// MarkFailed penalizes a proxy within a route key's domain and, when that
// domain's best points at it, clears the domain's best/tcpProbed so it
// re-discovers.  FailedCount is domain-scoped: a failure on one domain no
// longer degrades the proxy's score on unrelated domains sharing the same
// ASN row.
func (rt *RouteTable) MarkFailed(key, proxy, domain string, penalty float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	dc := rt.getOrCreateDomainCell(row, domain)
	cell := rt.getOrCreateCell(dc, proxy)
	cell.FailedCount = math.Min(cell.FailedCount+penalty, maxFailedCount)
	cell.Dirty = true

	if dc.tcpBestProxy == proxy {
		dc.tcpBestProxy = ""
		dc.tcpProbed = false
		row.rowDirty = true
	}
}

// MarkUDPFailed records the shared quality penalty and clears only UDP routing
// state. A UDP failure must not invalidate a separately learned TCP best.
func (rt *RouteTable) MarkUDPFailed(key, proxy, domain string, penalty float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	dc := rt.getOrCreateDomainCell(row, domain)
	cell := rt.getOrCreateCell(dc, proxy)
	cell.FailedCount = math.Min(cell.FailedCount+penalty, maxFailedCount)
	cell.Dirty = true
	if dc.udpBestProxy == proxy {
		dc.udpBestProxy = ""
		row.rowDirty = true
	}
}

// DecayFailedCounts reduces every cell's FailedCount by 0.1 (floor 0) across all
// rows and domains. Cells that change are marked dirty so they are persisted
// on the next cycle.
func (rt *RouteTable) DecayFailedCounts() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	dirtyCount := 0
	for _, row := range rt.rows {
		for _, dc := range row.domainTable {
			for _, cell := range dc.proxies {
				if cell.FailedCount > 0 {
					cell.FailedCount = math.Max(0, cell.FailedCount-0.1)
					cell.Dirty = true
					dirtyCount++
				}
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
// The map key is "{routeKey}\x00{domain}\x00{proxyName}".
func (rt *RouteTable) SnapshotAndClearDirty() map[string]PersistedCell {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	snapshot := make(map[string]PersistedCell)
	for key, row := range rt.rows {
		for domain, dc := range row.domainTable {
			for _, cell := range dc.proxies {
				if cell.Dirty {
					cellKey := key + "\x00" + domain + "\x00" + cell.Name
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
	}
	return snapshot
}

// MarkDirty sets the dirty flag on a cell so it will be included in the next
// periodic persist cycle.  Unlike RestoreRow, it does not touch cell data,
// LRU order, or the Score field.
func (rt *RouteTable) MarkDirty(key, domain, proxy string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row, ok := rt.rows[key]
	if !ok {
		return
	}
	dc, ok := row.domainTable[domain]
	if !ok {
		return
	}
	cell, ok := dc.proxies[proxy]
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

// RestoreRow restores a per-(key,domain,proxy) cell from previously persisted
// data.  The restored cell has dirty=false so it won't be flushed until modified.
func (rt *RouteTable) RestoreRow(key, domain, proxy string, pc PersistedCell) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	row := rt.getOrCreateRow(key)
	dc := rt.getOrCreateDomainCell(row, domain)
	cell := rt.getOrCreateCell(dc, proxy)
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
	rt.touchDomainLRU(row, domain)
	row.lastUsed = time.Now().Unix()
	rt.touchLRU(key)
}

// PersistedDomain is the JSON-serializable form of a domainCell's routing
// state (bestProxy, tcpProbed) plus its connection-size EMA so the connSize
// gating in calculateScore survives a restart.
type PersistedDomain struct {
	BestProxy         string  `json:"best_proxy,omitempty"` // legacy and TCP best compatibility
	TCPBestProxy      string  `json:"tcp_best_proxy,omitempty"`
	UDPBestProxy      string  `json:"udp_best_proxy,omitempty"`
	TCPEvaluatedAt    int64   `json:"tcp_evaluated_at,omitempty"`
	UDPEvaluatedAt    int64   `json:"udp_evaluated_at,omitempty"`
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

// PersistedTable identifies the authoritative live row set. Individual row
// records are update-based and can outlive an in-memory LRU eviction.
type PersistedTable struct {
	Rows []string `json:"rows"`
}

// SnapshotAndClearTableMeta returns the current live row set when row
// membership changed since the last snapshot.
func (rt *RouteTable) SnapshotAndClearTableMeta() (PersistedTable, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.tableDirty {
		return PersistedTable{}, false
	}
	rows := append([]string(nil), rt.lruOrder...)
	rt.tableDirty = false
	return PersistedTable{Rows: rows}, true
}

func (rt *RouteTable) MarkTableDirty() {
	rt.mu.Lock()
	rt.tableDirty = true
	rt.mu.Unlock()
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
			cell.tcpBestProxy = pd.TCPBestProxy
			if cell.tcpBestProxy == "" {
				cell.tcpBestProxy = pd.BestProxy
			}
			cell.udpBestProxy = pd.UDPBestProxy
			cell.tcpProbed = pd.TCPProbed
			cell.connSize = pd.ConnSize
			cell.hasConnSizeSample = pd.HasConnSizeSample
			cell.lastUsed = now
			cell.tcpEvaluatedAt = pd.TCPEvaluatedAt
			if cell.tcpBestProxy != "" && cell.tcpEvaluatedAt == 0 {
				cell.tcpEvaluatedAt = now
			}
			cell.udpEvaluatedAt = pd.UDPEvaluatedAt
			if cell.udpBestProxy != "" && cell.udpEvaluatedAt == 0 {
				cell.udpEvaluatedAt = now
			}
		}
	} else if pr.BestProxy != "" {
		// Legacy row-level best: only a TARGET row can reconstruct its domain
		// from the key ("TARGET:<domain>").  ASN rows drop their old best and
		// re-learn per-domain on the next discovery.
		if domain, ok := strings.CutPrefix(key, "TARGET:"); ok {
			cell := rt.getOrCreateDomainCell(row, domain)
			cell.tcpBestProxy = pr.BestProxy
			cell.tcpProbed = pr.TCPProbed
			cell.lastUsed = now
			cell.tcpEvaluatedAt = now
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
					BestProxy:         cell.tcpBestProxy,
					TCPBestProxy:      cell.tcpBestProxy,
					UDPBestProxy:      cell.udpBestProxy,
					TCPEvaluatedAt:    cell.tcpEvaluatedAt,
					UDPEvaluatedAt:    cell.udpEvaluatedAt,
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
		for _, dc := range row.domainTable {
			delete(dc.proxies, name)
			for i, proxyName := range dc.proxyOrder {
				if proxyName == name {
					dc.proxyOrder = append(dc.proxyOrder[:i], dc.proxyOrder[i+1:]...)
					break
				}
			}
			if dc.tcpBestProxy == name {
				dc.tcpBestProxy = ""
				dc.tcpProbed = false
				row.rowDirty = true
			}
			if dc.udpBestProxy == name {
				dc.udpBestProxy = ""
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
		domains := make([]DomainSnapshot, 0, len(row.domainTable))
		var bestBestProxy string
		var bestTCPProbed bool
		var bestLastUsed int64 = -1
		for _, dc := range row.domainTable {
			proxies := make(map[string]ProxyRecord, len(dc.proxies))
			for _, cell := range dc.proxies {
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
			domains = append(domains, DomainSnapshot{
				Name:         dc.domainName,
				BestProxy:    dc.tcpBestProxy,
				UDPBestProxy: dc.udpBestProxy,
				TCPProbed:    dc.tcpProbed,
				LastUsed:     dc.lastUsed,
				ConnSize:     dc.connSize,
				Proxies:      proxies,
			})
			if dc.tcpBestProxy != "" && dc.lastUsed > bestLastUsed {
				bestLastUsed = dc.lastUsed
				bestBestProxy = dc.tcpBestProxy
				bestTCPProbed = dc.tcpProbed
			}
		}
		sort.Slice(domains, func(i, j int) bool {
			return domains[i].LastUsed > domains[j].LastUsed
		})

		// BestProxy/TCPProbed at the row level reflect the most-recently-used
		// domain that has a best, kept for display compatibility.  The
		// authoritative per-domain state lives in Domains.
		rows = append(rows, RowSnapshot{
			Key:       row.key,
			BestProxy: bestBestProxy,
			TCPProbed: bestTCPProbed,
			LastUsed:  row.lastUsed,
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

// DebugDumpRow returns a debug string of a single row's domains and, within
// each domain, its own proxy metrics.
func (rt *RouteTable) DebugDumpRow(key string) string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	row, ok := rt.rows[key]
	if !ok || len(row.domainTable) == 0 {
		return fmt.Sprintf("key=%s domains=<empty>", key)
	}

	domainNames := make([]string, 0, len(row.domainTable))
	for d := range row.domainTable {
		domainNames = append(domainNames, d)
	}
	sort.Strings(domainNames)

	type entry struct {
		name    string
		latency int64
		ttfb    int64
		use     int64
		fail    float64
		loss    float64
		jitter  float64
		speed   float64
		score   float64
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("key=%s domains=[", key))
	for i, d := range domainNames {
		dc := row.domainTable[d]
		if i > 0 {
			sb.WriteString(" ")
		}

		entries := make([]entry, 0, len(dc.proxies))
		for _, cell := range dc.proxies {
			entries = append(entries, entry{
				name:    cell.Name,
				latency: cell.Latency,
				ttfb:    cell.TTFB,
				use:     cell.UseCount,
				fail:    cell.FailedCount,
				loss:    cell.PkgLoss,
				jitter:  cell.Jitter,
				speed:   cell.Speed,
				score:   cell.Score,
			})
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].latency != entries[j].latency {
				return entries[i].latency < entries[j].latency
			}
			return entries[i].name < entries[j].name
		})

		var pb strings.Builder
		for j, e := range entries {
			if j > 0 {
				pb.WriteString(",")
			}
			pb.WriteString(fmt.Sprintf("%s(lat=%d,ttfb=%d,use=%d,fail=%.1f,loss=%.3f,jit=%.1f,spd=%.0f,score=%.4f)",
				e.name, e.latency, e.ttfb, e.use, e.fail, e.loss, e.jitter, e.speed, e.score))
		}

		sb.WriteString(fmt.Sprintf("%s(tcpBest=%s,udpBest=%s,tcpProbed=%v,proxies=[%s])", d, dc.tcpBestProxy, dc.udpBestProxy, dc.tcpProbed, pb.String()))
	}
	sb.WriteString("]")
	return sb.String()
}
