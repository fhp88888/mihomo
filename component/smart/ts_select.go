package smart

import (
	"encoding/json"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/metacubex/mihomo/common/lru"
)

// ────────────────────────────────────────────────────────────
// Thompson Sampling decision parameters
// ────────────────────────────────────────────────────────────

const (
	// DefaultUfail is the utility assigned to a failed connection.
	// p·q + (1-p)·Ufail penalises nodes with low success probability.
	DefaultUfail = -0.2

	// DefaultWr is the default weight for response quality in the
	// composite quality score q = wr·g(zr) + wt·g(zt).
	DefaultWr = 0.6

	// DefaultWt is the default weight for transfer quality.
	// Set to 0 initially; can be raised when Yt observations are enabled.
	DefaultWt = 0.2
)

// ────────────────────────────────────────────────────────────
// Pure scoring functions (no store dependency)
// ────────────────────────────────────────────────────────────

// TSScore computes the Thompson-sampling utility for one node.
//
//	zs ~ N(state.MuS, state.PS)     → p  = σ(zs)
//	zr ~ N(state.MuR, state.PR)     → qr = E[σ(zr + ε)]
//	zt ~ N(state.MuT, state.PT)     → qt = E[σ(zt + ε)]
//	q  = wr·qr + wt·qt
//	U  = p·q + (1-p)·Ufail
//
// The caller provides a *rand.Rand so that all nodes in one request
// share the same random source (one sample per request).
func TSScore(state *CellState, prior CellPrior, wr, wt, uFail float64, rng *rand.Rand) float64 {
	if state == nil {
		return uFail
	}

	// Score on a copy so ranking/prediction never mutates persisted freshness.
	scoringState := state.Clone()
	if scoringState == nil {
		return uFail
	}

	// Predict to now.
	now := time.Now().Unix()
	scoringState.PredictToNow(now, prior)

	// Sample from posteriors.
	zs := sampleNormal(scoringState.MuS, math.Sqrt(scoringState.PS), rng)
	zr := sampleNormal(scoringState.MuR, math.Sqrt(scoringState.PR), rng)
	zt := sampleNormal(scoringState.MuT, math.Sqrt(scoringState.PT), rng)

	// Success probability.
	p := sigmoid(zs)

	// Expected quality, integrated over observation noise.
	qr := SigmaGaussHermite(zr, prior.VnR)
	qt := SigmaGaussHermite(zt, prior.VnT)
	q := wr*qr + wt*qt

	// Thompson utility.
	return p*q + (1.0-p)*uFail
}

// TSRank ranks a set of nodes by their TS utility (descending).
//
// Each node's state is PredictToNow'd before sampling.  The same *rand.Rand
// is used for all nodes, ensuring a consistent comparison within one request.
//
// priors maps node names to their per-node OU prior (mean-reversion centre
// and noise).  If a node has no entry, DefaultPrior() is used.
//
// Returns nodes sorted by score descending, then by name for determinism.
func TSRank(states map[string]*CellState, priors map[string]CellPrior, wr, wt, uFail float64, rng *rand.Rand) []NodeWithWeight {
	result := make([]NodeWithWeight, 0, len(states))
	for name, st := range states {
		prior, ok := priors[name]
		if !ok {
			prior = DefaultPrior()
		}
		score := TSScore(st, prior, wr, wt, uFail, rng)
		result = append(result, NodeWithWeight{Node: name, Weight: score})
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Weight != result[j].Weight {
			return result[i].Weight > result[j].Weight
		}
		return result[i].Node < result[j].Node
	})

	return result
}

// ────────────────────────────────────────────────────────────
// OU state DB-backed persistence via *Store
// ────────────────────────────────────────────────────────────

// ouStateDBKey builds the bbolt key for a cell.
// Format: "smart/ou_state/{config}/{group}/{node}_{protocol}_{asn}"
func ouStateDBKey(config, group, node, asn string, isUDP bool) string {
	proto := "tcp"
	if isUDP {
		proto = "udp"
	}
	return FormatDBKey(KeyTypeOUState, config, group, node+"_"+proto+"_"+asn)
}

// cacheKey builds the in-memory cache key for a cell.
func ouStateCacheKey(config, group, node, asn string, isUDP bool) string {
	proto := "tcp"
	if isUDP {
		proto = "udp"
	}
	return config + "/" + group + "/" + node + "/" + asn + "_" + proto
}

// GetOrCreateOUState returns the CellState for a (node, ASN, protocol) cell.
// It tries the LRU cache first, then bbolt DB, then creates a new state
// initialised from the prior.
// ensureOUStateCache initialises ouStateCache lazily (needed for tests).
func ensureOUStateCache() {
	if ouStateCache == nil {
		ouStateCache = lru.New[string, *CellState](
			lru.WithSize[string, *CellState](500),
			lru.WithAge[string, *CellState](600),
		)
	}
}

func (s *Store) GetOrCreateOUState(group, config, node, asn string, isUDP bool, prior CellPrior) *CellState {
	ensureOUStateCache()
	ck := ouStateCacheKey(config, group, node, asn, isUDP)

	// 1. LRU cache hit.
	if cached, ok := ouStateCache.Get(ck); ok {
		return cached
	}

	// 2. Try bbolt DB.
	dbKey := ouStateDBKey(config, group, node, asn, isUDP)
	if data, err := s.DBViewGetItem(dbKey); err == nil && len(data) > 0 {
		var st CellState
		if json.Unmarshal(data, &st) == nil {
			ouStateCache.Set(ck, &st)
			return &st
		}
	}

	// 3. Create new.
	now := time.Now().Unix()
	st := NewCellState(now, prior)
	ouStateCache.Set(ck, st)
	return st
}

// saveOUState persists a CellState to the async write queue.
func (s *Store) saveOUState(group, config, node, asn string, isUDP bool, st *CellState) {
	data, err := json.Marshal(st)
	if err != nil {
		return
	}
	s.AppendToGlobalQueue(StoreOperation{
		Type:   OpSaveOUState,
		Group:  group,
		Config: config,
		Node:   node + "_" + boolToUDPStr(isUDP) + "_" + asn,
		Data:   data,
	})
}

func boolToUDPStr(isUDP bool) string {
	if isUDP {
		return "udp"
	}
	return "tcp"
}

// UpdateOUState applies an observation to a cell's OU state and persists.
func (s *Store) UpdateOUState(group, config, node, asn string, isUDP bool, prior CellPrior, obs ObsResult) {
	st := s.GetOrCreateOUState(group, config, node, asn, isUDP, prior)
	now := time.Now().Unix()

	ev := CellEvent{
		Time: now,
		HasS: obs.HasS,
		S:    obs.S,
		HasR: obs.HasR,
		Yr:   obs.Yr,
		HasT: obs.HasT,
		Yt:   obs.Yt,
	}
	st.ApplyObservation(now, prior, ev)

	// Persist to bbolt via async queue.
	s.saveOUState(group, config, node, asn, isUDP, st)
}

// warmStartParentPrior computes a warm-start prior for a node × protocol
// parent by aggregating the node's success/failure counts across all known
// targets from the old stats table.  If insufficient data exists it falls
// back to DefaultPrior().
func (s *Store) warmStartParentPrior(group, config, node string, isUDP bool) CellPrior {
	// If no DB backend (tests), return neutral prior.
	if db == nil {
		return DefaultPrior()
	}
	// Try to read aggregate stats from the old stats table.
	allStats, err := s.GetAllStats(group, config)
	if err != nil || len(allStats) == 0 {
		return DefaultPrior()
	}

	var totalSuccess, totalFailure int64
	var sumConnectTime, sumLatency int64
	var sumMaxDR, sumMaxUR float64
	var countWithLatency, countWithRate float64

	for _, nodeStats := range allStats {
		data, ok := nodeStats[node]
		if !ok {
			continue
		}
		var record StatsRecord
		if json.Unmarshal(data, &record) != nil {
			continue
		}
		totalSuccess += record.Success
		totalFailure += record.Failure

		if record.ConnectTime > 0 || record.Latency > 0 {
			sumConnectTime += record.ConnectTime
			sumLatency += record.Latency
			countWithLatency++
		}
		if record.MaxDownloadRate > 0 || record.MaxUploadRate > 0 {
			sumMaxDR += record.MaxDownloadRate
			sumMaxUR += record.MaxUploadRate
			countWithRate++
		}
	}

	total := totalSuccess + totalFailure
	if total < 3 {
		return DefaultPrior()
	}

	prior := DefaultPrior()

	// S: aggregate success rate → log-odds (shrink toward 0.5).
	shrunkRate := float64(totalSuccess+1) / float64(total+2)
	prior.MS = Logit(clampEps(shrunkRate))
	prior.VfS = DefaultVfS / math.Sqrt(float64(total)+1)

	// Yr: reuse extractResponseQuality on avg response time.
	if countWithLatency > 0 {
		avgCT := sumConnectTime / int64(countWithLatency)
		avgLat := sumLatency / int64(countWithLatency)
		prior.MR = extractResponseQuality(avgCT, avgLat, 5*time.Second)
		prior.VfR = DefaultVfR / math.Sqrt(countWithLatency+1)
	}

	// Yt: reuse extractTransferQuality on avg throughput.
	if countWithRate > 0 {
		avgDR := sumMaxDR / countWithRate
		avgUR := sumMaxUR / countWithRate
		prior.MT = extractTransferQuality(avgDR, avgUR)
		prior.VfT = DefaultVfT / math.Sqrt(countWithRate+1)
	}

	return prior
}

// GetTSProxyRankingForTarget is the TS replacement for GetUCB1ProxyRankingForTarget.
//
// It ranks the given proxyNames for the specified (target, asn, protocol) using
// Thompson sampling with hierarchical priors:
//   - A node × protocol parent state provides the prior mean.
//   - An ASN-specific child state inflates the parent's variance.
//
// Once the child has its own observations the parent only serves as the
// mean-reversion centre (via the OU transition's m parameter).
func (s *Store) GetTSProxyRankingForTarget(
	group, config, target, asn string, isUDP bool,
	proxyNames []string,
) ([]string, []float64, error) {
	if len(proxyNames) == 0 {
		return nil, nil, nil
	}

	wr := DefaultWr
	wt := DefaultWt
	uFail := DefaultUfail

	// One random source per ranking call so all nodes share the same
	// sample (the adapter must not re-sample across batches).
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// ── Build states map ────────────────────────────────────
	states := make(map[string]*CellState, len(proxyNames))
	priors := make(map[string]CellPrior, len(proxyNames))
	isASN := asn != "" && !CdnASNs[asn]

	for _, name := range proxyNames {
		var st *CellState
		var scoringPrior CellPrior
		scoringPrior = DefaultPrior()

		if isASN {
			// Hierarchical: parent = node × protocol, child = node × ASN × protocol.
			parentPrior := s.warmStartParentPrior(group, config, name, isUDP)
			parent := s.GetOrCreateOUState(group, config, name, "", isUDP, parentPrior)
			childPrior := parent.DerivedPrior(2.0) // inflate parent variance ×2
			st = s.GetOrCreateOUState(group, config, name, asn, isUDP, childPrior)

			// Wire the child's mean-reversion centre to the parent's current mean.
			scoringPrior = childPrior
			scoringPrior.MS = parent.MuS
			scoringPrior.MR = parent.MuR
			scoringPrior.MT = parent.MuT

		} else {
			parentPrior := s.warmStartParentPrior(group, config, name, isUDP)
			st = s.GetOrCreateOUState(group, config, name, asn, isUDP, parentPrior)
			scoringPrior = parentPrior
		}
		states[name] = st
		priors[name] = scoringPrior
	}

	// each cell via the prior it was created with).
	ranked := TSRank(states, priors, wr, wt, uFail, rng)

	nodes := make([]string, len(ranked))
	scores := make([]float64, len(ranked))
	for i, r := range ranked {
		nodes[i] = r.Node
		scores[i] = r.Weight
	}

	return nodes, scores, nil
}

// peekOUState looks up a cell state without creating.  Returns nil if
// not found in cache or DB.
func (s *Store) peekOUState(group, config, node, asn string, isUDP bool) *CellState {
	ensureOUStateCache()
	ck := ouStateCacheKey(config, group, node, asn, isUDP)
	if cached, ok := ouStateCache.Get(ck); ok {
		return cached
	}
	dbKey := ouStateDBKey(config, group, node, asn, isUDP)
	if data, err := s.DBViewGetItem(dbKey); err == nil && len(data) > 0 {
		var st CellState
		if json.Unmarshal(data, &st) == nil {
			ouStateCache.Set(ck, &st)
			return &st
		}
	}
	return nil
}

// GetStaleNodes returns nodes that have not received a bandit update
// for more than StaleThreshold * H hours, sorted oldest-first.
func (s *Store) GetStaleNodes(group, config, asn string, isUDP bool, proxyNames []string) []string {
	now := time.Now().Unix()
	basePrior := DefaultPrior()

	type staleEntry struct {
		name             string
		lastObservedTime int64
	}
	var stale []staleEntry

	for _, name := range proxyNames {
		st := s.peekOUState(group, config, name, asn, isUDP)
		if st == nil {
			// Never observed — definitely stale.
			stale = append(stale, staleEntry{name, 0})
			continue
		}
		if st.IsStale(now, basePrior) {
			stale = append(stale, staleEntry{name, st.LastObservedAt()})
		}
	}

	sort.Slice(stale, func(i, j int) bool {
		return stale[i].lastObservedTime < stale[j].lastObservedTime
	})

	result := make([]string, len(stale))
	for i, e := range stale {
		result[i] = e.name
	}
	return result
}

// ────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────

// sampleNormal draws from N(mean, std) using the provided random source.
func sampleNormal(mean, std float64, rng *rand.Rand) float64 {
	if std <= 0 {
		return mean
	}
	return mean + std*rng.NormFloat64()
}
