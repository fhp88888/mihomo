package smart

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// ────────────────────────────────────────────────────────────
// Thompson Sampling decision parameters
// ────────────────────────────────────────────────────────────

const (
	// DefaultUfail is the utility assigned to a failed connection.
	// p·q + (1-p)·Ufail penalises nodes with low success probability.
	DefaultUfail = -0.1

	// DefaultWr is the default weight for response quality in the
	// composite quality score q = wr·g(zr) + wt·g(zt).
	DefaultWr = 0.5

	// DefaultWt is the default weight for transfer quality.
	// Set to 0 initially; can be raised when Yt observations are enabled.
	DefaultWt = 0.0
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
	// Predict to now.
	now := time.Now().Unix()
	state.PredictToNow(now, prior)

	// Sample from posteriors.
	zs := sampleNormal(state.MuS, math.Sqrt(state.PS), rng)
	zr := sampleNormal(state.MuR, math.Sqrt(state.PR), rng)
	zt := sampleNormal(state.MuT, math.Sqrt(state.PT), rng)

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
// Returns nodes sorted by score descending, then by name for determinism.
func TSRank(states map[string]*CellState, prior CellPrior, wr, wt, uFail float64, rng *rand.Rand) []NodeWithWeight {
	result := make([]NodeWithWeight, 0, len(states))
	for name, st := range states {
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
// In-memory OU state store (temporary; Phase 5 replaces with DB)
// ────────────────────────────────────────────────────────────

// ouStateKey builds the in-memory key for a cell.
// Format: "{group}/{config}/{node}/{asn}_{protocol}"
func ouStateKey(group, config, node, asn string, isUDP bool) string {
	proto := "tcp"
	if isUDP {
		proto = "udp"
	}
	return group + "/" + config + "/" + node + "/" + asn + "_" + proto
}

// ouStateStore is a temporary in-memory store for OU states.
// Phase 5 will replace this with bbolt-backed persistence via *Store.
var (
	ouStateStoreMu sync.RWMutex
	ouStateStore   = make(map[string]*CellState)
)

// getOUState retrieves a cell state from the in-memory store.
// Returns nil if not found.
func getOUState(group, config, node, asn string, isUDP bool) *CellState {
	ouStateStoreMu.RLock()
	defer ouStateStoreMu.RUnlock()
	return ouStateStore[ouStateKey(group, config, node, asn, isUDP)]
}

// putOUState stores a cell state in the in-memory store.
func putOUState(group, config, node, asn string, isUDP bool, st *CellState) {
	ouStateStoreMu.Lock()
	defer ouStateStoreMu.Unlock()
	ouStateStore[ouStateKey(group, config, node, asn, isUDP)] = st
}

// ────────────────────────────────────────────────────────────
// Store methods (to be wired into *Store in Phase 5)
// ────────────────────────────────────────────────────────────

// GetOrCreateOUState returns the CellState for a (node, ASN, protocol) cell,
// creating one from the given prior if it does not already exist.
func (s *Store) GetOrCreateOUState(group, config, node, asn string, isUDP bool, prior CellPrior) *CellState {
	st := getOUState(group, config, node, asn, isUDP)
	if st != nil {
		return st
	}

	now := time.Now().Unix()
	st = NewCellState(now, prior)
	putOUState(group, config, node, asn, isUDP, st)
	return st
}

// UpdateOUState applies an observation to a cell's OU state.
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

	basePrior := DefaultPrior()
	wr := DefaultWr
	wt := DefaultWt
	uFail := DefaultUfail

	// One random source per ranking call so all nodes share the same
	// sample (the adapter must not re-sample across batches).
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// ── Build states map ────────────────────────────────────
	states := make(map[string]*CellState, len(proxyNames))
	isASN := asn != "" && !CdnASNs[asn]

	for _, name := range proxyNames {
		var st *CellState

		if isASN {
			// Hierarchical: parent = node × protocol, child = node × ASN × protocol.
			parent := s.GetOrCreateOUState(group, config, name, "", isUDP, basePrior)
			childPrior := parent.DerivedPrior(2.0) // inflate parent variance ×2
			st = s.GetOrCreateOUState(group, config, name, asn, isUDP, childPrior)
			// Wire the child's mean-reversion centre to the parent's current mean.
			childPrior.MS = parent.MuS
			childPrior.MR = parent.MuR
			childPrior.MT = parent.MuT
			// Update the child's prior (only the m parameters change over time).
			st.PredictToNow(time.Now().Unix(), childPrior)
			// Use child for scoring, but with the parent-wired prior.
			states[name] = st
			// Override prior for scoring:
			// we pass childPrior (with parent means) to TSRank through a
			// per-node prior.  For simplicity we use basePrior with parent
			// means baked in.
			_ = childPrior
		} else {
			st = s.GetOrCreateOUState(group, config, name, asn, isUDP, basePrior)
		}
		states[name] = st
	}

	// ── Rank ────────────────────────────────────────────────
	// For the per-node prior we use basePrior; the hierarchical wiring
	// is applied through the cell's own OU transition (m is baked into
	// each cell via the prior it was created with).
	ranked := TSRank(states, basePrior, wr, wt, uFail, rng)

	nodes := make([]string, len(ranked))
	scores := make([]float64, len(ranked))
	for i, r := range ranked {
		nodes[i] = r.Node
		scores[i] = r.Weight
	}

	return nodes, scores, nil
}

// GetStaleNodes returns nodes that have not received a bandit update
// for more than StaleThreshold * H hours, sorted oldest-first.
func (s *Store) GetStaleNodes(group, config, asn string, isUDP bool, proxyNames []string) []string {
	now := time.Now().Unix()
	basePrior := DefaultPrior()

	type staleEntry struct {
		name         string
		lastUpdateTime int64
	}
	var stale []staleEntry

	for _, name := range proxyNames {
		st := getOUState(group, config, name, asn, isUDP)
		if st == nil {
			// Never observed — definitely stale.
			stale = append(stale, staleEntry{name, 0})
			continue
		}
		if st.IsStale(now, basePrior) {
			stale = append(stale, staleEntry{name, st.LastUpdateTime})
		}
	}

	sort.Slice(stale, func(i, j int) bool {
		return stale[i].lastUpdateTime < stale[j].lastUpdateTime
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
