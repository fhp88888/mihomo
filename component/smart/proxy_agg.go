package smart

import (
	"math"
	"sort"
	"time"
)

// ProxyAggregate is the per-proxy summary produced by AggregateByProxy.
// Each proxy that has at least one sampled cell appears once, with its
// attributes being the UseCount-weighted mean across all rows where the
// proxy has been used.
type ProxyAggregate struct {
	Name       string          `json:"name"`
	UseCount   int64           `json:"use_count"`
	Attributes ProxyAttributes `json:"attributes"`
	Rows       int             `json:"rows"` // number of rows contributing samples
}

// ProxyAggregation is the whole-table aggregation result (directly
// serialized for the REST API).
type ProxyAggregation struct {
	Group     string           `json:"group"`
	UpdatedAt int64            `json:"updated_at"` // 0 until the first aggregation runs
	Count     int              `json:"count"`
	Proxies   []ProxyAggregate `json:"proxies"`
}

// AggregateByProxy rolls every sampled cell in the table up to one entry per
// proxy.  Within each row a cell's weight is its UseCount divided by the
// row's total UseCount, so heavily-used proxies dominate their row's
// contribution.  Cells without any sample are skipped entirely — they have no
// data to contribute and would otherwise drag the means toward zero.
//
// The result is both returned and pushed back into rt.proxyAttrs, where it
// backs the proxy-wise component of calculateScore (via SetProxyAttrs).
// The per-proxy Score uses the raw atom formula — this data IS the proxy-wise
// view, so blending it with itself would be a self-reference.
func (rt *RouteTable) AggregateByProxy() ProxyAggregation {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// name -> accumulator
	type agg struct {
		name       string
		useCount   int64
		rows       int
		totalW     float64
		wLatency   float64
		wPkgLoss   float64
		wSpeed     float64
		wJitter    float64
		wFailed    float64
	}
	accum := make(map[string]*agg)

	for _, row := range rt.rows {
		var rowTotal int64
		for _, cell := range row.proxies {
			if cell.hasSample() {
				rowTotal += cell.UseCount
			}
		}
		if rowTotal <= 0 {
			continue
		}

		for _, cell := range row.proxies {
			if !cell.hasSample() {
				continue
			}
			a := accum[cell.Name]
			if a == nil {
				a = &agg{name: cell.Name}
				accum[cell.Name] = a
			}
			weight := float64(cell.UseCount) / float64(rowTotal)
			a.useCount += cell.UseCount
			a.rows++
			a.totalW += weight
			a.wLatency += float64(cell.Latency) * weight
			a.wPkgLoss += cell.PkgLoss * weight
			a.wSpeed += cell.Speed * weight
			a.wJitter += cell.Jitter * weight
			a.wFailed += cell.FailedCount * weight
		}
	}

	proxies := make([]ProxyAggregate, 0, len(accum))
	for _, a := range accum {
		// A proxy's totalW is the sum of its per-row weights (each ≤ 1), so
		// it can exceed 1 across multiple rows — divide to get a proper
		// weighted mean. Guard zero to skip proxies with no contribution.
		div := a.totalW
		if div <= 0 {
			continue
		}
		latency := int64(math.Round(a.wLatency / div))
		speed := a.wSpeed / div
		pkgLoss := a.wPkgLoss / div
		jitter := a.wJitter / div
		failedCount := a.wFailed / div

		proxies = append(proxies, ProxyAggregate{
			Name:     a.name,
			UseCount: a.useCount,
			Rows:     a.rows,
			Attributes: ProxyAttributes{
				Latency:     latency,
				Speed:       speed,
				PkgLoss:     pkgLoss,
				Jitter:      jitter,
				FailedCount: failedCount,
				Score:       calculateScoreAtom(latency, speed, pkgLoss, failedCount, jitter),
			},
		})
	}

	// Heaviest use first; ties broken by name for stable output.
	sort.Slice(proxies, func(i, j int) bool {
		if proxies[i].UseCount != proxies[j].UseCount {
			return proxies[i].UseCount > proxies[j].UseCount
		}
		return proxies[i].Name < proxies[j].Name
	})

	result := ProxyAggregation{
		UpdatedAt: time.Now().Unix(),
		Count:     len(proxies),
		Proxies:   proxies,
	}

	// Push back so the proxy-wise score component reflects this aggregation.
	attrs := make(map[string]ProxyAttributes, len(proxies))
	for i := range proxies {
		attrs[proxies[i].Name] = proxies[i].Attributes
	}
	rt.proxyAttrs = attrs

	return result
}

// BuildNodeRanking derives the legacy MostUsed / OccasionalUsed / RarelyUsed
// ranking from a UseCount-sorted proxy aggregation (see AggregateByProxy).  It
// preserves the NodeRank shape returned by the /weights REST API so the GUI
// contract is unchanged.  Proxies that were never used (UseCount <= 0) are
// omitted, mirroring the old behaviour where nodes without a positive weight
// carried no rank.
func BuildNodeRanking(proxies []ProxyAggregate) NodeRank {
	items := make([]NodeRankItem, 0, len(proxies))

	var maxUse int64
	for _, p := range proxies {
		if p.UseCount > maxUse {
			maxUse = p.UseCount
		}
	}

	for _, p := range proxies {
		if p.UseCount <= 0 {
			continue
		}
		weight := 0.0
		if maxUse > 0 {
			weight = math.Round(float64(p.UseCount)/float64(maxUse)*100*100) / 100
		}
		items = append(items, NodeRankItem{Name: p.Name, Weight: weight})
	}

	positive := len(items)
	mostUsedBound := int(float64(positive) * 0.2)
	if mostUsedBound < 1 {
		mostUsedBound = 1
	}
	occasionalBound := mostUsedBound + int(float64(positive)*0.5)
	for i := range items {
		switch {
		case i < mostUsedBound:
			items[i].Rank = RankMostUsed
		case i < occasionalBound:
			items[i].Rank = RankOccasional
		default:
			items[i].Rank = RankRarelyUsed
		}
	}

	return NodeRank{LastUpdated: time.Now().Unix(), Result: items}
}
