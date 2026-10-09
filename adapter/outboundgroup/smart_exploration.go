package outboundgroup

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// Business exploration assigns one real client connection to a challenger;
// it creates no independent probe request. This file owns timing, candidates,
// risk gates, reservations, and exploration outcome policy. Routing owns dials
// and shared feedback collection; RouteTable owns observations and priors.

const (
	// Explicit exploration is suppressed only when aggregate TTFB evidence says
	// the challenger is both materially harmful and unable to beat the current
	// best even at one standard deviation below its mean.
	smartExploreMinGainMs = 25.0
	// Promotion has a separate threshold from exploration risk pruning.
	smartPromotionMinGainMs     = 35.0
	smartExploreMinDamageBudget = 250.0
	smartExploreDamageFraction  = 0.35
	// rediscoverEvery is the mature-route cadence in business connections.
	rediscoverEvery     = 25
	initialExploreEvery = 5
	fastExploreEvery    = 2
)

// explorationRouteKey scopes scheduling to one route and target domain.
type explorationRouteKey struct{ routeKey, domain string }

// Scheduling state only; observations and priors remain in RouteTable.
type businessExplorationState struct {
	calls, coldTrials int
	tried             map[string]bool
	inflight          map[string]int
	last              time.Time
}

// explorationDecision carries a reserved choice; routing owns the actual dial.
type explorationDecision struct {
	state                          *businessExplorationState
	initial, challenger            C.Proxy
	incumbent, capturedBest, phase string
	call, trials                   int
	roll                           bool
}

func explorationURLTestDelay(p C.Proxy, url string) uint16 {
	d := p.LastDelayForTestUrl(url)
	if d == 0 {
		return 65535
	}
	return d
}

// Caller holds explorationMu. Both the URL-test cost tier and original prior veto
// are mandatory. Mature rolls may re-evaluate measured nodes once no new
// candidate remains; cold-start opportunities always use distinct new nodes.
func (s *Smart) selectBusinessChallenger(st *businessExplorationState, key, domain, best string, ps []C.Proxy, cold bool) C.Proxy {
	if len(ps) == 0 {
		return nil
	}
	ordered := append([]C.Proxy(nil), ps...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return explorationURLTestDelay(ordered[i], s.testUrl) < explorationURLTestDelay(ordered[j], s.testUrl)
	})
	minimum := uint32(explorationURLTestDelay(ordered[0], s.testUrl))
	tested := []C.Proxy{}
	for _, p := range ordered {
		name := p.Name()
		costOK := uint32(explorationURLTestDelay(p, s.testUrl)) <= 2*minimum
		if name == best || st.inflight[name] > 0 || !p.AliveForTestUrl(s.testUrl) || s.routeTable.ProxyFailedCount(key, domain, name) > 0 {
			continue
		}
		priorOK := s.explorationWorthRisk(key, domain, best, name)
		log.Debugln("[SmartExplore] candidate target=%s call=%d proxy=%s delay=%d minimum=%d cost_ok=%t prior_ok=%t", domain, st.calls, name, explorationURLTestDelay(p, s.testUrl), minimum, costOK, priorOK)
		if !costOK || !priorOK {
			continue
		}
		if !st.tried[name] && !s.routeTable.ProxyHasTTFBSample(key, domain, name) {
			return p
		}
		if !cold && s.routeTable.ProxyHasTTFBSample(key, domain, name) {
			tested = append(tested, p)
		}
	}
	if !cold {
		if ranked := s.rankCandidates(key, domain, tested, nil); len(ranked) > 0 {
			return ranked[0]
		}
	}
	return nil
}

// explorationWorthRisk compares optimistic discovery gain with expected
// request damage. Missing evidence remains explorable. A challenger is pruned
// only when its expected TTFB exceeds a conservative damage budget and its
// one-sigma optimistic estimate still offers no meaningful gain.
func (s *Smart) explorationWorthRisk(key, domain, bestName, challengerName string) bool {
	best, bestOK := s.routeTable.ProxyTTFBPrior(key, domain, bestName)
	// Unrelated destinations are not evidence against a proxy's first trial on
	// this route family. Use route-family / ASN evidence when it exists.
	challenger, challengerOK := s.routeTable.SimilarTTFBPrior(key, domain, challengerName)
	if !bestOK || !challengerOK {
		return true
	}
	optimisticChallenger := challenger.Mean - challenger.StdDev
	potentialGain := best.Mean - optimisticChallenger
	if potentialGain < 0 {
		potentialGain = 0
	}
	// Related-domain failures discount exploration's benefit, not the damage
	// budget. Low-damage and missing-TTFB candidates keep their existing paths.
	sharedFailedCount := s.routeTable.SimilarFailedCount(key, domain, challengerName)
	potentialGain *= math.Pow(0.8, sharedFailedCount)
	potentialDamage := challenger.Mean - best.Mean
	if potentialDamage < 0 {
		potentialDamage = 0
	}
	damageBudget := best.Mean * smartExploreDamageFraction
	if damageBudget < smartExploreMinDamageBudget {
		damageBudget = smartExploreMinDamageBudget
	}
	futureGain := potentialGain * s.routeTable.ExpectedFutureRequests(domain, 30*time.Second)
	riskBudget := potentialDamage
	if risk, ok := s.routeTable.ExplorationRisk(domain, challengerName); ok {
		riskBudget = math.Max(riskBudget, math.Max(0, risk.Mean+1.282*risk.StdDev))
	}
	allowed := potentialDamage <= damageBudget || potentialGain >= smartExploreMinGainMs && futureGain >= riskBudget
	if !allowed {
		log.Debugln("[Smart] skip challenger %s for %s: prior mean=%.0fms stddev=%.0fms best=%.0fms gain=%.0fms damage=%.0fms budget=%.0fms samples=%d shared_failed=%.2f",
			challengerName, domain, challenger.Mean, challenger.StdDev, best.Mean, potentialGain, potentialDamage, damageBudget, challenger.Samples, sharedFailedCount)
	}
	return allowed
}

// planBusinessExploration keeps selection and reservations in one critical section.
func (s *Smart) planBusinessExploration(key, domain string, healthy []C.Proxy) explorationDecision {
	s.explorationMu.Lock()
	if s.explorationRoutes == nil {
		s.explorationRoutes = map[explorationRouteKey]*businessExplorationState{}
	}
	id := explorationRouteKey{key, domain}
	st := s.explorationRoutes[id]
	if st == nil {
		if len(s.explorationRoutes) >= 1024 {
			var oldest explorationRouteKey
			var stamp time.Time
			for k, v := range s.explorationRoutes {
				if stamp.IsZero() || v.last.Before(stamp) {
					oldest, stamp = k, v.last
				}
			}
			delete(s.explorationRoutes, oldest)
		}
		st = &businessExplorationState{tried: map[string]bool{}, inflight: map[string]int{}}
		s.explorationRoutes[id] = st
	}
	st.calls++
	st.last = time.Now()
	call := st.calls
	best, _ := s.routeTable.GetBestProxy(key, domain)
	capturedBest := best
	available := false
	for _, p := range healthy {
		if p.Name() == best {
			available = true
		}
	}
	if !available {
		best = ""
	}
	var challenger C.Proxy
	var initial C.Proxy
	cold := call >= 2 && st.coldTrials < 2 && (capturedBest == "" || best != "")
	if cold {
		challenger = s.selectBusinessChallenger(st, key, domain, best, healthy, true)
	}
	phase := "normal"
	roll := false
	if challenger != nil {
		phase = "cold"
		st.coldTrials++
	} else if best != "" {
		// Keep Beta's coverage- and demand-aware cadence after the initial budget.
		remaining := len(healthy)
		if remaining > 12 {
			remaining = 12
		}
		remaining -= s.routeTable.RouteTTFBProxyCount(key, domain)
		minimum := uint32(65535)
		for _, p := range healthy {
			if d := uint32(explorationURLTestDelay(p, s.testUrl)); d < minimum {
				minimum = d
			}
		}
		eligible := 0
		for _, p := range healthy {
			name := p.Name()
			if name != best && !st.tried[name] && st.inflight[name] == 0 && !s.routeTable.ProxyHasTTFBSample(key, domain, name) && s.routeTable.ProxyFailedCount(key, domain, name) == 0 && uint32(explorationURLTestDelay(p, s.testUrl)) <= 2*minimum && s.explorationWorthRisk(key, domain, best, name) {
				eligible++
			}
		}
		if remaining > eligible {
			remaining = eligible
		}
		every := uint64(rediscoverEvery)
		if remaining > 0 {
			every = initialExploreEvery
			if s.routeTable.ExpectedRouteFutureRequests(key, domain, 30*time.Second) >= float64(remaining*4) {
				every = fastExploreEvery
			}
		}
		roll = s.routeTable.ShouldExplore(key, domain, every)
		if roll {
			challenger = s.selectBusinessChallenger(st, key, domain, best, healthy, false)
			if challenger != nil {
				phase = "roll"
			}
		}
	}
	if challenger != nil {
		st.tried[challenger.Name()] = true
		st.inflight[challenger.Name()]++
	}
	if challenger == nil && best == "" {
		if ranked := s.rankCandidates(key, domain, healthy, nil); len(ranked) > 0 {
			initial = ranked[0]
			st.tried[initial.Name()] = true
			st.inflight[initial.Name()]++
		}
	}
	trials := st.coldTrials
	s.explorationMu.Unlock()
	return explorationDecision{state: st, initial: initial, challenger: challenger, incumbent: best, capturedBest: capturedBest, phase: phase, call: call, trials: trials, roll: roll}
}

// reserveBusinessNode makes normal connections visible to challenger selection.
func (s *Smart) reserveBusinessNode(key, domain, name string) func() {
	s.explorationMu.Lock()
	st := s.explorationRoutes[explorationRouteKey{key, domain}]
	if st != nil {
		st.tried[name] = true
		st.inflight[name]++
	}
	s.explorationMu.Unlock()
	return s.explorationRelease(st, name)
}

func (s *Smart) explorationRelease(st *businessExplorationState, name string) func() {
	return func() {
		s.explorationMu.Lock()
		if st != nil {
			st.inflight[name]--
		}
		s.explorationMu.Unlock()
	}
}

// Capture before updating the candidate's TTFB, matching the existing feedback order.
func (s *Smart) explorationBaseline(key, domain, incumbent, challenger string) (smart.TTFBPrior, bool) {
	if challenger != "" && incumbent != "" {
		return s.routeTable.ProxyTTFBPrior(key, domain, incumbent)
	}
	return smart.TTFBPrior{}, false
}

// Both explicit trials and stale-route rechecks use the same regret/promotion policy.
func (s *Smart) recordExplorationOutcome(key, domain, proxyName, incumbent, challenger string, ttfb int64, baseline smart.TTFBPrior, baselineOK bool) {
	if !baselineOK {
		return
	}
	regret := float64(ttfb) - baseline.Mean
	s.routeTable.UpdateExplorationRegret(key, domain, challenger, regret)
	if proxyName != incumbent && float64(ttfb)+smartPromotionMinGainMs < baseline.Mean &&
		s.routeTable.PromoteTCPBestIfCurrent(key, domain, incumbent, proxyName) {
		log.Infoln("[Smart] promoted challenger %s for %s after TTFB improved %.0fms -> %dms",
			proxyName, domain, baseline.Mean, ttfb)
	}
	candidate, _ := s.routeTable.ProxyTTFBPrior(key, domain, proxyName)
	log.Infoln("[Smart] exploration outcome key=%s target=%s challenger=%s incumbent=%s ttfb=%dms baseline=%.0fms candidate_ema=%.0fms regret=%.0fms",
		key, domain, challenger, incumbent, ttfb, baseline.Mean, candidate.Mean, regret)
}

// explorationReservationConn releases the in-flight node reservation on close.
// Both initial business dials and challenger trials hold these reservations.
type explorationReservationConn struct {
	C.Conn
	once    sync.Once
	release func()
	err     error
}

func (c *explorationReservationConn) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close(); c.release() })
	return c.err
}

// Preserve protocol metadata through the lifecycle-only wrapper.
func (c *explorationReservationConn) Upstream() any { return c.Conn }
