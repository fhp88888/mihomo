package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/tcpstats"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

const (
	smartBestProxyFreshness = 1 * time.Second
	smartTCPFallbackStagger = 200 * time.Millisecond
	// smartDefaultDialWindow is used until a candidate has a historical dial
	// sample. Once sampled, its head start is 2.0x the dial-latency EMA, clamped
	// so scheduler noise cannot trigger spurious fallback and an outlier cannot
	// delay recovery indefinitely.
	smartDefaultDialWindow = 400 * time.Millisecond
	smartMinDialWindow     = 20 * time.Millisecond
	smartMaxDialWindow     = 2 * time.Second
	// Explicit exploration is suppressed only when aggregate TTFB evidence says
	// the challenger is both materially harmful and unable to beat the current
	// best even at one standard deviation below its mean.
	smartExploreMinGainMs = 25.0
	// Promotion has a separate threshold from exploration risk pruning.
	smartPromotionMinGainMs     = 35.0
	smartExploreMinDamageBudget = 250.0
	smartExploreDamageFraction  = 0.35
	// smartEarlyDeathLatencyLimit: a connection that fails before its first
	// byte within this window is treated as a dead proxy, not a slow target.
	smartEarlyDeathLatencyLimit = 5 * time.Second
	// exploreBatch is the number of top-quality candidates dialed first in a
	// discovery wave; beyond it the top tier is shuffled so we don't always
	// dial the same proxy first.
	exploreBatch = 16
	// highLossThreshold is the pkg-loss EMA above which a proxy is deferred to
	// the end of discovery.
	highLossThreshold = 0.1
	// rediscoverEvery is the known-route cadence: every Nth request re-discovers.
	rediscoverEvery     = 25
	initialExploreEvery = 5
	fastExploreEvery    = 2
	smartBestTag        = "Best"
)

// routeKey returns the route table key for a connection's metadata:
// "ASN:<number> <org>" when ASN is available, otherwise "TARGET:<effective-target>".
func routeKey(metadata *C.Metadata) string {
	if dst := metadata.DstIPASN; dst != "0" && dst != "" && dst != "unknown" {
		return "ASN:" + dst
	}

	target := smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
	if metadata.SmartTarget == "" {
		metadata.SmartTarget = target
	}
	return "TARGET:" + target
}

func routeDomain(metadata *C.Metadata) string {
	if metadata.Host == "" {
		return metadata.DstIP.String()
	}
	// Keep the full hostname as the local decision key. Related hosts still
	// share weighted evidence through RouteFamily and SimilarTTFBPrior.
	return strings.ToLower(strings.TrimSuffix(metadata.Host, "."))
}

// tcpRoute implements the TCP routing strategy using the route table and probe coordinator.
func (s *Smart) tcpRoute(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	key := routeKey(metadata)
	domain := routeDomain(metadata)
	proxies := s.GetProxies(true)

	// If manually selected, use that proxy directly
	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				dialCtx, dialCancel := context.WithTimeout(ctx, C.DefaultTCPTimeout)
				defer dialCancel()
				return s.dialAndWrap(dialCtx, p, metadata, key, domain)
			}
		}
		return nil, fmt.Errorf("selected proxy %q not found", s.selected)
	}

	proxies = s.responseCandidates(metadata, proxies)
	if len(proxies) == 0 {
		return nil, errors.New("no accessible proxies for target")
	}

	// A known route exploits its current best on most requests. Every
	// rediscoverEvery-th request explicitly explores a single challenger first,
	// giving previously untested nodes a chance to collect a TTFB sample.
	if s.routeTable.IsTCPProbed(key, domain) {
		exploreEvery := uint64(rediscoverEvery)
		covered := s.routeTable.RouteTTFBProxyCount(key, domain)
		// Detailed per-domain observations are intentionally capped. Initial
		// coverage is complete once that many distinct proxies have evidence;
		// comparing against the full provider forever would keep large groups in
		// aggressive exploration after the cache has reached its designed size.
		targetCoverage := len(proxies)
		if targetCoverage > smart.MaxProxyCellsPerDomain {
			targetCoverage = smart.MaxProxyCellsPerDomain
		}
		remaining := targetCoverage - covered
		if remaining > 0 {
			bestName, _ := s.routeTable.GetBestProxy(key, domain)
			eligibleUnknown := 0
			for _, proxy := range proxies {
				if !proxy.AliveForTestUrl(s.testUrl) || s.routeTable.ProxyHasTTFBSample(key, domain, proxy.Name()) {
					continue
				}
				if bestName != "" && !s.explorationWorthRisk(key, domain, bestName, proxy.Name()) {
					continue
				}
				eligibleUnknown++
			}
			if eligibleUnknown < remaining {
				remaining = eligibleUnknown
			}
		}
		if remaining > 0 {
			exploreEvery = initialExploreEvery
			if s.routeTable.ExpectedRouteFutureRequests(key, domain, 30*time.Second) >= float64(remaining*4) {
				exploreEvery = fastExploreEvery
			}
		}
		shouldExplore := s.routeTable.ShouldExplore(key, domain, exploreEvery)
		bestName, _ := s.routeTable.GetBestProxy(key, domain)
		log.Debugln("[SmartTrace] decision key=%s target=%s best=%s coverage=%d/%d eligible_remaining=%d cadence=%d explore=%t",
			key, domain, bestName,
			covered, targetCoverage, remaining, exploreEvery, shouldExplore)
		if shouldExplore {
			conn, err := s.exploreTcpConn(ctx, metadata, key, domain, proxies)
			if conn != nil || err != nil {
				return conn, err
			}
		} else {
			conn, err := s.serialTcpConn(ctx, metadata, key, domain, proxies)
			if conn != nil || err != nil {
				return conn, err
			}
		}
		log.Debugln("[Smart] route key=%s known proxies all failed, running full discovery", key)
	}

	// Fallback, Cold start, 4% re-discover, or all serial fallbacks exhausted:
	// full parallel discovery.
	return s.discoverAndRoute(ctx, metadata, key, domain, proxies)
}

func (s *Smart) serialTcpConn(ctx context.Context, metadata *C.Metadata, key, domain string, proxies []C.Proxy) (C.Conn, error) {
	if bestName, ok := s.routeTable.GetBestProxyIfFresh(key, domain, smartBestProxyFreshness); ok {
		for _, p := range proxies {
			if p.Name() == bestName && p.AliveForTestUrl(s.testUrl) {
				// Part 1 dials only the current best.  If it has not connected
				// within its adaptive dial window, part 2 starts the remaining
				// candidates as a staggered fallback race.  The best remains in
				// flight and can still win after the fallback has started.
				ordered := s.rankCandidates(key, domain, proxies, p)
				firstWindow := s.adaptiveDialWindow(key, domain, p.Name())
				raceCtx, finishRace, trackErr := s.probeCoordinator.TrackRace(ctx)
				if trackErr != nil {
					return nil, trackErr
				}
				var drainWG sync.WaitGroup
				conn, err := s.raceAndWrap(raceCtx, metadata, key, domain, ordered,
					firstWindow, &drainWG, bestName)
				// raceAndWrap registers any asynchronous loser drain before it
				// returns. Keep the coordinator registration alive until that local
				// drain settles, then release its context and WaitGroup slot.
				go func() {
					drainWG.Wait()
					finishRace()
				}()
				if conn != nil || err != nil {
					return conn, err
				}
				return nil, nil
			}
		}
	}

	// The best is stale, unavailable, or failed. Re-rank all healthy proxies so
	// score changes are applied on a time bound independently of the explicit
	// rediscovery cadence. A stale rerank is still only a dial-time hypothesis:
	// retain the incumbent until the served request's TTFB proves an improvement.
	ordered := s.rankCandidates(key, domain, proxies, nil)
	if len(ordered) == 0 {
		return nil, nil
	}
	firstWindow := s.adaptiveDialWindow(key, domain, ordered[0].Name())
	incumbent, _ := s.routeTable.GetBestProxy(key, domain)
	log.Debugln("[SmartTrace] stale-rerank key=%s target=%s best=%s first=%s", key, domain, incumbent, ordered[0].Name())
	incumbentAlive := false
	for _, proxy := range proxies {
		if proxy.Name() == incumbent && proxy.AliveForTestUrl(s.testUrl) {
			incumbentAlive = true
			break
		}
	}
	if !incumbentAlive {
		incumbent = ""
	}
	conn, err := s.raceAndWrapExploration(ctx, metadata, key, domain, ordered, firstWindow, nil,
		incumbent, true)
	if conn != nil || err != nil {
		return conn, err
	}
	return nil, nil
}

// adaptiveDialWindow returns 2.0x the candidate's historical dial-latency
// EMA. A default covers unsampled candidates; bounds keep fallback useful in
// both very-low-latency tests and pathological histories.
func (s *Smart) adaptiveDialWindow(key, domain, proxy string) time.Duration {
	latency, ok := s.routeTable.ProxyDialLatency(key, domain, proxy)
	if !ok || latency <= 0 {
		return smartDefaultDialWindow
	}
	window := time.Duration(latency*2) * time.Millisecond
	if window < smartMinDialWindow {
		return smartMinDialWindow
	}
	if window > smartMaxDialWindow {
		return smartMaxDialWindow
	}
	return window
}

// challengerSequence orders healthy non-best proxies for explicit
// exploration. Only a TTFB sample for this route key marks a proxy as tested;
// sibling observations can prune risky candidates but never replace a local
// trial. Untested proxies come first by dial latency, followed by tested
// proxies in score order.
func (s *Smart) challengerSequence(key, domain string, proxies []C.Proxy, bestName string) []C.Proxy {
	untested := make([]C.Proxy, 0, len(proxies))
	tested := make([]C.Proxy, 0, len(proxies))
	for _, p := range proxies {
		if p.Name() == bestName || !p.AliveForTestUrl(s.testUrl) {
			continue
		}
		if bestName != "" && !s.explorationWorthRisk(key, domain, bestName, p.Name()) {
			continue
		}
		hasExactSample := s.routeTable.ProxyHasTTFBSample(key, domain, p.Name())
		if hasExactSample {
			tested = append(tested, p)
		} else {
			untested = append(untested, p)
		}
	}
	dialLatency := func(p C.Proxy) int64 {
		if latency, ok := s.routeTable.ProxyDialLatency(key, domain, p.Name()); ok {
			return latency
		}
		latency := p.LastDelayForTestUrl(s.testUrl)
		if latency == 0 || latency == 0xffff {
			return int64(^uint16(0))
		}
		return int64(latency)
	}
	sort.SliceStable(untested, func(i, j int) bool {
		iLatency, jLatency := dialLatency(untested[i]), dialLatency(untested[j])
		if iLatency != jLatency {
			return iLatency < jLatency
		}
		return untested[i].Name() < untested[j].Name()
	})
	tested = s.rankCandidates(key, domain, tested, nil)
	return append(untested, tested...)
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

// explorationCallSequence builds one exploration race. With a known best it
// prepends exactly one challenger to the normal best-first sequence. Without
// a best (cold start), up to topK challengers form the discovery sequence.
func (s *Smart) explorationCallSequence(key, domain string, proxies []C.Proxy) ([]C.Proxy, string) {
	bestName, hasBest := s.routeTable.GetBestProxy(key, domain)
	var best C.Proxy
	if hasBest {
		for _, p := range proxies {
			if p.Name() == bestName && p.AliveForTestUrl(s.testUrl) {
				best = p
				break
			}
		}
	}
	if best == nil {
		challengers := s.challengerSequence(key, domain, proxies, "")
		if len(challengers) > topK {
			challengers = challengers[:topK]
		}
		return challengers, ""
	}

	bestSequence := s.rankCandidates(key, domain, proxies, best)
	challengers := s.challengerSequence(key, domain, proxies, bestName)
	if len(challengers) == 0 {
		return bestSequence, bestName
	}
	challenger := challengers[0]
	log.Debugln("[SmartTrace] challenger key=%s target=%s best=%s chosen=%s eligible=%d exact_sample=%t",
		key, domain, bestName, challenger.Name(), len(challengers), s.routeTable.ProxyHasTTFBSample(key, domain, challenger.Name()))
	callSequence := make([]C.Proxy, 0, len(bestSequence)+1)
	callSequence = append(callSequence, challenger)
	for _, p := range bestSequence {
		if p.Name() != challenger.Name() {
			callSequence = append(callSequence, p)
		}
	}
	return callSequence, bestName
}

func (s *Smart) exploreTcpConn(ctx context.Context, metadata *C.Metadata, key, domain string, proxies []C.Proxy) (C.Conn, error) {
	ordered, bestName := s.explorationCallSequence(key, domain, proxies)
	if len(ordered) == 0 {
		return nil, nil
	}
	raceCtx, finishRace, trackErr := s.probeCoordinator.TrackRace(ctx)
	if trackErr != nil {
		return nil, trackErr
	}
	var drainWG sync.WaitGroup
	firstWindow := s.adaptiveDialWindow(key, domain, ordered[0].Name())
	conn, err := s.raceAndWrapExploration(raceCtx, metadata, key, domain, ordered, firstWindow, &drainWG, bestName, false)
	go func() {
		drainWG.Wait()
		finishRace()
	}()
	return conn, err
}

// rankCandidates filters proxies to alive ones (excluding best when given),
// refreshes their scores, and returns the score-ranked order with best (if any)
// anchored first.
func (s *Smart) rankCandidates(key, domain string, proxies []C.Proxy, best C.Proxy) []C.Proxy {
	over := make([]string, 0, len(proxies))
	proxyMap := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		if best != nil && p.Name() == best.Name() {
			continue
		}
		if p.AliveForTestUrl(s.testUrl) {
			over = append(over, p.Name())
			proxyMap[p.Name()] = p
		}
	}
	if best != nil {
		proxyMap[best.Name()] = best
	}

	// Refresh with best included so its score stays current for the race, but
	// rank only the non-best proxies — best is already anchored at the front.
	refresh := over
	if best != nil {
		refresh = append(append([]string{}, over...), best.Name())
	}
	s.routeTable.RefreshScores(key, domain, refresh)
	ranked := s.routeTable.RankByScore(over, func(proxyName string) uint16 {
		if p, ok := proxyMap[proxyName]; ok {
			return p.LastDelayForTestUrl(s.testUrl)
		}
		return 0xffff
	}, key, domain)

	if best != nil {
		return append([]C.Proxy{best}, orderByNamesFrom(ranked, proxyMap)...)
	}
	return orderByNamesFrom(ranked, proxyMap)
}

// raceAndWrap runs a staggered race over ordered and wraps the winner.
// firstStagger is the gap before the 2nd candidate (ordered[0] is dialed
// immediately); later candidates follow at smartTCPFallbackStagger.  wg != nil
// makes loser draining asynchronous so the caller returns the winner
// immediately while late successful losers still sample latency in the
// background.
func (s *Smart) raceAndWrap(ctx context.Context, metadata *C.Metadata, key, domain string,
	ordered []C.Proxy, firstStagger time.Duration, wg *sync.WaitGroup, bestName string) (C.Conn, error) {
	return s.raceAndWrapPolicy(ctx, metadata, key, domain, ordered, firstStagger, wg, bestName, "", false)
}

func (s *Smart) raceAndWrapExploration(ctx context.Context, metadata *C.Metadata, key, domain string,
	ordered []C.Proxy, firstStagger time.Duration, wg *sync.WaitGroup, bestName string, refreshAfterSample bool) (C.Conn, error) {
	challenger := ""
	if len(ordered) > 0 && bestName != "" && ordered[0].Name() != bestName {
		challenger = ordered[0].Name()
	}
	return s.raceAndWrapPolicy(ctx, metadata, key, domain, ordered, firstStagger, wg, bestName, challenger, refreshAfterSample)
}

func (s *Smart) raceAndWrapPolicy(ctx context.Context, metadata *C.Metadata, key, domain string,
	ordered []C.Proxy, firstStagger time.Duration, wg *sync.WaitGroup, bestName, challenger string, refreshAfterSample bool) (C.Conn, error) {
	winner, conn, connectTime, err := raceStaggered(ctx, ordered, wg, firstStagger,
		// Race dials go through dialTCP, which records a genuine dial failure
		// as MarkFailed.
		func(dialCtx context.Context, proxy C.Proxy) (C.Conn, int64, error) {
			return s.dialTCP(dialCtx, proxy, metadata, key)
		},
		// onConnect: successful dials (winner + late successful losers) each
		// contribute a latency sample.
		func(proxyName string, connectTime int64) {
			s.routeTable.UpdateLatency(key, domain, proxyName, connectTime)
		},
		// onFail: dialTCP already handles MarkFailed for genuine failures, so
		// the race has nothing to add for failures.
		nil,
		// onWinner: normal races promote their dial winner. During explicit
		// exploration, retain the incumbent until first-byte evidence proves
		// that the challenger improved the end-to-end request.
		func(proxy C.Proxy, connectTime int64) {
			tag := staggerTag(ordered, proxy.Name(), bestName)
			log.Infoln("[Smart] route key=%s target=%s routed via %s (%dms, %s)", key, domain, proxy.Name(), connectTime, tag)
			s.routeTable.IncrementUseCount(key, domain, proxy.Name())
			if challenger != "" && bestName != "" {
				s.routeTable.SetTCPProbedPreserveEvaluation(key, domain)
			} else if bestName != "" && proxy.Name() == bestName {
				s.routeTable.SetTCPProbedPreserveEvaluation(key, domain)
			} else if bestName != "" {
				s.routeTable.PromoteTCPBestAfterFallback(key, domain, bestName, proxy.Name())
			} else {
				s.routeTable.SetBestProxyAndTCPProbed(key, domain, proxy.Name())
			}
		},
	)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, nil
	}
	return s.wrapTCPConnWithExploration(conn, winner, metadata, connectTime, bestName, challenger, refreshAfterSample), nil
}

// staggerTag identifies the winner by its position in this race. When a best
// proxy is present it is named Best and excluded from the stagger numbering;
// otherwise numbering starts at the first candidate.
func staggerTag(ordered []C.Proxy, winnerName, bestName string) string {
	if bestName != "" && winnerName == bestName {
		return smartBestTag
	}
	ordinal := 0
	for _, proxy := range ordered {
		if bestName != "" && proxy.Name() == bestName {
			continue
		}
		ordinal++
		if proxy.Name() == winnerName {
			return fmt.Sprintf("Stagger#%d", ordinal)
		}
	}
	return "Stagger#unknown"
}

func raceStaggered(ctx context.Context, ordered []C.Proxy, wg *sync.WaitGroup,
	firstStagger time.Duration,
	dial func(context.Context, C.Proxy) (C.Conn, int64, error),
	onConnect func(proxyName string, connectTime int64),
	onFail func(proxy C.Proxy, err error),
	onWinner func(proxy C.Proxy, connectTime int64),
) (C.Proxy, C.Conn, int64, error) {
	if len(ordered) == 0 {
		return nil, nil, 0, nil
	}

	raceCtx, cancelRace := context.WithCancel(ctx)

	// keepLosersAlive lets an asynchronous discovery/exploration race finish
	// in-flight loser dials so their connectTime is sampled (see stopAndDrain).
	keepLosersAlive := false
	defer func() {
		if !keepLosersAlive {
			cancelRace()
		}
	}()

	results := make(chan dialResult, len(ordered))
	var workers sync.WaitGroup
	launched, received := 0, 0
	// firstFailed: the first candidate (the best during exploitation or a
	// challenger during exploration) failed inside its head-start window —
	// collapse it and dial the next candidate immediately.
	firstFailed := false

	launch := func(proxy C.Proxy) {
		launched++
		workers.Add(1)
		go func() {
			defer workers.Done()
			dialCtx, dialCancel := context.WithTimeout(raceCtx, C.DefaultTCPTimeout)
			conn, connectTime, err := dial(dialCtx, proxy)
			dialCancel()
			results <- dialResult{proxy: proxy, conn: conn, connectTime: connectTime, err: err}
		}()
	}

	// drain consumes n results, closing successful connections and feeding
	// onConnect.  It runs from the select-loop goroutine (sync drain) or the
	// stopAndDrain background goroutine (async discovery drain).
	drain := func(n int) {
		for i := 0; i < n; i++ {
			result := <-results
			if result.err == nil && result.conn != nil {
				onConnect(result.proxy.Name(), result.connectTime)
				result.conn.Close()
			}
		}
	}

	// stopAndDrain cancels in-flight dials and clears the remaining results.
	// With wg nil it does so synchronously (deterministic, fallback).  With wg
	// set it hands the drain to a background goroutine so the caller can return
	// the winner immediately. On that path the goroutine
	// skips cancelRace until all losers finish, so late successful losers still
	// contribute a latency sample via onConnect before their conn is closed.
	stopAndDrain := func() {
		remaining := launched - received
		if wg == nil {
			cancelRace()
			drain(remaining)
			workers.Wait()
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Cancel raceCtx when the drain settles — even if drain or
			// workers.Wait panics, the deferred cancelRace fires so in-flight
			// dial contexts are never leaked.
			defer cancelRace()
			drain(remaining)
			workers.Wait()
		}()
	}

	launch(ordered[0])
	next := 1
	var timer *time.Timer
	var timerC <-chan time.Time
	if next < len(ordered) {
		timer = time.NewTimer(firstStagger)
		timerC = timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for received < launched || next < len(ordered) {
		select {
		case result := <-results:
			received++
			if result.err == nil && result.conn == nil {
				result.err = errors.New("proxy dial returned nil connection without error")
			}
			if result.err == nil {
				onConnect(result.proxy.Name(), result.connectTime)
				if onWinner != nil {
					onWinner(result.proxy, result.connectTime)
				}
				// Winner found: on the async discovery path let the in-flight
				// losers finish dialing so their connectTime is sampled, then
				// hand them to the background drain goroutine.  The deferred
				// cancelRace is skipped (keepLosersAlive) and the goroutine
				// cancels after all losers settle.
				if wg != nil {
					keepLosersAlive = true
				}
				stopAndDrain()
				return result.proxy, result.conn, result.connectTime, nil
			}
			if onFail != nil {
				onFail(result.proxy, result.err)
			}
			if tunnel.ShouldStopRetry(result.err) {
				stopAndDrain()
				return nil, nil, 0, result.err
			}
			// Collapse the head-start if the first candidate failed early, so
			// fallback starts immediately instead of waiting for the full
			// adaptive window.
			if !firstFailed && result.proxy == ordered[0] && next < len(ordered) {
				firstFailed = true
				if timer != nil {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
				}
				timerC = nil
				launch(ordered[next])
				next++
				if next < len(ordered) {
					timer = time.NewTimer(smartTCPFallbackStagger)
					timerC = timer.C
				}
			}
		case <-timerC:
			launch(ordered[next])
			next++
			if next < len(ordered) {
				timer.Reset(smartTCPFallbackStagger)
			} else {
				timerC = nil
			}
		case <-ctx.Done():
			stopAndDrain()
			return nil, nil, 0, ctx.Err()
		}
	}

	return nil, nil, 0, nil
}

// dialTCP dials a known proxy and records a genuine dial failure.
func (s *Smart) dialTCP(ctx context.Context, proxy C.Proxy, metadata *C.Metadata, key string) (C.Conn, int64, error) {
	dialCtx, timer := dialer.WithConnTimer(ctx)
	start := time.Now()
	conn, err := proxy.DialContext(dialCtx, metadata)
	connectTime := time.Since(start).Milliseconds()
	if connectTime < 1 {
		connectTime = 1
	}

	if err == nil && conn == nil {
		err = errors.New("proxy dial returned nil connection without error")
	}
	if err != nil {
		log.Debugln("[Smart] route key=%s dial %s failed after %dms: %v",
			key, proxy.Name(), connectTime, err)
		if !tunnel.ShouldStopRetry(err) && !errors.Is(err, context.Canceled) {
			s.routeTable.MarkFailed(key, proxy.Name(), routeDomain(metadata), 1.0)
		}
		return nil, connectTime, err
	}

	// Carry the raw TCP-connect-to-proxy-server duration on the connection so
	// wrapTCPConn can log it alongside latency and TTFB.
	if conn != nil {
		conn = &tcpTimingConn{Conn: conn, tcpConnectTime: timer.Duration()}
	}

	return conn, connectTime, nil
}

// dialAndWrap dials a known proxy and wraps the connection for metrics collection.
func (s *Smart) dialAndWrap(ctx context.Context, proxy C.Proxy, metadata *C.Metadata, key, domain string) (C.Conn, error) {
	conn, connectTime, err := s.dialTCP(ctx, proxy, metadata, key)
	if err != nil {
		return nil, err
	}

	// Write connectTime to the route table so that all paths (fast-path,
	// serial fallback, discovery) contribute the same metric. TTFB varies
	// by connection lifetime and is not available for losers.
	s.routeTable.UpdateLatency(key, domain, proxy.Name(), connectTime)
	s.routeTable.IncrementUseCount(key, domain, proxy.Name())
	s.routeTable.SetBestProxyAndTCPProbed(key, domain, proxy.Name())

	return s.wrapTCPConn(conn, proxy, metadata, connectTime), nil
}

// discoverAndRoute performs pre-rank + concurrent discovery for a new or failed route.
func (s *Smart) discoverAndRoute(ctx context.Context, metadata *C.Metadata, key, domain string, proxies []C.Proxy) (C.Conn, error) {
	// Filter proxies: must be alive
	available := make([]C.Proxy, 0, len(proxies))
	for _, p := range proxies {
		if p.AliveForTestUrl(s.testUrl) {
			available = append(available, p)
		}
	}

	if len(available) == 0 {
		if err := s.waitForHealthRecovery(ctx); err != nil {
			return nil, err
		}
		for _, p := range s.responseCandidates(metadata, s.GetProxies(true)) {
			if p.AliveForTestUrl(s.testUrl) {
				available = append(available, p)
			}
		}
		if len(available) == 0 {
			return nil, errors.New("no alive proxies available")
		}
	}

	// With no usable best sequence, build a cold-start challenger sequence:
	// healthy proxies without TTFB samples come first, ordered by historical
	// dial latency or health-check latency, and discovery is capped at topK.
	ordered, _ := s.explorationCallSequence(key, domain, available)

	// Concurrent discovery through probe coordinator
	proxy, conn, connectTime, err := s.probeCoordinator.Discover(
		ctx, key, available, metadata, namesOf(ordered),
		func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
			dialCtx, timer := dialer.WithConnTimer(ctx)
			dialCtx, dialCancel := context.WithTimeout(dialCtx, C.DefaultTCPTimeout)
			conn, err := p.DialContext(dialCtx, m)
			dialCancel()
			elapsed := time.Since(start).Milliseconds()
			if err == nil && conn == nil {
				err = errors.New("proxy dial returned nil connection without error")
			}
			if conn != nil && err == nil {
				conn = &tcpTimingConn{Conn: conn, tcpConnectTime: timer.Duration()}
			}
			return conn, elapsed, err
		},
		s.routeTable,
	)

	if err != nil {
		log.Infoln("[Smart] route key=%s discovery failed: %v", key, err)
		return nil, err
	}

	// Discover already recorded latency for the leader or follower connection.
	// Do not write it again here, which would double-count the sample.
	s.routeTable.IncrementUseCount(key, domain, proxy.Name())
	s.routeTable.SetBestProxyAndTCPProbed(key, domain, proxy.Name())

	return s.wrapTCPConn(conn, proxy, metadata, connectTime), nil
}

func (s *Smart) exploreOrder(available []C.Proxy, proxies []C.Proxy, key, domain string) []C.Proxy {
	attrs := s.routeTable.ProxyAttrsSnapshot()
	if len(attrs) == 0 {
		names := namesOf(available)
		preRanked := s.routeTable.PreRankLatency(names, s.lastDelayOf(proxies), key, domain)
		return orderByNames(available, preRanked)
	}

	// Neutral score: median of the sampled proxies' Scores.
	var sampled []float64
	for _, a := range attrs {
		sampled = append(sampled, a.Score)
	}
	median := median(sampled)

	type cand struct {
		proxy    C.Proxy
		score    float64
		deferred bool
	}
	cands := make([]cand, 0, len(available))
	for _, p := range available {
		a, ok := attrs[p.Name()]
		score := median
		if ok {
			score = a.Score
		}
		deferred := ok && (a.FailedCount > 0 || a.PkgLoss > highLossThreshold)
		// A proxy that has only failed for this key has no sample, so it is
		// absent from the UseCount-weighted attrs aggregation — defer it from
		// the row's own FailedCount so it isn't re-dialed in the top tier.
		if !deferred && s.routeTable.ProxyFailedCount(key, domain, p.Name()) > 0 {
			deferred = true
		}
		cands = append(cands, cand{
			proxy:    p,
			score:    score,
			deferred: deferred,
		})
	}

	// Sort: deferred last; otherwise Score descending; ties by name.
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].deferred != cands[j].deferred {
			return !cands[i].deferred
		}
		if cands[i].score != cands[j].score {
			return cands[i].score > cands[j].score
		}
		return cands[i].proxy.Name() < cands[j].proxy.Name()
	})

	// Shuffle the best exploreBatch non-deferred candidates so the dial order
	// within the top quality tier is randomized (exploration).  This only kicks
	// in when the pool is bigger than exploreBatch — a small pool keeps its
	// deterministic quality order.  Deferred proxies stay at the end — they are
	// the last-resort tier and must not be pulled forward by the shuffle.
	nonDeferred := 0
	for i := range cands {
		if !cands[i].deferred {
			nonDeferred++
		}
	}
	if nonDeferred > exploreBatch {
		rand.Shuffle(exploreBatch, func(i, j int) {
			cands[i], cands[j] = cands[j], cands[i]
		})
	}

	out := make([]C.Proxy, 0, len(cands))
	for i := range cands {
		out = append(out, cands[i].proxy)
	}
	return out
}

func namesOf(ps []C.Proxy) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Name()
	}
	return out
}

func orderByNames(ps []C.Proxy, names []string) []C.Proxy {
	byName := make(map[string]C.Proxy, len(ps))
	for _, p := range ps {
		byName[p.Name()] = p
	}
	out := make([]C.Proxy, 0, len(names))
	for _, name := range names {
		if p, ok := byName[name]; ok {
			out = append(out, p)
		}
	}
	return out
}

// lastDelayOf returns a health-check latency lookup over proxies, returning
// 0xffff for names not in the set.
func (s *Smart) lastDelayOf(proxies []C.Proxy) func(string) uint16 {
	byName := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		byName[p.Name()] = p
	}
	return func(name string) uint16 {
		if p, ok := byName[name]; ok {
			return p.LastDelayForTestUrl(s.testUrl)
		}
		return 0xffff
	}
}

// orderByNamesFrom reorders names using a prebuilt name→proxy map.
func orderByNamesFrom(names []string, byName map[string]C.Proxy) []C.Proxy {
	out := make([]C.Proxy, 0, len(names))
	for _, name := range names {
		if p, ok := byName[name]; ok {
			out = append(out, p)
		}
	}
	return out
}

func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := append([]float64(nil), vals...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

// tcpTimingConn carries the raw TCP-connect-to-proxy-server duration measured by
// the dialer on the returned connection, so wrapTCPConn can log it alongside the
// full dial latency and TTFB without threading it through the race plumbing.
type tcpTimingConn struct {
	C.Conn
	tcpConnectTime time.Duration
}

func (t *tcpTimingConn) TCPConnectTime() time.Duration { return t.tcpConnectTime }

// Upstream exposes the wrapped connection so common.Cast (and thus N.NeedHandshake)
// can still unwrap through this wrapper to the underlying conn.
func (t *tcpTimingConn) Upstream() any { return t.Conn }

// wrapTCPConn wraps a TCP connection with close-callbacks that collect pkg_loss
// and speed, and penalize RST / early-death failures.  Latency (dial connectTime)
// is recorded at dial time by the callers, not here.
func (s *Smart) wrapTCPConn(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64) C.Conn {
	return s.wrapTCPConnWithExploration(c, proxy, metadata, connectTime, "", "", false)
}

func (s *Smart) wrapTCPConnWithExploration(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64, incumbent, challenger string, refreshAfterSample bool) C.Conn {
	if s.exitWatch != nil && metadata.Host != "" && metadata.DstPort == 443 && metadata.Type != C.INNER && s.selected == "" {
		s.exitWatch.MaybeProbe(s.ctx, proxy)
	}
	key := routeKey(metadata)
	domain := routeDomain(metadata)

	// The raw TCP-connect-to-proxy-server duration is attached to the connection
	// by dialTCP / the discovery singleDial; surface it for the establishment log.
	var tcpConnectTime time.Duration
	if tc, ok := c.(interface{ TCPConnectTime() time.Duration }); ok {
		tcpConnectTime = tc.TCPConnectTime()
	}

	c.AppendToChains(s)

	sampled := s.sampleConnection()
	start := time.Now()
	var firstReadErr atomic.TypedValue[error]
	var firstReadLatency atomic.Int64

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err != nil {
				firstReadErr.Store(err)
			}
		})
	}

	c = callback.NewFirstReadCallBackConn(c, func(err error) {
		// TTFB is end-to-end from dial start: connectTime covers dial →
		// handshake-done, and time.Since(start) the remainder until the first
		// byte arrives from the target.
		ttfb := connectTime + time.Since(start).Milliseconds()
		if ttfb < 1 {
			ttfb = 1
		}
		firstReadLatency.Store(ttfb)
		if err != nil {
			firstReadErr.Store(err)
		}
		baseline, baselineOK := smart.TTFBPrior{}, false
		if challenger != "" && incumbent != "" {
			baseline, baselineOK = s.routeTable.ProxyTTFBPrior(key, domain, incumbent)
		}
		if sampled {
			s.routeTable.UpdateTTFB(key, domain, proxy.Name(), ttfb)
		}
		if err == nil && baselineOK {
			regret := float64(ttfb) - baseline.Mean
			s.routeTable.UpdateExplorationRegret(key, domain, challenger, regret)
			if proxy.Name() != incumbent && float64(ttfb)+smartPromotionMinGainMs < baseline.Mean &&
				s.routeTable.PromoteTCPBestIfCurrent(key, domain, incumbent, proxy.Name()) {
				log.Infoln("[Smart] promoted challenger %s for %s after TTFB improved %.0fms -> %dms",
					proxy.Name(), domain, baseline.Mean, ttfb)
			}
			candidate, _ := s.routeTable.ProxyTTFBPrior(key, domain, proxy.Name())
			log.Infoln("[Smart] exploration outcome key=%s target=%s challenger=%s incumbent=%s ttfb=%dms baseline=%.0fms candidate_ema=%.0fms regret=%.0fms",
				key, domain, challenger, incumbent, ttfb, baseline.Mean, candidate.Mean, regret)
		}
		if err == nil && refreshAfterSample {
			// A stale-route recheck has completed even if its candidate did not
			// beat the incumbent. Without this, every subsequent request remains
			// stale and repeats the same forced challenger trial.
			s.routeTable.SetTCPProbed(key, domain)
			log.Debugln("[SmartTrace] recheck-complete key=%s target=%s winner=%s incumbent=%s", key, domain, proxy.Name(), incumbent)
		}
		log.Infoln("[Smart] established key=%s target=%s proxy=%s latency=%dms tcp_connect=%dms ttfb=%dms",
			key, domain, proxy.Name(), connectTime, tcpConnectTime.Milliseconds(), ttfb)
	})

	return callback.NewCloseCallbackConn(c, func() {
		firstRead := firstReadLatency.Load()
		readErr := firstReadErr.Load()

		// Collect speed and pkg_loss from tracker
		tracker := statistic.DefaultManager.Get(metadata.UUID)
		if sampled && tracker != nil {
			info := tracker.Info()
			maxUpload := info.MaxUploadRate.Load()
			maxDownload := info.MaxDownloadRate.Load()
			speed := float64(maxUpload)
			if maxDownload > maxUpload {
				speed = float64(maxDownload)
			}
			if speed > 0 {
				s.routeTable.UpdateSpeed(key, domain, proxy.Name(), speed)
			}

			// Collect pkg_loss from TCP stats.
			// Always update when TCP stats are available — even 0% loss
			// drives the EMA back toward 0, preventing stale loss from
			// accumulating indefinitely.
			if trackerConn, ok := tracker.(net.Conn); ok {
				stats := tcpstats.GetTCPStats(trackerConn)
				if stats != nil {
					lossRate := stats.LossRate()
					s.routeTable.UpdatePkgLoss(key, domain, proxy.Name(), lossRate)
				}
			}
		}

		// check for TCP RST or early death and mark-failed if necessary
		s.checkResetByPeer(key, domain, proxy.Name(), readErr)
		s.checkEarlyDeath(key, domain, proxy.Name(), readErr, firstRead, tracker)

		if tracker != nil && smart.ResponseProbeEligible(metadata, float64(tracker.Info().DownloadTotal.Load())/1024/1024, false) {
			s.probeAfterClose(metadata, proxy)
		}
	})
}

// checkResetByPeer penalizes a proxy when a connection was aborted with a TCP RST
func (s *Smart) checkResetByPeer(key, domain, proxyName string, readErr error) {
	if readErr == nil || !errors.Is(readErr, syscall.ECONNRESET) {
		return
	}
	s.routeTable.MarkFailed(key, proxyName, domain, 0.4)
	log.Debugln("[Smart] RST mark-failed key=%s proxy=%s err=%v", key, proxyName, readErr)
}

// checkEarlyDeath penalizes a proxy when a connection failed before its first
// byte ever arrived and never completed a bidirectional flow.
func (s *Smart) checkEarlyDeath(key, domain, proxyName string, readErr error, firstReadLatencyMs int64, tracker statistic.Tracker) {
	if readErr == nil || readErr == io.EOF {
		return
	}
	// don't double-count RST
	if errors.Is(readErr, syscall.ECONNRESET) {
		return
	}
	if firstReadLatencyMs >= smartEarlyDeathLatencyLimit.Milliseconds() {
		return
	}
	if tracker != nil {
		info := tracker.Info()
		if info.DownloadTotal.Load() > 0 {
			return
		}
	}
	s.routeTable.MarkFailed(key, proxyName, domain, 0.8)
	log.Debugln("[Smart] EARLY-DEATH mark-failed key=%s proxy=%s firstReadLat=%dms err=%v",
		key, proxyName, firstReadLatencyMs, readErr)
}

// udpRoute implements the UDP routing strategy.
// It tries the current best proxy first, then falls through to pre-rank order.
func (s *Smart) udpRoute(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	if !s.SupportUDP() {
		return nil, errors.New("UDP not supported")
	}

	key := routeKey(metadata)
	domain := routeDomain(metadata)
	proxies := s.GetProxies(true)

	// If manually selected, use that proxy
	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected && p.SupportUDP() {
				dialCtx, dialCancel := context.WithTimeout(ctx, C.DefaultUDPTimeout)
				defer dialCancel()
				return s.dialUDPAndWrap(dialCtx, p, metadata, key, domain, true)
			}
		}
		return nil, errors.New("selected proxy not found or does not support UDP")
	}

	proxies = s.responseCandidates(metadata, proxies)
	if len(proxies) == 0 {
		return nil, errors.New("no accessible proxies for target")
	}

	// Filter to UDP-capable, alive proxies
	udpProxies := make([]C.Proxy, 0, len(proxies))
	for _, p := range proxies {
		if p.SupportUDP() && p.AliveForTestUrl(s.testUrl) {
			udpProxies = append(udpProxies, p)
		}
	}
	if len(udpProxies) == 0 {
		// TCP health recovery is group-wide. Unsupported UDP alone must not
		// trigger retries while the group has healthy nodes.
		if !s.hasHealthyProxy() {
			if err := s.waitForHealthRecovery(ctx); err != nil {
				return nil, err
			}
			for _, p := range s.responseCandidates(metadata, s.GetProxies(true)) {
				if p.SupportUDP() && p.AliveForTestUrl(s.testUrl) {
					udpProxies = append(udpProxies, p)
				}
			}
		}
		if len(udpProxies) == 0 {
			return nil, errors.New("no UDP-capable proxies available")
		}
	}

	// Try fresh best proxy first
	failedBest := ""
	if bestName, ok := s.routeTable.GetUDPBestProxyIfFresh(key, domain, smartBestProxyFreshness); ok {
		for _, p := range udpProxies {
			if p.Name() == bestName {
				dialCtx, dialCancel := context.WithTimeout(ctx, C.DefaultUDPTimeout)
				pc, err := s.dialUDPAndWrap(dialCtx, p, metadata, key, domain, false)
				dialCancel()
				if err == nil {
					return pc, nil
				}
				if tunnel.ShouldStopRetry(err) {
					return nil, err
				}
				s.routeTable.MarkUDPFailed(key, bestName, domain, 1.0)
				failedBest = bestName
				break
			}
		}
	}

	// Rank remaining by latency.  UDP has no TTFB of its own; a TCP-written
	// TTFB must not gate UDP candidates, otherwise a row with any TCP TTFB
	// sample could drop every UDP-capable proxy and leave none to try.
	names := namesOf(udpProxies)
	ranked := s.routeTable.PreRankLatency(names, s.lastDelayOf(proxies), key, domain)

	ordered := orderByNames(udpProxies, ranked)

	// Serial try
	var lastErr error
	for _, p := range ordered {
		if p.Name() == failedBest {
			continue
		}
		dialCtx, dialCancel := context.WithTimeout(ctx, C.DefaultUDPTimeout)
		pc, err := s.dialUDPAndWrap(dialCtx, p, metadata, key, domain, true)
		dialCancel()
		if err == nil {
			return pc, nil
		}
		lastErr = err
		if tunnel.ShouldStopRetry(err) {
			return nil, err
		}
	}

	return nil, lastErr
}

// dialUDPAndWrap dials a UDP proxy and wraps the packet conn for latency collection.
func (s *Smart) dialUDPAndWrap(ctx context.Context, proxy C.Proxy, metadata *C.Metadata, key, domain string, evaluated bool) (C.PacketConn, error) {
	start := time.Now()
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	connectTime := time.Since(start).Milliseconds()

	if err == nil && pc == nil {
		err = errors.New("proxy packet listen returned nil connection without error")
	}
	if err != nil {
		return nil, err
	}

	s.routeTable.UpdateLatency(key, domain, proxy.Name(), connectTime)
	s.routeTable.IncrementUseCount(key, domain, proxy.Name())
	s.routeTable.SetUDPBestProxy(key, domain, proxy.Name(), evaluated)

	return s.wrapUDPConn(pc, proxy, metadata), nil
}

// wrapUDPConn wraps a UDP packet connection. Unlike TCP, UDP does not collect
// connectTime here — connectTime is already written by dialUDPAndWrap.
func (s *Smart) wrapUDPConn(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata) C.PacketConn {
	pc.AppendToChains(s)
	return pc
}
