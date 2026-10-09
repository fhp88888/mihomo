package outboundgroup

import (
	"context"
	"errors"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
	"sort"
	"sync"
	"time"
)

// Scheduling only: performance observations remain in Beta's RouteTable.
type mergeRouteState struct {
	calls, coldTrials int
	tried             map[string]bool
	inflight          map[string]int
	last              time.Time
}

func mergeDelay(p C.Proxy, url string) uint16 {
	d := p.LastDelayForTestUrl(url)
	if d == 0 {
		return 65535
	}
	return d
}

// Caller holds mergeMu. Both the URL-test cost tier and original prior veto
// are mandatory. Mature rolls may re-evaluate measured nodes once no new
// candidate remains; cold-start opportunities always use distinct new nodes.
func (s *Smart) mergeCandidate(st *mergeRouteState, key, domain, best string, ps []C.Proxy, cold bool) C.Proxy {
	if len(ps) == 0 {
		return nil
	}
	ordered := append([]C.Proxy(nil), ps...)
	sort.SliceStable(ordered, func(i, j int) bool { return mergeDelay(ordered[i], s.testUrl) < mergeDelay(ordered[j], s.testUrl) })
	minimum := uint32(mergeDelay(ordered[0], s.testUrl))
	tested := []C.Proxy{}
	for _, p := range ordered {
		name := p.Name()
		costOK := uint32(mergeDelay(p, s.testUrl)) <= 2*minimum
		if name == best || st.inflight[name] > 0 || !p.AliveForTestUrl(s.testUrl) || s.routeTable.ProxyFailedCount(key, domain, name) > 0 {
			continue
		}
		priorOK := s.explorationWorthRisk(key, domain, best, name)
		log.Debugln("[MergeBeta] candidate target=%s call=%d proxy=%s delay=%d minimum=%d cost_ok=%t prior_ok=%t", domain, st.calls, name, mergeDelay(p, s.testUrl), minimum, costOK, priorOK)
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

func (s *Smart) mergeBetaRoute(ctx context.Context, m *C.Metadata, key, domain string, ps []C.Proxy) (C.Conn, error) {
	healthy := []C.Proxy{}
	for _, p := range ps {
		if p.AliveForTestUrl(s.testUrl) {
			healthy = append(healthy, p)
		}
	}
	if len(healthy) == 0 {
		if err := s.waitForHealthRecovery(ctx); err != nil {
			return nil, err
		}
		ps = s.responseCandidates(m, s.GetProxies(true))
		for _, p := range ps {
			if p.AliveForTestUrl(s.testUrl) {
				healthy = append(healthy, p)
			}
		}
		if len(healthy) == 0 {
			return nil, errors.New("no healthy merge_beta candidates")
		}
	}
	s.mergeMu.Lock()
	if s.mergeRoutes == nil {
		s.mergeRoutes = map[discoveryKey]*mergeRouteState{}
	}
	id := discoveryKey{key, domain}
	st := s.mergeRoutes[id]
	if st == nil {
		if len(s.mergeRoutes) >= 1024 {
			var oldest discoveryKey
			var stamp time.Time
			for k, v := range s.mergeRoutes {
				if stamp.IsZero() || v.last.Before(stamp) {
					oldest, stamp = k, v.last
				}
			}
			delete(s.mergeRoutes, oldest)
		}
		st = &mergeRouteState{tried: map[string]bool{}, inflight: map[string]int{}}
		s.mergeRoutes[id] = st
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
		challenger = s.mergeCandidate(st, key, domain, best, healthy, true)
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
			if d := uint32(mergeDelay(p, s.testUrl)); d < minimum {
				minimum = d
			}
		}
		eligible := 0
		for _, p := range healthy {
			name := p.Name()
			if name != best && !st.tried[name] && st.inflight[name] == 0 && !s.routeTable.ProxyHasTTFBSample(key, domain, name) && s.routeTable.ProxyFailedCount(key, domain, name) == 0 && uint32(mergeDelay(p, s.testUrl)) <= 2*minimum && s.explorationWorthRisk(key, domain, best, name) {
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
			challenger = s.mergeCandidate(st, key, domain, best, healthy, false)
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
	s.mergeMu.Unlock()
	if challenger == nil {
		log.Debugln("[MergeBeta] decision target=%s call=%d phase=normal cold_trials=%d roll=%t", domain, call, trials, roll)
		if initial != nil {
			conn, err := s.mergeDial(ctx, m, key, domain, initial, capturedBest, false)
			release := func() { s.mergeMu.Lock(); st.inflight[initial.Name()]--; s.mergeMu.Unlock() }
			if err == nil {
				return &mergeTrialConn{Conn: conn, release: release}, nil
			}
			release()
			if ctx.Err() != nil || tunnel.ShouldStopRetry(err) {
				return nil, err
			}
			remaining := []C.Proxy{}
			for _, p := range healthy {
				if p.Name() != initial.Name() {
					remaining = append(remaining, p)
				}
			}
			if len(remaining) == 0 {
				return nil, err
			}
			return s.mergeNormal(ctx, m, key, domain, remaining)
		}
		return s.mergeNormal(ctx, m, key, domain, healthy)
	}
	log.Debugln("[MergeBeta] decision target=%s call=%d phase=%s proxy=%s incumbent=%s cold_trials=%d roll=%t", domain, call, phase, challenger.Name(), best, trials, roll)
	release := func() { s.mergeMu.Lock(); st.inflight[challenger.Name()]--; s.mergeMu.Unlock() }
	conn, err := s.mergeDial(ctx, m, key, domain, challenger, best, true)
	if err != nil {
		release()
		if ctx.Err() != nil || tunnel.ShouldStopRetry(err) {
			return nil, err
		}
		remaining := []C.Proxy{}
		for _, p := range healthy {
			if p.Name() != challenger.Name() {
				remaining = append(remaining, p)
			}
		}
		if len(remaining) == 0 {
			return nil, err
		}
		return s.mergeNormal(ctx, m, key, domain, remaining)
	}
	return &mergeTrialConn{Conn: conn, release: release}, nil
}
func (s *Smart) mergeNormal(ctx context.Context, m *C.Metadata, key, domain string, ps []C.Proxy) (C.Conn, error) {
	previous, _ := s.routeTable.GetBestProxy(key, domain)
	if best, ok := s.routeTable.GetBestProxy(key, domain); ok {
		for _, p := range ps {
			if p.Name() == best && p.AliveForTestUrl(s.testUrl) {
				c, e := s.serialTcpConn(ctx, m, key, domain, ps)
				if c != nil || e != nil {
					return c, e
				}
			}
		}
	}
	// First business connection uses Beta's ranking without a discovery batch.
	// Normal Beta best-first/stale-rerank and its existing recovery remain intact.
	ranked := s.rankCandidates(key, domain, ps, nil)
	var last error
	for _, p := range ranked {
		s.mergeMu.Lock()
		st := s.mergeRoutes[discoveryKey{key, domain}]
		if st != nil {
			st.tried[p.Name()] = true
			st.inflight[p.Name()]++
		}
		s.mergeMu.Unlock()
		conn, err := s.mergeDial(ctx, m, key, domain, p, previous, false)
		release := func() {
			s.mergeMu.Lock()
			if st != nil {
				st.inflight[p.Name()]--
			}
			s.mergeMu.Unlock()
		}
		if err == nil {
			return &mergeTrialConn{Conn: conn, release: release}, nil
		}
		release()
		last = err
		if ctx.Err() != nil || tunnel.ShouldStopRetry(err) {
			return nil, err
		}
	}
	if last == nil {
		last = errors.New("no ranked merge_beta candidates")
	}
	return nil, last
}
func (s *Smart) mergeDial(ctx context.Context, m *C.Metadata, key, domain string, p C.Proxy, incumbent string, trial bool) (C.Conn, error) {
	conn, latency, err := s.dialTCP(ctx, p, m, key)
	if err != nil {
		return nil, err
	}
	s.routeTable.UpdateLatency(key, domain, p.Name(), latency)
	s.routeTable.IncrementUseCount(key, domain, p.Name())
	if !trial || incumbent == "" {
		s.routeTable.PromoteTCPBestAfterFallback(key, domain, incumbent, p.Name())
		s.routeTable.SetTCPProbedPreserveEvaluation(key, domain)
	} else {
		s.routeTable.SetTCPProbedPreserveEvaluation(key, domain)
	}
	if trial {
		return s.wrapTCPConnWithExploration(conn, p, m, latency, incumbent, p.Name(), false), nil
	}
	return s.wrapTCPConn(conn, p, m, latency), nil
}

type mergeTrialConn struct {
	C.Conn
	once    sync.Once
	release func()
	err     error
}

func (c *mergeTrialConn) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close(); c.release() })
	return c.err
}

// Preserve protocol metadata through the lifecycle-only wrapper.
func (c *mergeTrialConn) Upstream() any { return c.Conn }
