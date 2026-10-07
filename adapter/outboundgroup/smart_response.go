package outboundgroup

// HTTP answer probes and host recovery ported from Alpha. Beta keeps its route
// scoring and exploration, and does not close existing flows after a refusal.
import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/samber/lo"
)

const blockCodeAnswer = 2
const responseParallelDials = 5

func (s *Smart) markNodeFailure(metadata *C.Metadata, proxyName string, isDegraded bool, checked bool, blockCode int64, ttl time.Duration) bool {
	wildcardTarget := metadata.WildcardTarget
	target := metadata.SmartTarget

	failedBlock := s.store.UpdateHostStatus(s.Name(), s.configName, wildcardTarget, metadata, proxyName, s.maxFailedTimes, s.maxFailedTimes, isDegraded, checked, blockCode, ttl)

	if (isDegraded || failedBlock || ttl > 0) && target != "" && target != wildcardTarget {
		// Keep Alpha target-level propagation for callers with an extra target.
		if s.store.UpdateHostStatus(s.Name(), s.configName, target, metadata, proxyName, s.maxFailedTimes, s.maxFailedTimes, isDegraded, checked, blockCode, ttl) {
			failedBlock = true
		}
	}

	return failedBlock
}

func (s *Smart) applyNodeAnswer(metadata *C.Metadata, node string, verdict smart.Verdict) {
	switch verdict.Action {
	case smart.VerdictReachable:
		s.markNodeFailure(metadata, node, false, true, 0, 0)
		if s.exitWatch != nil && verdict.ControlSuccess {
			s.exitWatch.EnsureLoaded(node)
			s.exitWatch.Clear(routeDomain(metadata), node)
			s.exitWatch.NoteSuccess(routeDomain(metadata), node)
		}
	case smart.VerdictRecord:
		s.markNodeFailure(metadata, node, true, true, blockCodeAnswer, verdict.TTL)
		if s.exitWatch != nil && smart.RegionEvidence(verdict.Reason) {
			s.exitWatch.EnsureLoaded(node)
			s.exitWatch.Note(routeDomain(metadata), node)
		}
	}
	if s.exitWatch != nil && (verdict.ControlSuccess || (verdict.Action == smart.VerdictRecord && smart.RegionEvidence(verdict.Reason))) {
		for _, proxy := range s.GetProxies(false) {
			if proxy.Name() == node {
				s.exitWatch.MaybeProbe(s.ctx, proxy)
				break
			}
		}
	}
}

func (s *Smart) checkHostStatus() {
	if s.store == nil || s.selected != "" {
		return
	}
	proxies := s.GetProxies(false)
	proxyMap := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		proxyMap[p.Name()] = p
	}

	toCheck, err := s.store.CheckHostStatus(s.Name(), s.configName, s.maxFailedTimes)
	if err != nil {
		return
	}

	type checkItem struct {
		wildcardTarget string
		nodeName       string
		host           string
	}

	var items []checkItem
	for wildcardTarget, nodeMap := range toCheck {
		for nodeName, host := range nodeMap {
			items = append(items, checkItem{wildcardTarget, nodeName, host})
		}
	}

	var toProbe []checkItem
	now := time.Now()
	for _, it := range items {
		if s.probeThrottle.Blind(it.host, now) {
			continue
		}
		if rand.Float64() < 0.5 {
			toProbe = append(toProbe, it)
		}
	}
	if len(toProbe) == 0 {
		return
	}

	jobs := make(chan checkItem)
	var wg sync.WaitGroup
	for i := 0; i < responseParallelDials; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range jobs {
				select {
				case <-s.ctx.Done():
					return
				default:
				}
				p, ok := proxyMap[it.nodeName]
				if !ok {
					continue
				}
				metadata := &C.Metadata{Host: it.host, WildcardTarget: it.wildcardTarget}
				now := time.Now()
				if !s.probeThrottle.AllowNode(it.wildcardTarget, it.nodeName, now) || !smart.AllowGlobalProbe(now) {
					continue
				}
				done, ok := smart.TryStartProbe()
				if !ok {
					continue
				}
				verdict := s.probeVerdict(p, it.host)
				done()
				if s.ctx.Err() != nil || s.selected != "" || !lo.ContainsBy(s.GetProxies(false), func(p C.Proxy) bool { return p.Name() == it.nodeName }) || !s.probeThrottle.AllowHostRecord(it.host, verdict, time.Now()) {
					continue
				}
				s.applyNodeAnswer(metadata, it.nodeName, verdict)
				switch verdict.Action {
				case smart.VerdictReachable:
					log.Debugln("[Smart] Recheck Group: [%s] - Node: [%s] - Host: [%s] recovered [%s]", s.Name(), it.nodeName, it.host, verdict.Reason)
				case smart.VerdictRecord:
					log.Debugln("[Smart] Recheck Group: [%s] - Node: [%s] - Host: [%s] avoided for [%s]: [%s]", s.Name(), it.nodeName, it.host, verdict.TTL, verdict.Reason)
				default:
					log.Debugln("[Smart] Recheck Group: [%s] - Node: [%s] - Host: [%s] left unchanged [%s]", s.Name(), it.nodeName, it.host, verdict.Reason)
				}
			}
		}()
	}

sendLoop:
	for _, it := range toProbe {
		select {
		case jobs <- it:
		case <-s.ctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	wg.Wait()
}

func (s *Smart) probeAfterClose(metadata *C.Metadata, proxy C.Proxy) {
	if s.store == nil || s.selected != "" {
		return
	}
	metadata = metadata.Clone()
	metadata.WildcardTarget = routeDomain(metadata)
	metadata.SmartTarget = metadata.WildcardTarget
	now := time.Now()
	if !s.probeThrottle.AllowNode(metadata.WildcardTarget, proxy.Name(), now) {
		return
	}
	// a blind host is not probed until its window passes, then one probe decides its state
	if s.probeThrottle.Blind(metadata.Host, now) {
		return
	}
	if !smart.AllowGlobalProbe(now) {
		return
	}
	s.responseMu.Lock()
	if s.responseClosed || (s.ctx != nil && s.ctx.Err() != nil) {
		s.responseMu.Unlock()
		return
	}
	done, ok := smart.TryStartProbe()
	if !ok {
		s.responseMu.Unlock()
		return
	}
	s.responseWG.Add(1)
	s.responseMu.Unlock()

	clone := metadata
	nodeName := proxy.Name()
	go func() {
		defer done()
		defer s.responseWG.Done()

		verdict := s.probeVerdict(proxy, clone.Host)
		if verdict.Action == smart.VerdictIgnore {
			log.Debugln("[Smart] Probe Group: [%s] - Node: [%s] - Host: [%s] ignored answer [%s]", s.Name(), nodeName, clone.Host, verdict.Reason)
			return
		}
		if !s.probeThrottle.AllowHostRecord(clone.Host, verdict, time.Now()) {
			log.Debugln("[Smart] Probe Group: [%s] - Node: [%s] - Host: [%s] kept, no node reaches the host [%s]", s.Name(), nodeName, clone.Host, verdict.Reason)
			return
		}
		if (s.ctx != nil && s.ctx.Err() != nil) || s.selected != "" || !lo.ContainsBy(s.GetProxies(false), func(p C.Proxy) bool { return p.Name() == nodeName }) {
			return
		}
		if _, wtBlocked := s.store.GetHostStatus(s.Name(), s.configName, clone.WildcardTarget, s.maxFailedTimes, clone.SmartTarget); wtBlocked {
			return
		}

		if verdict.Action == smart.VerdictReachable {
			s.applyNodeAnswer(clone, nodeName, verdict)
			log.Debugln("[Smart] Probe Group: [%s] - Node: [%s] - Host: [%s] recovered [%s]", s.Name(), nodeName, clone.Host, verdict.Reason)
			return
		}

		s.probeThrottle.NoteFailure(clone.WildcardTarget, nodeName, time.Now())
		s.applyNodeAnswer(clone, nodeName, verdict)
		log.Debugln("[Smart] Probe Group: [%s] - Node: [%s] - Host: [%s] avoided for [%s]: [%s]", s.Name(), nodeName, clone.Host, verdict.TTL, verdict.Reason)
	}()
}

func (s *Smart) probeVerdict(proxy C.Proxy, host string) smart.Verdict {
	prober, ok := proxy.(smart.StatusProber)
	if !ok {
		return smart.Verdict{Action: smart.VerdictIgnore, Reason: "no probe support"}
	}

	parent := s.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, smart.ProbeTimeout)
	defer cancel()

	result, err := prober.StatusProbe(ctx, smart.ProbeURL(host))
	if err != nil {
		return smart.ClassifyProbeError(err)
	}
	return smart.ClassifyResponse(result.StatusCode, result.Header, result.Body, time.Now())
}

func (s *Smart) responseCandidates(metadata *C.Metadata, proxies []C.Proxy) []C.Proxy {
	if s.store == nil || s.selected != "" || metadata == nil {
		return proxies
	}
	failed, blocked := s.store.GetHostStatus(s.Name(), s.configName, routeDomain(metadata), s.maxFailedTimes)
	if len(failed) == 0 {
		return s.exitCandidates(metadata, proxies)
	}
	available := make([]C.Proxy, 0, len(proxies))
	for _, p := range proxies {
		code := failed[p.Name()]
		if code == 0 || (blocked && code != 1) {
			available = append(available, p)
		}
	}
	return s.exitCandidates(metadata, available)
}

// Exit inference changes eligibility, never Beta's measured quality or scoring.
// Node-specific HTTP exclusions are applied before this layer and stay excluded.
func (s *Smart) exitCandidates(metadata *C.Metadata, proxies []C.Proxy) []C.Proxy {
	if s.exitWatch == nil || !s.exitWatch.Active() {
		return proxies
	}
	target, now := routeDomain(metadata), time.Now()
	var available, deferred []C.Proxy
	healthy := false
	for _, p := range proxies {
		// A TCP-only node must not consume the UDP recovery lease.
		if metadata.NetWork == C.UDP && !p.SupportUDP() {
			continue
		}
		s.exitWatch.EnsureLoaded(p.Name())
		if s.exitWatch.Defer(target, p.Name(), now, smart.ProbeTimeout) {
			deferred = append(deferred, p)
		} else {
			available = append(available, p)
			healthy = healthy || p.AliveForTestUrl(s.testUrl)
		}
	}
	if len(deferred) == 0 {
		return proxies
	}
	if !healthy {
		// Prefer a healthy deferred exit; retain health recovery when none are alive.
		hasHealthyDeferred := false
		for _, p := range deferred {
			hasHealthyDeferred = hasHealthyDeferred || p.AliveForTestUrl(s.testUrl)
		}
		for _, p := range deferred {
			if hasHealthyDeferred && !p.AliveForTestUrl(s.testUrl) {
				continue
			}
			if s.exitWatch.AllowFallback(target, p.Name(), now, smart.ProbeTimeout) {
				available = append(available, p)
				break
			}
		}
	}
	return available
}
