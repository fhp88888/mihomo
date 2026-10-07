package outboundgroup

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
)

func attachExitWatcher(t *testing.T, s *Smart) {
	t.Helper()
	s.exitWatch = smart.NewExitWatcher(smart.ExitWatcherOptions{Name: s.Name(), Config: s.configName, Store: s.store})
	t.Cleanup(func() { s.cancel(); s.exitWatch.Close() })
}

func TestSmartExitGatePreservesHTTPBansScoreAndManual(t *testing.T) {
	a, b, untried, control := newResponseProxy("a", 403), newResponseProxy("b", 403), newResponseProxy("untried", 200), newResponseProxy("control", 200)
	nodes := []C.Proxy{a, b, untried, control}
	s := newResponseSmart(t, nodes...)
	attachExitWatcher(t, s)
	m := &C.Metadata{Host: "api.example.com", DstPort: 443, DstIPASN: "13335"}
	key, domain := routeKey(m), routeDomain(m)
	m.WildcardTarget = domain
	m.SmartTarget = domain
	s.routeTable.UpdateLatency(key, domain, untried.Name(), 1)
	s.routeTable.SetBestProxyAndTCPProbed(key, domain, untried.Name())
	before := s.routeTable.ProxyFailedCount(key, domain, untried.Name())
	for _, p := range nodes {
		region := "US"
		if p == control {
			region = "JP"
		}
		s.exitWatch.Store(p.Name(), smart.ExitInfo{Region: region, Key: p.Name()}, time.Now())
	}
	verdict := func(p *responseTestProxy) {
		s.applyNodeAnswer(m, p.Name(), smart.ClassifyResponse(int(p.status.Load()), nil, nil, time.Now()))
	}
	verdict(control)
	verdict(a)
	verdict(b)
	available := s.responseCandidates(m, nodes)
	if len(available) != 1 || available[0] != control {
		t.Fatal("untried same-region candidate not deferred")
	}
	if s.Unwrap(m, false).Name() != control.Name() {
		t.Fatal("cached best bypassed class gate")
	}
	var dialed atomic.Int32
	control.dial = func(context.Context, *C.Metadata) (C.Conn, error) { dialed.Add(1); return &stubConn{}, nil }
	conn, err := s.tcpRoute(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if dialed.Load() != 1 {
		t.Fatal("TCP exploitation bypassed exit gate")
	}

	if after := s.routeTable.ProxyFailedCount(key, domain, untried.Name()); after != before {
		t.Fatal("exit suspicion changed quality")
	}
	if got := s.responseCandidates(&C.Metadata{Host: "other.example.com", DstIPASN: "13335"}, nodes); len(got) != 4 {
		t.Fatal("inference affected another hostname in same ASN")
	}
	// With the control unavailable, only the untried node may get a bounded fallback.
	got := s.responseCandidates(m, []C.Proxy{a, b, untried})
	if len(got) != 1 || got[0] != untried {
		t.Fatal("fallback bypassed explicit HTTP bans or lost usable trial")
	}
	if got := s.responseCandidates(m, []C.Proxy{a, b}); len(got) != 0 {
		t.Fatal("class fallback resurrected HTTP banned node")
	}
	s.selected = a.Name()
	if len(s.responseCandidates(m, nodes)) != 4 {
		t.Fatal("manual choice subject to inference")
	}
	s.selected = ""
	verdict(untried)
	if len(s.responseCandidates(m, nodes)) != 2 {
		t.Fatal("success failed to clear class while preserving node bans")
	}
	if s.exitWatch.Suspected(domain, untried.Name()) {
		t.Fatal("successful trial did not restore region")
	}
}

type exitResponseProxy struct {
	*responseTestProxy
	result             smart.ExitProbeResult
	started            chan struct{}
	release            chan struct{}
	probes             atomic.Int32
	once               sync.Once
	ignoreCancellation bool
}

func (p *exitResponseProxy) ExitProbe(ctx context.Context, _ bool) (*smart.ExitProbeResult, error) {
	p.probes.Add(1)
	p.once.Do(func() { close(p.started) })
	if p.ignoreCancellation {
		<-p.release
	} else {
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &p.result, nil
}

func TestSmartExitProbeCloseDropsLateAnswer(t *testing.T) {
	p := &exitResponseProxy{responseTestProxy: newResponseProxy("a", 200), result: smart.ExitProbeResult{Region: "US", Key: "a"}, started: make(chan struct{}), release: make(chan struct{}), ignoreCancellation: true}
	s := newResponseSmart(t, p)
	attachExitWatcher(t, s)
	s.exitWatch.MaybeProbe(s.ctx, p)
	select {
	case <-p.started:
	case <-time.After(time.Second):
		t.Fatal("exit probe not started")
	}
	finished := make(chan struct{})
	s.cancel()
	go func() { s.exitWatch.Close(); close(finished) }()
	// Even a prober that ignores cancellation must not persist a late answer.
	close(p.release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("close did not wait for probe")
	}
	if s.exitWatch.Info(p.Name()).Region != "" {
		t.Fatal("late exit answer accepted")
	}
	for i := 0; i < 20; i++ {
		s.exitWatch.MaybeProbe(s.ctx, p)
	}
	if p.probes.Load() != 1 {
		t.Fatal("closed watcher admitted more probes")
	}
}

func TestSmartExitProbeReplaysHeldHTTPControl(t *testing.T) {
	p := &exitResponseProxy{responseTestProxy: newResponseProxy("control", 200), result: smart.ExitProbeResult{Region: "JP", Key: "control"}, started: make(chan struct{}), release: make(chan struct{})}
	a, b := newResponseProxy("a", 403), newResponseProxy("b", 403)
	s := newResponseSmart(t, a, b, p)
	attachExitWatcher(t, s)
	m := &C.Metadata{Host: "api.example.com", DstPort: 443, WildcardTarget: "api.example.com", SmartTarget: "api.example.com"}
	for _, n := range []C.Proxy{a, b} {
		s.exitWatch.Store(n.Name(), smart.ExitInfo{Region: "US", Key: n.Name()}, time.Now())
		s.applyNodeAnswer(m, n.Name(), smart.ClassifyResponse(403, nil, nil, time.Now()))
	}
	s.applyNodeAnswer(m, p.Name(), smart.ClassifyResponse(200, nil, nil, time.Now()))
	select {
	case <-p.started:
	case <-time.After(time.Second):
		t.Fatal("HTTP evidence did not request exit info")
	}
	if s.exitWatch.Suspected(routeDomain(m), a.Name()) {
		t.Fatal("unknown control exit prematurely corroborated restriction")
	}
	close(p.release)
	deadline := time.Now().Add(time.Second)
	for !s.exitWatch.Suspected(routeDomain(m), a.Name()) {
		if time.Now().After(deadline) {
			t.Fatal("held control not replayed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSmartExitDoesNotPoolRateLimitOrChallenge(t *testing.T) {
	for _, reason := range []string{"rate limited", "challenge"} {
		t.Run(reason, func(t *testing.T) {
			a, b, c := newResponseProxy("a", 403), newResponseProxy("b", 403), newResponseProxy("c", 200)
			s := newResponseSmart(t, a, b, c)
			attachExitWatcher(t, s)
			m := &C.Metadata{Host: "api.example", SmartTarget: "api.example", WildcardTarget: "api.example"}
			for _, p := range []C.Proxy{a, b, c} {
				region := "US"
				if p == c {
					region = "JP"
				}
				s.exitWatch.Store(p.Name(), smart.ExitInfo{Region: region, Key: p.Name()}, time.Now())
			}
			s.applyNodeAnswer(m, c.Name(), smart.ClassifyResponse(200, nil, nil, time.Now()))
			for _, p := range []C.Proxy{a, b} {
				s.applyNodeAnswer(m, p.Name(), smart.Verdict{Action: smart.VerdictRecord, Reason: reason, TTL: time.Minute})
			}
			if s.exitWatch.Active() {
				t.Fatal("non-regional refusal pooled into exit class")
			}
		})
	}
}

func TestSmartExitUDPFallbackSkipsTCPOnlyNode(t *testing.T) {
	tcp := newResponseProxy("tcp-only", 200)
	udp := &udpErrorProxy{stubProxy: &stubProxy{name: "udp"}}
	control := newResponseProxy("control", 200)
	s := newResponseSmart(t, tcp, udp, control)
	attachExitWatcher(t, s)
	now := time.Now()
	s.exitWatch.Store(tcp.Name(), smart.ExitInfo{Region: "US", Key: "tcp"}, now)
	s.exitWatch.Store(udp.Name(), smart.ExitInfo{Region: "US", Key: "udp"}, now)
	s.exitWatch.Store(control.Name(), smart.ExitInfo{Region: "JP", Key: "control"}, now)
	target := "api.example"
	s.exitWatch.NoteSuccess(target, control.Name())
	s.exitWatch.Note(target, tcp.Name())
	s.exitWatch.Note(target, udp.Name())
	m := &C.Metadata{Host: target, NetWork: C.UDP}
	s.routeTable.SetUDPBestProxy(routeKey(m), target, tcp.Name(), true)
	if _, err := s.udpRoute(context.Background(), m); err == nil {
		t.Fatal("fake UDP dial must fail")
	}
	if udp.calls != 1 {
		t.Fatal("TCP-only node consumed UDP fallback lease")
	}
}
