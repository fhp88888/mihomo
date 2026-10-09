package outboundgroup

import (
	"context"
	"errors"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mergeTestSmart() *Smart {
	return &Smart{GroupBase: NewGroupBase(GroupBaseOption{Name: "merge", Type: C.Smart}), routeTable: smart.NewRouteTable(10), probeCoordinator: NewProbeCoordinator(), ctx: context.Background(), sampleRate: 1}
}
func TestMergeCandidateRequiresCostAndOriginalPrior(t *testing.T) {
	s := mergeTestSmart()
	defer s.probeCoordinator.Close()
	key, domain := "TARGET:www.example.test", "www.example.test"
	s.routeTable.UpdateTTFB(key, domain, "a", 200)
	s.routeTable.UpdateTTFB("TARGET:cdn.example.test", "cdn.example.test", "b", 1000)
	st := &mergeRouteState{calls: 2, tried: map[string]bool{}, inflight: map[string]int{}}
	ps := []C.Proxy{&stubProxy{name: "a", delay: 20}, &stubProxy{name: "b", delay: 25}, &stubProxy{name: "c", delay: 35}, &stubProxy{name: "slow", delay: 100}}
	if s.explorationWorthRisk(key, domain, "a", "b") {
		t.Fatal("fixture must activate original prior veto")
	}
	if p := s.mergeCandidate(st, key, domain, "a", ps, true); p == nil || p.Name() != "c" {
		t.Fatal("candidate bypassed cost/prior intersection")
	}
	st.inflight["c"] = 1
	if p := s.mergeCandidate(st, key, domain, "a", ps, true); p != nil {
		t.Fatal("inflight or expensive candidate selected")
	}
}
func TestMergeEarlyTwoDistinctTrialsWithoutExtraDials(t *testing.T) {
	s := mergeTestSmart()
	defer s.probeCoordinator.Close()
	var dials atomic.Int32
	ps := []C.Proxy{}
	for _, v := range []struct {
		name  string
		delay uint16
	}{{"a", 20}, {"b", 25}, {"c", 30}} {
		ps = append(ps, &stubProxy{name: v.name, delay: v.delay, dial: func(context.Context, *C.Metadata) (C.Conn, error) { dials.Add(1); return &stubConn{}, nil }})
	}
	m := &C.Metadata{Host: "www.example.test", NetWork: C.TCP}
	key := routeKey(m)
	domain := routeDomain(m)
	conns := []C.Conn{}
	for i := 0; i < 3; i++ {
		c, e := s.mergeBetaRoute(context.Background(), m, key, domain, ps)
		if e != nil {
			t.Fatal(e)
		}
		conns = append(conns, c)
	}
	st := s.mergeRoutes[discoveryKey{key, domain}]
	if st.coldTrials != 2 || dials.Load() != 3 || !st.tried["b"] || !st.tried["c"] {
		t.Fatal("early trials not distinct or auxiliary dials occurred")
	}
	for _, c := range conns {
		c.Close()
		c.Close()
	}
	for _, v := range st.inflight {
		if v != 0 {
			t.Fatal("close release mismatch")
		}
	}
}
func TestMergeConcurrentTrialReservations(t *testing.T) {
	s := mergeTestSmart()
	defer s.probeCoordinator.Close()
	var dials atomic.Int32
	ps := []C.Proxy{}
	for _, v := range []struct {
		name  string
		delay uint16
	}{{"a", 20}, {"b", 25}, {"c", 30}} {
		ps = append(ps, &stubProxy{name: v.name, delay: v.delay, dial: func(context.Context, *C.Metadata) (C.Conn, error) { dials.Add(1); return &stubConn{}, nil }})
	}
	m := &C.Metadata{Host: "d.test", NetWork: C.TCP}
	key := routeKey(m)
	domain := routeDomain(m)
	c, e := s.mergeBetaRoute(context.Background(), m, key, domain, ps)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := s.mergeBetaRoute(context.Background(), m, key, domain, ps)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			time.Sleep(10 * time.Millisecond)
		}()
	}
	wg.Wait()
	if s.mergeRoutes[discoveryKey{key, domain}].coldTrials != 2 || dials.Load() != 3 {
		t.Fatal("concurrent trial budget exceeded")
	}
}
func TestMergeNoCandidatePreservesBudgetAndScorePath(t *testing.T) {
	s := mergeTestSmart()
	defer s.probeCoordinator.Close()
	ps := []C.Proxy{&stubProxy{name: "a", delay: 20, dial: mergeStubDial}, &stubProxy{name: "slow", delay: 100, dial: mergeStubDial}}
	s.routeTable.UpdateTTFB("k", "d", "a", 200)
	s.routeTable.SetBestProxyAndTCPProbed("k", "d", "a")
	m := &C.Metadata{Host: "d", NetWork: C.TCP}
	for i := 0; i < 3; i++ {
		c, e := s.mergeBetaRoute(context.Background(), m, "k", "d", ps)
		if e != nil {
			t.Fatal(e)
		}
		c.Close()
	}
	if s.mergeRoutes[discoveryKey{"k", "d"}].coldTrials != 0 {
		t.Fatal("missing candidate consumed budget")
	}
}
func TestMergeFailureConsumesTrialAndFallsBackOnlyAfterFailure(t *testing.T) {
	s := mergeTestSmart()
	defer s.probeCoordinator.Close()
	var a, b atomic.Int32
	p := &stubProxy{name: "a", delay: 20, dial: func(context.Context, *C.Metadata) (C.Conn, error) { a.Add(1); return &stubConn{}, nil }}
	q := &stubProxy{name: "b", delay: 25, dial: func(context.Context, *C.Metadata) (C.Conn, error) { b.Add(1); return nil, errors.New("failed") }}
	m := &C.Metadata{Host: "d", NetWork: C.TCP}
	c, e := s.mergeBetaRoute(context.Background(), m, "k", "d", []C.Proxy{p, q})
	if e != nil {
		t.Fatal(e)
	}
	c.Close()
	c, e = s.mergeBetaRoute(context.Background(), m, "k", "d", []C.Proxy{p, q})
	if e != nil {
		t.Fatal(e)
	}
	c.Close()
	if a.Load() != 2 || b.Load() != 1 || s.mergeRoutes[discoveryKey{"k", "d"}].coldTrials != 1 {
		t.Fatal("failed trial quota/fallback incorrect")
	}
}
func TestMergeMatureRollCanRetestMeasuredCandidate(t *testing.T) {
	s := mergeTestSmart()
	defer s.probeCoordinator.Close()
	s.routeTable.UpdateTTFB("k", "d", "a", 200)
	s.routeTable.UpdateTTFB("k", "d", "b", 220)
	s.routeTable.SetBestProxyAndTCPProbed("k", "d", "a")
	st := &mergeRouteState{coldTrials: 2, tried: map[string]bool{"a": true, "b": true}, inflight: map[string]int{}}
	s.mergeRoutes = map[discoveryKey]*mergeRouteState{{"k", "d"}: st}
	ps := []C.Proxy{&stubProxy{name: "a", delay: 20, dial: mergeStubDial}, &stubProxy{name: "b", delay: 25, dial: mergeStubDial}}
	m := &C.Metadata{Host: "d", NetWork: C.TCP}
	for i := 0; i < 24; i++ {
		c, e := s.mergeBetaRoute(context.Background(), m, "k", "d", ps)
		if e != nil {
			t.Fatal(e)
		}
		c.Close()
	}
	if st.tried["b"] != true {
		t.Fatal("fixture")
	}
	c, e := s.mergeBetaRoute(context.Background(), m, "k", "d", ps)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if st.inflight["b"] != 1 || st.coldTrials != 2 {
		t.Fatal("mature cadence stopped after cold budget")
	}
}

func mergeStubDial(context.Context, *C.Metadata) (C.Conn, error) { return &stubConn{}, nil }

func TestMergeSlowTrialDoesNotStartAnotherDial(t *testing.T) {
	s := mergeTestSmart()
	defer s.probeCoordinator.Close()
	var a, b atomic.Int32
	entered := make(chan struct{})
	finish := make(chan struct{})
	ps := []C.Proxy{&stubProxy{name: "a", delay: 20, dial: func(context.Context, *C.Metadata) (C.Conn, error) { a.Add(1); return &stubConn{}, nil }}, &stubProxy{name: "b", delay: 25, dial: func(context.Context, *C.Metadata) (C.Conn, error) {
		b.Add(1)
		close(entered)
		<-finish
		return &stubConn{}, nil
	}}}
	m := &C.Metadata{Host: "d", NetWork: C.TCP}
	c, e := s.mergeBetaRoute(context.Background(), m, "k", "d", ps)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	s.routeTable.UpdateLatency("k", "d", "b", 10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := s.mergeBetaRoute(context.Background(), m, "k", "d", ps)
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}()
	<-entered
	time.Sleep(80 * time.Millisecond)
	if a.Load() != 1 || b.Load() != 1 {
		t.Error("slow challenger caused speculative fallback")
	}
	close(finish)
	<-done
}
func TestMergeLateInitialDialCannotReplaceEstablishedBest(t *testing.T) {
	s := mergeTestSmart()
	defer s.probeCoordinator.Close()
	s.routeTable.UpdateLatency("k", "d", "a", 20)
	s.routeTable.SetBestProxyAndTCPProbed("k", "d", "a")
	p := &stubProxy{name: "b", delay: 25, dial: mergeStubDial}
	m := &C.Metadata{Host: "d", NetWork: C.TCP}
	c, e := s.mergeDial(context.Background(), m, "k", "d", p, "", false)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if best, _ := s.routeTable.GetBestProxy("k", "d"); best != "a" {
		t.Fatal("late initial dial overwrote a newer best")
	}
}

type mergeHeadroomConn struct{ stubConn }

func (c *mergeHeadroomConn) FrontHeadroom() int { return 18 }
func TestMergeLifecycleWrapperPreservesProtocolHeadroom(t *testing.T) {
	raw := &mergeHeadroomConn{}
	wrapped := &mergeTrialConn{Conn: raw, release: func() {}}
	if got := N.CalculateFrontHeadroom(wrapped); got != 18 {
		t.Fatalf("lost encryption headroom: %d", got)
	}
}
