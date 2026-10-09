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

func businessExplorationTestSmart() *Smart {
	return &Smart{GroupBase: NewGroupBase(GroupBaseOption{Name: "business-exploration", Type: C.Smart}), routeTable: smart.NewRouteTable(10), raceCoordinator: newSmartRaceCoordinator(), ctx: context.Background(), sampleRate: 1}
}
func TestBusinessExplorationCandidateRequiresCostAndOriginalPrior(t *testing.T) {
	s := businessExplorationTestSmart()
	defer s.raceCoordinator.Close()
	key, domain := "TARGET:www.example.test", "www.example.test"
	s.routeTable.UpdateTTFB(key, domain, "a", 200)
	s.routeTable.UpdateTTFB("TARGET:cdn.example.test", "cdn.example.test", "b", 1000)
	st := &businessExplorationState{calls: 2, tried: map[string]bool{}, inflight: map[string]int{}}
	ps := []C.Proxy{&stubProxy{name: "a", delay: 20}, &stubProxy{name: "b", delay: 25}, &stubProxy{name: "c", delay: 35}, &stubProxy{name: "slow", delay: 100}}
	if s.explorationWorthRisk(key, domain, "a", "b") {
		t.Fatal("fixture must activate original prior veto")
	}
	if p := s.selectBusinessChallenger(st, key, domain, "a", ps, true); p == nil || p.Name() != "c" {
		t.Fatal("candidate bypassed cost/prior intersection")
	}
	st.inflight["c"] = 1
	if p := s.selectBusinessChallenger(st, key, domain, "a", ps, true); p != nil {
		t.Fatal("inflight or expensive candidate selected")
	}
}
func TestBusinessExplorationEarlyTwoDistinctTrialsWithoutExtraDials(t *testing.T) {
	s := businessExplorationTestSmart()
	defer s.raceCoordinator.Close()
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
		c, e := s.routeWithBusinessExploration(context.Background(), m, key, domain, ps)
		if e != nil {
			t.Fatal(e)
		}
		conns = append(conns, c)
	}
	st := s.explorationRoutes[explorationRouteKey{key, domain}]
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
func TestBusinessExplorationConcurrentTrialReservations(t *testing.T) {
	s := businessExplorationTestSmart()
	defer s.raceCoordinator.Close()
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
	c, e := s.routeWithBusinessExploration(context.Background(), m, key, domain, ps)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := s.routeWithBusinessExploration(context.Background(), m, key, domain, ps)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			time.Sleep(10 * time.Millisecond)
		}()
	}
	wg.Wait()
	if s.explorationRoutes[explorationRouteKey{key, domain}].coldTrials != 2 || dials.Load() != 3 {
		t.Fatal("concurrent trial budget exceeded")
	}
}
func TestBusinessExplorationNoCandidatePreservesBudgetAndScorePath(t *testing.T) {
	s := businessExplorationTestSmart()
	defer s.raceCoordinator.Close()
	ps := []C.Proxy{&stubProxy{name: "a", delay: 20, dial: businessExplorationStubDial}, &stubProxy{name: "slow", delay: 100, dial: businessExplorationStubDial}}
	s.routeTable.UpdateTTFB("k", "d", "a", 200)
	s.routeTable.SetBestProxyAndTCPProbed("k", "d", "a")
	m := &C.Metadata{Host: "d", NetWork: C.TCP}
	for i := 0; i < 3; i++ {
		c, e := s.routeWithBusinessExploration(context.Background(), m, "k", "d", ps)
		if e != nil {
			t.Fatal(e)
		}
		c.Close()
	}
	if s.explorationRoutes[explorationRouteKey{"k", "d"}].coldTrials != 0 {
		t.Fatal("missing candidate consumed budget")
	}
}
func TestBusinessExplorationFailureConsumesTrialAndFallsBackOnlyAfterFailure(t *testing.T) {
	s := businessExplorationTestSmart()
	defer s.raceCoordinator.Close()
	var a, b atomic.Int32
	p := &stubProxy{name: "a", delay: 20, dial: func(context.Context, *C.Metadata) (C.Conn, error) { a.Add(1); return &stubConn{}, nil }}
	q := &stubProxy{name: "b", delay: 25, dial: func(context.Context, *C.Metadata) (C.Conn, error) { b.Add(1); return nil, errors.New("failed") }}
	m := &C.Metadata{Host: "d", NetWork: C.TCP}
	c, e := s.routeWithBusinessExploration(context.Background(), m, "k", "d", []C.Proxy{p, q})
	if e != nil {
		t.Fatal(e)
	}
	c.Close()
	c, e = s.routeWithBusinessExploration(context.Background(), m, "k", "d", []C.Proxy{p, q})
	if e != nil {
		t.Fatal(e)
	}
	c.Close()
	if a.Load() != 2 || b.Load() != 1 || s.explorationRoutes[explorationRouteKey{"k", "d"}].coldTrials != 1 {
		t.Fatal("failed trial quota/fallback incorrect")
	}
}
func TestBusinessExplorationMatureRollCanRetestMeasuredCandidate(t *testing.T) {
	s := businessExplorationTestSmart()
	defer s.raceCoordinator.Close()
	s.routeTable.UpdateTTFB("k", "d", "a", 200)
	s.routeTable.UpdateTTFB("k", "d", "b", 220)
	s.routeTable.SetBestProxyAndTCPProbed("k", "d", "a")
	st := &businessExplorationState{coldTrials: 2, tried: map[string]bool{"a": true, "b": true}, inflight: map[string]int{}}
	s.explorationRoutes = map[explorationRouteKey]*businessExplorationState{{"k", "d"}: st}
	ps := []C.Proxy{&stubProxy{name: "a", delay: 20, dial: businessExplorationStubDial}, &stubProxy{name: "b", delay: 25, dial: businessExplorationStubDial}}
	m := &C.Metadata{Host: "d", NetWork: C.TCP}
	for i := 0; i < 24; i++ {
		c, e := s.routeWithBusinessExploration(context.Background(), m, "k", "d", ps)
		if e != nil {
			t.Fatal(e)
		}
		c.Close()
	}
	if st.tried["b"] != true {
		t.Fatal("fixture")
	}
	c, e := s.routeWithBusinessExploration(context.Background(), m, "k", "d", ps)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if st.inflight["b"] != 1 || st.coldTrials != 2 {
		t.Fatal("mature cadence stopped after cold budget")
	}
}

func businessExplorationStubDial(context.Context, *C.Metadata) (C.Conn, error) {
	return &stubConn{}, nil
}

func TestBusinessExplorationSlowTrialDoesNotStartAnotherDial(t *testing.T) {
	s := businessExplorationTestSmart()
	defer s.raceCoordinator.Close()
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
	c, e := s.routeWithBusinessExploration(context.Background(), m, "k", "d", ps)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	s.routeTable.UpdateLatency("k", "d", "b", 10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := s.routeWithBusinessExploration(context.Background(), m, "k", "d", ps)
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
func TestBusinessExplorationLateInitialDialCannotReplaceEstablishedBest(t *testing.T) {
	s := businessExplorationTestSmart()
	defer s.raceCoordinator.Close()
	s.routeTable.UpdateLatency("k", "d", "a", 20)
	s.routeTable.SetBestProxyAndTCPProbed("k", "d", "a")
	p := &stubProxy{name: "b", delay: 25, dial: businessExplorationStubDial}
	m := &C.Metadata{Host: "d", NetWork: C.TCP}
	c, e := s.dialBusinessRoute(context.Background(), m, "k", "d", p, "", false)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if best, _ := s.routeTable.GetBestProxy("k", "d"); best != "a" {
		t.Fatal("late initial dial overwrote a newer best")
	}
}

type businessExplorationHeadroomConn struct{ stubConn }

func (c *businessExplorationHeadroomConn) FrontHeadroom() int { return 18 }
func TestBusinessExplorationLifecycleWrapperPreservesProtocolHeadroom(t *testing.T) {
	raw := &businessExplorationHeadroomConn{}
	wrapped := &explorationReservationConn{Conn: raw, release: func() {}}
	if got := N.CalculateFrontHeadroom(wrapped); got != 18 {
		t.Fatalf("lost encryption headroom: %d", got)
	}
}

func TestSmartExploration_AggregatePriorKeepsPlausibleAndUnknownChallengers(t *testing.T) {
	const key, domain = "TARGET:preview.img2.hk-example.test", "preview.img2.hk-example.test"
	s, rt, pc := newBestRaceSmart()
	defer pc.Close()

	rt.UpdateTTFB(key, domain, "hk-best", 800)
	rt.UpdateTTFB("TARGET:www.hk-example.test", "www.hk-example.test", "hk-plausible", 760)
	rt.UpdateTTFB("TARGET:img1.hk-example.test", "img1.hk-example.test", "hk-plausible", 880)

	if !s.explorationWorthRisk(key, domain, "hk-best", "hk-plausible") {
		t.Fatal("plausibly better challenger rejected")
	}
	if !s.explorationWorthRisk(key, domain, "hk-best", "never-seen") {
		t.Fatal("fully unknown challenger rejected")
	}
}

func TestSmartExploration_AggregatePriorRejectsHighDamageChallenger(t *testing.T) {
	const key, domain = "TARGET:preview.img2.hk-example.test", "preview.img2.hk-example.test"
	s, rt, pc := newBestRaceSmart()
	defer pc.Close()

	rt.UpdateTTFB(key, domain, "hk-best", 800)
	// The challenger is untested for this target, but consistently slow on
	// other targets. Its optimistic estimate remains well behind the best.
	rt.UpdateTTFB("TARGET:www.hk-example.test", "www.hk-example.test", "eu-risky", 1700)
	rt.UpdateTTFB("TARGET:img1.hk-example.test", "img1.hk-example.test", "eu-risky", 1800)
	rt.UpdateTTFB("TARGET:img2.hk-example.test", "img2.hk-example.test", "eu-risky", 1900)

	if s.explorationWorthRisk(key, domain, "hk-best", "eu-risky") {
		t.Fatal("high-damage challenger accepted despite strong aggregate prior")
	}
}

func TestSmartExploration_SharedFailuresDiscountGain(t *testing.T) {
	for _, tc := range []struct {
		name      string
		low, high int64
		failures  float64
	}{
		// Gain falls below the risk while remaining above the 25ms minimum.
		{"future-gain", 200, 2000, 4},
		// Enough future demand covers the risk, but discounted gain is <25ms.
		{"minimum-gain", 600, 2000, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const key, domain = "current", "img.example.com"
			s, rt, pc := newBestRaceSmart()
			defer pc.Close()
			rt.UpdateTTFB(key, domain, "best", 800)
			rt.UpdateTTFB("ASN:1", "a.example.com", "challenger", tc.low)
			rt.UpdateTTFB("ASN:2", "b.example.com", "challenger", tc.high)
			if tc.name == "minimum-gain" {
				for i := 0; i < 8; i++ {
					rt.IncrementUseCount("ASN:1", "a.example.com", "challenger")
				}
			}
			if !s.explorationWorthRisk(key, domain, "best", "challenger") {
				t.Fatal("plausible challenger rejected before shared failures")
			}
			rt.MarkFailed("ASN:1", "challenger", "a.example.com", tc.failures)
			rt.MarkFailed("ASN:2", "challenger", "b.example.com", tc.failures)
			if s.explorationWorthRisk(key, domain, "best", "challenger") {
				t.Fatal("shared failures did not discount exploration gain")
			}
		})
	}
}

func TestSmartExploration_SharedFailuresPreserveExistingAllowPaths(t *testing.T) {
	const key, domain = "current", "img.example.com"
	s, rt, pc := newBestRaceSmart()
	defer pc.Close()
	rt.UpdateTTFB(key, domain, "best", 800)
	rt.UpdateTTFB("sibling", "www.example.com", "low-damage", 900)
	rt.MarkFailed("sibling", "low-damage", "www.example.com", 10)
	rt.MarkFailed("sibling", "failure-only", "www.example.com", 10)
	for _, proxy := range []string{"low-damage", "failure-only", "unknown"} {
		if !s.explorationWorthRisk(key, domain, "best", proxy) {
			t.Fatalf("existing allow path changed for %s", proxy)
		}
	}
}
