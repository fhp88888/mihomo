package smart

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

// clearOUStore resets the in-memory OU state store between tests.
func clearOUStore() {
	ouStateStoreMu.Lock()
	defer ouStateStoreMu.Unlock()
	ouStateStore = make(map[string]*CellState)
}

// ── TSScore ──────────────────────────────────────────────────────────

func TestTSScore_ConfidentGood(t *testing.T) {
	prior := DefaultPrior()
	now := time.Now().Unix()
	st := NewCellState(now, prior)
	// Very confident this is a good node.
	st.MuS = 5.0 // σ(5) ≈ 0.993
	st.PS = 0.01
	st.MuR = 3.0
	st.PR = 0.01
	st.MuT = 0.0
	st.PT = 1.0

	rng := rand.New(rand.NewSource(42))
	score := TSScore(st, prior, 0.5, 0.0, -0.1, rng)

	// p ≈ 0.993, q ≈ 0.5 * g(3) ≈ 0.5 * 0.95 ≈ 0.475
	// U ≈ 0.993 * 0.475 + 0.007 * (-0.1) ≈ 0.471
	if score < 0.3 || score > 0.7 {
		t.Errorf("confident good node: got score %.4f, expected ~0.47", score)
	}
}

func TestTSScore_ConfidentBad(t *testing.T) {
	prior := DefaultPrior()
	now := time.Now().Unix()
	st := NewCellState(now, prior)
	// Very confident this is a bad node.
	st.MuS = -5.0 // σ(-5) ≈ 0.007
	st.PS = 0.01
	st.MuR = -3.0
	st.PR = 0.01

	rng := rand.New(rand.NewSource(42))
	score := TSScore(st, prior, 0.5, 0.0, -0.1, rng)

	// p ≈ 0.007, q ≈ 0.5 * g(-3) ≈ 0.5 * 0.05 ≈ 0.025
	// U ≈ 0.007 * 0.025 + 0.993 * (-0.1) ≈ -0.099
	if score > -0.05 {
		t.Errorf("confident bad node: got score %.4f, expected negative", score)
	}
}

func TestTSScore_UncertainNode(t *testing.T) {
	prior := DefaultPrior()
	now := time.Now().Unix()
	st := NewCellState(now, prior)
	// High uncertainty — the TS sample can go either way.
	st.MuS = 0.0 // σ(0) = 0.5
	st.PS = 4.0  // large variance
	st.MuR = 0.0
	st.PR = 1.0

	// Run many times; scores should vary.
	scores := make([]float64, 100)
	for i := 0; i < 100; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		scores[i] = TSScore(st, prior, 0.5, 0.0, -0.1, rng)
	}

	// Check there is variance.
	min, max := scores[0], scores[0]
	for _, s := range scores {
		if s < min {
			min = s
		}
		if s > max {
			max = s
		}
	}
	if max-min < 0.01 {
		t.Errorf("uncertain node scores should vary: min=%.4f max=%.4f", min, max)
	}
}

func TestTSScore_WtZero(t *testing.T) {
	prior := DefaultPrior()
	now := time.Now().Unix()
	st := NewCellState(now, prior)
	st.MuS = 2.0
	st.PS = 0.1
	st.MuR = 2.0
	st.PR = 0.1
	st.MuT = 5.0 // excellent transfer, but wt=0 means ignored
	st.PT = 0.01

	// With wt=0, only wr matters.
	rng := rand.New(rand.NewSource(123))
	scoreWt0 := TSScore(st, prior, 0.5, 0.0, -0.1, rng)

	// With wt=0.3, transfer matters.
	rng = rand.New(rand.NewSource(123)) // same seed
	scoreWt03 := TSScore(st, prior, 0.5, 0.3, -0.1, rng)

	// Same seeds should produce same zs, zr, but different q.
	// q(wt=0.3) > q(wt=0) because MuT is high → score should be higher.
	if scoreWt03 <= scoreWt0 {
		t.Errorf("higher wt should increase score when MuT is good: %.4f <= %.4f", scoreWt03, scoreWt0)
	}
}

func TestTSScore_UfailEffect(t *testing.T) {
	prior := DefaultPrior()
	now := time.Now().Unix()
	st := NewCellState(now, prior)
	// Low success probability → Ufail dominates.
	st.MuS = -3.0
	st.PS = 0.01
	st.MuR = 0.0
	st.PR = 1.0

	rng := rand.New(rand.NewSource(99))
	score01 := TSScore(st, prior, 0.5, 0.0, -0.1, rng)

	rng = rand.New(rand.NewSource(99))
	score05 := TSScore(st, prior, 0.5, 0.0, -0.5, rng)

	// More negative Ufail → lower score for low-p nodes.
	if score05 >= score01 {
		t.Errorf("more negative Ufail should lower score: %.4f >= %.4f", score05, score01)
	}
}

// ── TSRank ───────────────────────────────────────────────────────────

func TestTSRank_Sorting(t *testing.T) {
	prior := DefaultPrior()
	now := time.Now().Unix()

	good := NewCellState(now, prior)
	good.MuS = 3.0
	good.PS = 0.01
	good.MuR = 2.0
	good.PR = 0.01

	bad := NewCellState(now, prior)
	bad.MuS = -3.0
	bad.PS = 0.01
	bad.MuR = -2.0
	bad.PR = 0.01

	states := map[string]*CellState{"good": good, "bad": bad}
	rng := rand.New(rand.NewSource(77))
	ranked := TSRank(states, prior, 0.5, 0.0, -0.1, rng)

	if len(ranked) != 2 {
		t.Fatalf("expected 2 results, got %d", len(ranked))
	}
	if ranked[0].Node != "good" {
		t.Errorf("good should rank first, got %s", ranked[0].Node)
	}
	if ranked[1].Node != "bad" {
		t.Errorf("bad should rank second, got %s", ranked[1].Node)
	}
	if ranked[0].Weight <= ranked[1].Weight {
		t.Errorf("good score should be > bad: %.4f <= %.4f", ranked[0].Weight, ranked[1].Weight)
	}
}

func TestTSRank_Deterministic(t *testing.T) {
	prior := DefaultPrior()
	now := time.Now().Unix()
	st := NewCellState(now, prior)
	st.MuS = 1.0
	st.PS = 0.5
	st.MuR = 0.5
	st.PR = 0.3
	states := map[string]*CellState{"a": st}

	rng1 := rand.New(rand.NewSource(42))
	r1 := TSRank(states, prior, 0.5, 0.0, -0.1, rng1)

	rng2 := rand.New(rand.NewSource(42))
	r2 := TSRank(states, prior, 0.5, 0.0, -0.1, rng2)

	if r1[0].Weight != r2[0].Weight {
		t.Errorf("same seed should give same score: %.6f != %.6f", r1[0].Weight, r2[0].Weight)
	}
}

// ── GetOrCreateOUState ───────────────────────────────────────────────

func TestGetOrCreateOUState_CreatesNew(t *testing.T) {
	clearOUStore()
	s := &Store{}
	prior := DefaultPrior()

	st := s.GetOrCreateOUState("g", "c", "node1", "", false, prior)
	if st == nil {
		t.Fatal("expected non-nil state")
	}
	if st.PS != prior.VfS*4.0 {
		t.Errorf("new state should have inflated variance: %.4f", st.PS)
	}
}

func TestGetOrCreateOUState_ReturnsExisting(t *testing.T) {
	clearOUStore()
	s := &Store{}
	prior := DefaultPrior()

	st1 := s.GetOrCreateOUState("g", "c", "node1", "", false, prior)
	st1.MuS = 99.0 // modify in place

	st2 := s.GetOrCreateOUState("g", "c", "node1", "", false, prior)
	if st2.MuS != 99.0 {
		t.Errorf("second call should return same state: MuS=%.4f", st2.MuS)
	}
}

func TestGetOrCreateOUState_DifferentCells(t *testing.T) {
	clearOUStore()
	s := &Store{}
	prior := DefaultPrior()

	stTCP := s.GetOrCreateOUState("g", "c", "n", "", false, prior)
	stUDP := s.GetOrCreateOUState("g", "c", "n", "", true, prior)
	stASN := s.GetOrCreateOUState("g", "c", "n", "AS123", false, prior)

	// Different cells should be independent.
	stTCP.MuS = 1.0
	stUDP.MuS = 2.0
	stASN.MuS = 3.0

	if s.GetOrCreateOUState("g", "c", "n", "", false, prior).MuS != 1.0 {
		t.Error("TCP cell modified")
	}
	if s.GetOrCreateOUState("g", "c", "n", "", true, prior).MuS != 2.0 {
		t.Error("UDP cell modified")
	}
	if s.GetOrCreateOUState("g", "c", "n", "AS123", false, prior).MuS != 3.0 {
		t.Error("ASN cell modified")
	}
}

// ── UpdateOUState ────────────────────────────────────────────────────

func TestUpdateOUState_AppliesObservation(t *testing.T) {
	clearOUStore()
	s := &Store{}
	prior := DefaultPrior()

	obs := ObsResult{HasS: true, S: true}
	s.UpdateOUState("g", "c", "n", "", false, prior, obs)

	st := s.GetOrCreateOUState("g", "c", "n", "", false, prior)
	// Success should have increased MuS.
	if st.MuS <= 0.0 {
		t.Errorf("success should increase MuS: got %.4f", st.MuS)
	}
}

// ── GetTSProxyRankingForTarget ───────────────────────────────────────

func TestGetTSProxyRankingForTarget_Basic(t *testing.T) {
	clearOUStore()
	s := &Store{}

	// Pre-populate one good and one bad node.
	prior := DefaultPrior()
	good := s.GetOrCreateOUState("g", "c", "good", "", false, prior)
	good.MuS = 3.0
	good.PS = 0.01
	good.MuR = 2.0
	good.PR = 0.01

	bad := s.GetOrCreateOUState("g", "c", "bad", "", false, prior)
	bad.MuS = -3.0
	bad.PS = 0.01
	bad.MuR = -2.0
	bad.PR = 0.01

	nodes, scores, err := s.GetTSProxyRankingForTarget("g", "c", "google.com", "", false,
		[]string{"good", "bad"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] != "good" {
		t.Errorf("good should rank first, got %s", nodes[0])
	}
	if scores[0] <= scores[1] {
		t.Errorf("good score should exceed bad: %.4f <= %.4f", scores[0], scores[1])
	}
}

func TestGetTSProxyRankingForTarget_Empty(t *testing.T) {
	clearOUStore()
	s := &Store{}

	nodes, scores, err := s.GetTSProxyRankingForTarget("g", "c", "t", "", false, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Errorf("expected nil nodes, got %v", nodes)
	}
	if scores != nil {
		t.Errorf("expected nil scores, got %v", scores)
	}
}

func TestGetTSProxyRankingForTarget_ASNHierarchical(t *testing.T) {
	clearOUStore()
	s := &Store{}
	prior := DefaultPrior()

	// Pre-train the parent state with one success.
	parent := s.GetOrCreateOUState("g", "c", "node", "", false, prior)
	parent.UpdateSuccess(time.Now().Unix(), prior, true)
	// Parent is now slightly positive.

	// Get ranking with ASN — should create a child derived from parent.
	// Use "AS1234" which is NOT in CdnASNs.
	nodes, scores, err := s.GetTSProxyRankingForTarget("g", "c", "t", "AS1234", false,
		[]string{"node"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	// The child should exist separately.
	child := getOUState("g", "c", "node", "AS1234", false)
	if child == nil {
		t.Fatal("expected child state to be created")
	}
	// Check we didn't modify the parent when scoring.
	if parent.MuS <= 0.0 {
		t.Error("parent should still have positive MuS after one success")
	}
	_ = scores
}

// ── GetStaleNodes ────────────────────────────────────────────────────

func TestGetStaleNodes_NeverObserved(t *testing.T) {
	clearOUStore()
	s := &Store{}

	stale := s.GetStaleNodes("g", "c", "", false, []string{"new_node"})
	if len(stale) != 1 {
		t.Fatalf("unobserved node should be stale: got %v", stale)
	}
	if stale[0] != "new_node" {
		t.Errorf("expected new_node, got %s", stale[0])
	}
}

func TestGetStaleNodes_RecentlyUpdated(t *testing.T) {
	clearOUStore()
	s := &Store{}
	prior := DefaultPrior()

	st := s.GetOrCreateOUState("g", "c", "active", "", false, prior)
	st.LastUpdateTime = time.Now().Unix() // just now

	stale := s.GetStaleNodes("g", "c", "", false, []string{"active"})
	if len(stale) != 0 {
		t.Errorf("recently updated node should not be stale: got %v", stale)
	}
}

func TestGetStaleNodes_OldUpdate(t *testing.T) {
	clearOUStore()
	s := &Store{}
	prior := DefaultPrior()

	st := s.GetOrCreateOUState("g", "c", "old", "", false, prior)
	// Set last update to 4*H hours ago (> 2*H threshold).
	hoursAgo := int64(4.0 * prior.H * 3600)
	st.LastUpdateTime = time.Now().Unix() - hoursAgo

	stale := s.GetStaleNodes("g", "c", "", false, []string{"old"})
	if len(stale) != 1 {
		t.Errorf("old node should be stale: got %v", stale)
	}
}

// ── sampleNormal ─────────────────────────────────────────────────────

func TestSampleNormal_ZeroStd(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	for i := 0; i < 10; i++ {
		val := sampleNormal(5.0, 0.0, rng)
		if val != 5.0 {
			t.Errorf("zero std should return mean: got %.6f", val)
		}
	}
}

func TestSampleNormal_Distribution(t *testing.T) {
	rng := rand.New(rand.NewSource(123))
	n := 10000
	sum := 0.0
	sumSq := 0.0
	for i := 0; i < n; i++ {
		val := sampleNormal(3.0, 2.0, rng)
		sum += val
		sumSq += val * val
	}
	mean := sum / float64(n)
	std := math.Sqrt(sumSq/float64(n) - mean*mean)

	if math.Abs(mean-3.0) > 0.1 {
		t.Errorf("sample mean should be ~3.0: got %.4f", mean)
	}
	if math.Abs(std-2.0) > 0.1 {
		t.Errorf("sample std should be ~2.0: got %.4f", std)
	}
}
