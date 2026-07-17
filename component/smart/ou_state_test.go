package smart

import (
	"math"
	"testing"
)

// ── Helpers ──────────────────────────────────────────────────────────

func assertFloat(t *testing.T, got, want, tol float64, label string) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s: got %.8f, want %.8f (diff %.2e)", label, got, want, got-want)
	}
}

func assertBool(t *testing.T, got, want bool, label string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v, want %v", label, got, want)
	}
}

// ── PredictToNow ─────────────────────────────────────────────────────

func TestPredictToNow_NoElapsedTime(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(1000, prior)
	// Modify state so we can detect a no-op.
	cs.MuS = 1.0
	cs.PS = 2.0

	cs.PredictToNow(1000, prior) // d = 0

	assertFloat(t, cs.MuS, 1.0, 1e-10, "MuS unchanged")
	assertFloat(t, cs.PS, 2.0, 1e-10, "PS unchanged")
}

func TestPredictToNow_ConvergesToPrior(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)
	cs.MuS = 5.0 // far from prior mean (0)
	cs.PS = 10.0

	// Advance a very long time (1000 * H)
	hugeDelta := int64(prior.H * 3600 * 1000)
	cs.PredictToNow(hugeDelta, prior)

	// Mean should be very close to prior.MS
	assertFloat(t, cs.MuS, prior.MS, 1e-6, "MuS → prior.MS")

	// Variance should be very close to VfS
	assertFloat(t, cs.PS, prior.VfS, 1e-6, "PS → VfS")
}

func TestPredictToNow_HalfLife(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)
	cs.MuS = 2.0
	cs.PS = 0.5

	// Advance exactly H hours
	hSec := int64(prior.H * 3600)
	cs.PredictToNow(hSec, prior)

	// After one half-life, the mean is halfway to prior.
	mid := prior.MS + 0.5*(2.0-prior.MS)
	assertFloat(t, cs.MuS, mid, 1e-6, "MuS halfway to prior")

	// Variance: rho = exp(-ln(2)) = 0.5, rho² = 0.25
	// P_ = 0.25*0.5 + 1.0*0.75 = 0.125 + 0.75 = 0.875
	expectedP := 0.25*0.5 + prior.VfS*0.75
	assertFloat(t, cs.PS, expectedP, 1e-6, "PS after half-life")
}

func TestPredictToNow_PNeverBelowMin(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)
	cs.PS = minP + 0.01

	// Advance so the deterministic part of P drops below minP.
	cs.PredictToNow(1, prior)
	if cs.PS < minP {
		t.Errorf("PS = %.8f below minP = %.8f", cs.PS, minP)
	}
}

// ── UpdateSuccess (Laplace approximation) ────────────────────────────

func TestUpdateSuccess_SuccessIncreasesMean(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)
	initialMu := cs.MuS // 0

	cs.UpdateSuccess(1, prior, true)

	if cs.MuS <= initialMu {
		t.Errorf("MuS should increase after success: got %.6f, was %.6f", cs.MuS, initialMu)
	}
	if cs.PS >= 4.0*prior.VfS {
		t.Errorf("PS should decrease after observation: got %.6f, init %.6f", cs.PS, 4.0*prior.VfS)
	}
}

func TestUpdateSuccess_FailureDecreasesMean(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)
	initialMu := cs.MuS // 0

	cs.UpdateSuccess(1, prior, false)

	if cs.MuS >= initialMu {
		t.Errorf("MuS should decrease after failure: got %.6f, was %.6f", cs.MuS, initialMu)
	}
}

func TestUpdateSuccess_VarianceDecreases(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)
	initialP := cs.PS

	cs.UpdateSuccess(1, prior, true)

	if cs.PS >= initialP {
		t.Errorf("PS should decrease after observation: got %.6f, init %.6f", cs.PS, initialP)
	}
}

func TestUpdateSuccess_ManySuccesses(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	for i := 0; i < 20; i++ {
		cs.UpdateSuccess(int64(i+1), prior, true)
	}

	// After many successes, mu should be strongly positive.
	if cs.MuS < 2.0 {
		t.Errorf("MuS should be large positive after 20 successes: got %.6f", cs.MuS)
	}
	// P should have shrunk from its initial 4.0.
	if cs.PS >= 4.0 {
		t.Errorf("PS should have decreased from 4.0: got %.6f", cs.PS)
	}
}

func TestUpdateSuccess_MixedObservations(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	// 10 successes, 10 failures
	for i := 0; i < 10; i++ {
		cs.UpdateSuccess(int64(i*2+1), prior, true)
		cs.UpdateSuccess(int64(i*2+2), prior, false)
	}

	// Should stay near 0 (uninformative)
	assertFloat(t, cs.MuS, 0.0, 0.5, "MuS near 0 with balanced obs")
	// P should be small (we have evidence)
	if cs.PS > 1.0 {
		t.Errorf("PS should shrink with evidence: got %.6f", cs.PS)
	}
}

// ── UpdateResponse / UpdateTransfer (Kalman) ────────────────────────

func TestUpdateResponse_PositiveObservation(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	cs.UpdateResponse(1, prior, 2.0) // positive logit → good response

	if cs.MuR <= 0.0 {
		t.Errorf("MuR should increase after positive observation: got %.6f", cs.MuR)
	}
	if cs.PR >= 4.0*prior.VfR {
		t.Errorf("PR should decrease: got %.6f, init %.6f", cs.PR, 4.0*prior.VfR)
	}
}

func TestUpdateResponse_NegativeObservation(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	cs.UpdateResponse(1, prior, -2.0)

	if cs.MuR >= 0.0 {
		t.Errorf("MuR should decrease after negative observation: got %.6f", cs.MuR)
	}
}

func TestUpdateResponse_VarianceShrinks(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)
	initialPR := cs.PR

	cs.UpdateResponse(1, prior, 0.5)

	if cs.PR >= initialPR {
		t.Errorf("PR should shrink: got %.6f, init %.6f", cs.PR, initialPR)
	}
}

func TestUpdateTransfer_SameAsResponse(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	cs.UpdateTransfer(1, prior, 1.0)

	// Same Kalman math as response, just different head.
	if cs.MuT <= 0.0 {
		t.Errorf("MuT should increase: got %.6f", cs.MuT)
	}
}

// ── ApplyObservation ─────────────────────────────────────────────────

func TestApplyObservation_InOrder(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	ev1 := CellEvent{Time: 1, HasS: true, S: true}
	ev2 := CellEvent{Time: 2, HasS: true, S: true}

	cs.ApplyObservation(2, prior, ev1)
	cs.ApplyObservation(2, prior, ev2)

	// Two successes should push MuS positive.
	if cs.MuS <= 1.0 {
		t.Errorf("MuS should be clearly positive after 2 successes: got %.6f", cs.MuS)
	}
}

func TestApplyObservation_OutOfOrder_Fallback(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	// Apply an event at time 10 first.
	cs.ApplyObservation(10, prior, CellEvent{Time: 10, HasS: true, S: true})
	muAfter := cs.MuS

	// Now apply an out-of-order event at time 5 (before checkpoint).
	// This should hit the fallback path.
	cs.ApplyObservation(10, prior, CellEvent{Time: 5, HasS: true, S: false})

	// The state should have been modified (fallback applied).
	// We just check it doesn't panic and produces finite values.
	if math.IsNaN(cs.MuS) || math.IsInf(cs.MuS, 0) {
		t.Errorf("MuS is NaN/Inf after out-of-order event")
	}
	_ = muAfter
}

func TestApplyObservation_RecordsEventsForReplay(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	cs.ApplyObservation(10, prior, CellEvent{Time: 1, HasS: true, S: true})
	cs.ApplyObservation(10, prior, CellEvent{Time: 5, HasS: true, S: true})

	if len(cs.EventsAfterCP) != 2 {
		t.Fatalf("expected 2 replay events, got %d", len(cs.EventsAfterCP))
	}
	if cs.EventsAfterCP[0].Time != 1 || cs.EventsAfterCP[1].Time != 5 {
		t.Fatalf("events not recorded in order: %#v", cs.EventsAfterCP)
	}
}

func TestApplyObservation_OutOfOrder_Replay(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	// Build a sequence with checkpoints.
	cs.ApplyObservation(10, prior, CellEvent{Time: 1, HasS: true, S: true})  // sets cp
	cs.setCheckpoint(1)
	cs.EventsAfterCP = append(cs.EventsAfterCP, CellEvent{Time: 5, HasS: true, S: true})
	cs.LastUpdateTime = 10
	cs.MuS = 3.0 // simulate state after time 5

	// Out-of-order event at time 3 (between cp=1 and event at 5).
	cs.ApplyObservation(10, prior, CellEvent{Time: 3, HasS: true, S: false})

	// Just verify no NaN/Inf.
	if math.IsNaN(cs.MuS) || math.IsInf(cs.MuS, 0) {
		t.Errorf("MuS is NaN/Inf after replay")
	}
}

// ── SigmaGaussHermite ────────────────────────────────────────────────

func TestSigmaGaussHermite_Noiseless(t *testing.T) {
	// With vn=0, g(z) ≡ σ(z).
	for _, z := range []float64{-5, -2, -1, 0, 1, 2, 5} {
		got := SigmaGaussHermite(z, 0)
		want := sigmoid(z)
		assertFloat(t, got, want, 1e-10, "g(z,0) = σ(z)")
	}
}

func TestSigmaGaussHermite_Symmetry(t *testing.T) {
	// g(-z, vn) = 1 - g(z, vn) because σ(-x)=1-σ(x) and GH is symmetric.
	for _, z := range []float64{0.5, 1.0, 2.0, 3.0} {
		for _, vn := range []float64{0.1, 0.25, 0.5, 1.0} {
			gPos := SigmaGaussHermite(z, vn)
			gNeg := SigmaGaussHermite(-z, vn)
			assertFloat(t, gPos+gNeg, 1.0, 1e-10, "symmetry g(z)+g(-z)=1")
		}
	}
}

func TestSigmaGaussHermite_Monotonic(t *testing.T) {
	vn := 0.5
	prev := -1.0
	for z := -5.0; z <= 5.0; z += 0.2 {
		val := SigmaGaussHermite(z, vn)
		if val < prev {
			t.Errorf("g(z, vn) not monotonic at z=%.1f: %.8f < prev %.8f", z, val, prev)
			break
		}
		prev = val
	}
}

func TestSigmaGaussHermite_Bounds(t *testing.T) {
	for _, vn := range []float64{0.1, 1.0, 10.0} {
		for _, z := range []float64{-10, -1, 0, 1, 10} {
			val := SigmaGaussHermite(z, vn)
			if val < 0 || val > 1 {
				t.Errorf("g(%.1f, %.1f) = %.8f out of [0,1]", z, vn, val)
			}
		}
	}
}

func TestSigmaGaussHermite_NoiseEffect(t *testing.T) {
	// At z=0, the result should be 0.5 regardless of noise (symmetry).
	assertFloat(t, SigmaGaussHermite(0, 0.1), 0.5, 1e-10, "g(0,0.1)=0.5")
	assertFloat(t, SigmaGaussHermite(0, 1.0), 0.5, 1e-10, "g(0,1.0)=0.5")
	assertFloat(t, SigmaGaussHermite(0, 5.0), 0.5, 1e-10, "g(0,5.0)=0.5")

	// Higher noise pulls values toward 0.5.
	g05 := SigmaGaussHermite(2.0, 0.5)
	g10 := SigmaGaussHermite(2.0, 1.0)
	if g10 >= g05 {
		t.Errorf("higher vn should pull toward 0.5: g(z=2,vn=0.5)=%.6f, g(z=2,vn=1.0)=%.6f", g05, g10)
	}
}

// ── Logit / InverseLogit ─────────────────────────────────────────────

func TestLogitRoundTrip(t *testing.T) {
	for _, p := range []float64{0.01, 0.1, 0.3, 0.5, 0.7, 0.9, 0.99} {
		z := Logit(p)
		recovered := InverseLogit(z)
		assertFloat(t, recovered, p, 1e-10, "logit round-trip")
	}
}

func TestLogitClamping(t *testing.T) {
	// Values outside [eps, 1-eps] are clamped.
	z0 := Logit(0)
	z1 := Logit(1)
	if math.IsInf(z0, -1) || math.IsInf(z0, 1) {
		t.Error("Logit(0) should be clamped, not infinite")
	}
	if math.IsInf(z1, -1) || math.IsInf(z1, 1) {
		t.Error("Logit(1) should be clamped, not infinite")
	}
}

// ── DerivedPrior ─────────────────────────────────────────────────────

func TestDerivedPrior_InheritsMean(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)
	cs.MuS = 1.5
	cs.MuR = 0.8
	cs.MuT = -0.3

	childP := cs.DerivedPrior(2.0)

	assertFloat(t, childP.MS, 1.5, 1e-10, "child inherits MuS")
	assertFloat(t, childP.MR, 0.8, 1e-10, "child inherits MuR")
	assertFloat(t, childP.MT, -0.3, 1e-10, "child inherits MuT")
}

func TestDerivedPrior_InflatedVariance(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)
	cs.PS = 0.5
	cs.PR = 0.3

	childP := cs.DerivedPrior(2.0)

	if childP.VfS < 0.5*2.0*0.99 { // allow small rounding
		t.Errorf("VfS should be inflated: got %.6f, parent PS=%.6f", childP.VfS, cs.PS)
	}
	if childP.VfR < 0.3*2.0*0.99 {
		t.Errorf("VfR should be inflated: got %.6f, parent PR=%.6f", childP.VfR, cs.PR)
	}
}

func TestDerivedPrior_MinimumFloor(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)
	// Force tiny parent variances.
	cs.PS = 0.01
	cs.PR = 0.01
	cs.PT = 0.01

	childP := cs.DerivedPrior(1.0) // no inflation

	if childP.VfS < DefaultVfS {
		t.Errorf("VfS should be floored: got %.6f < default %.6f", childP.VfS, DefaultVfS)
	}
}

// ── IsStale ──────────────────────────────────────────────────────────

func TestIsStale_RecentUpdate(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(1000, prior)

	// Just updated — not stale.
	assertBool(t, cs.IsStale(1000, prior), false, "just updated")
}

func TestIsStale_OldUpdate(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	// 3*H hours later (> 2*H threshold).
	staleTime := int64(3.0 * prior.H * 3600)
	assertBool(t, cs.IsStale(staleTime, prior), true, "3H old → stale")
}

func TestIsStale_Boundary(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	// Exactly 2*H hours later.
	boundaryTime := int64(2.0 * prior.H * 3600)
	assertBool(t, cs.IsStale(boundaryTime, prior), false, "exactly 2H → not stale")
}

// ── NewCellState ─────────────────────────────────────────────────────

func TestNewCellState_InitialVariance(t *testing.T) {
	prior := DefaultPrior()
	cs := NewCellState(0, prior)

	assertFloat(t, cs.PS, prior.VfS*4.0, 1e-10, "PS init = 4*VfS")
	assertFloat(t, cs.PR, prior.VfR*4.0, 1e-10, "PR init = 4*VfR")
	assertFloat(t, cs.PT, prior.VfT*4.0, 1e-10, "PT init = 4*VfT")
	assertFloat(t, cs.MuS, prior.MS, 1e-10, "MuS init = prior.MS")
}

// ── DefaultPrior ─────────────────────────────────────────────────────

func TestDefaultPrior_Neutral(t *testing.T) {
	p := DefaultPrior()

	// Neutral prior: logit(0.5) = 0.
	assertFloat(t, p.MS, 0.0, 1e-10, "neutral MS")
	assertFloat(t, p.MR, 0.0, 1e-10, "neutral MR")
	assertFloat(t, p.MT, 0.0, 1e-10, "neutral MT")
	assertFloat(t, p.H, DefaultH, 1e-10, "default H")
}

// ── kalmanUpdate ─────────────────────────────────────────────────────

func TestKalmanUpdate_VarianceReduction(t *testing.T) {
	_, P := kalmanUpdate(0, 1.0, 0.5, 0.5)
	if P >= 1.0 {
		t.Errorf("Kalman should reduce variance: %.6f >= 1.0", P)
	}
}

func TestKalmanUpdate_ExactObservation(t *testing.T) {
	// When Vn → 0, the posterior mean should equal the observation.
	// But Vn is always > 0; test with very small Vn.
	mu, P := kalmanUpdate(0, 1.0, 5.0, 1e-6)
	assertFloat(t, mu, 5.0, 1e-3, "mu ≈ y for tiny Vn")
	// P should be very small.
	if P > 1e-4 {
		t.Errorf("P should be tiny for small Vn: %.8f", P)
	}
}

// ── Sigmoid ──────────────────────────────────────────────────────────

func TestSigmoid_Range(t *testing.T) {
	for _, x := range []float64{-10, -1, 0, 1, 10} {
		s := sigmoid(x)
		if s <= 0 || s >= 1 {
			t.Errorf("sigmoid(%.0f) = %.10f not in (0,1)", x, s)
		}
	}
	// At extreme values, sigmoid rounds to 0 or 1 — this is expected.
	assertFloat(t, sigmoid(100), 1.0, 1e-10, "sigmoid(100) ≈ 1")
	assertFloat(t, sigmoid(-100), 0.0, 1e-10, "sigmoid(-100) ≈ 0")
}

func TestSigmoid_Symmetry(t *testing.T) {
	for _, x := range []float64{0.1, 0.5, 1.0, 2.0, 5.0} {
		pos := sigmoid(x)
		neg := sigmoid(-x)
		assertFloat(t, pos+neg, 1.0, 1e-10, "σ(x)+σ(-x)=1")
	}
}
