package smart

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

// ── S (success / failure) ────────────────────────────────────────────

func TestExtractSuccess_GoodConnection(t *testing.T) {
	hasS, S := extractSuccess(nil, 50, 30, testTimeout)
	assertBool(t, hasS, true, "hasS")
	assertBool(t, S, true, "S")
}

func TestExtractSuccess_ConnectTimeout(t *testing.T) {
	// DeadlineExceeded without any response.
	hasS, S := extractSuccess(context.DeadlineExceeded, 0, 0, testTimeout)
	assertBool(t, hasS, true, "hasS")
	assertBool(t, S, false, "S")
}

func TestExtractSuccess_ConnectionRefused(t *testing.T) {
	hasS, S := extractSuccess(errors.New("connection refused"), 0, 0, testTimeout)
	assertBool(t, hasS, true, "hasS")
	assertBool(t, S, false, "S")
}

func TestExtractSuccess_Cancelled(t *testing.T) {
	// Race-cancelled — should be treated as MISSING, not failure.
	hasS, _ := extractSuccess(context.Canceled, 0, 0, testTimeout)
	assertBool(t, hasS, false, "hasS — cancelled is missing")
}

func TestExtractSuccess_NoLatency(t *testing.T) {
	// No error but zero latency — treated as failure (no data received).
	hasS, S := extractSuccess(nil, 50, 0, testTimeout)
	assertBool(t, hasS, true, "hasS")
	assertBool(t, S, false, "S")
}

// ── Yr (response quality) ───────────────────────────────────────────

func TestExtractResponseQuality_Fast(t *testing.T) {
	yr := extractResponseQuality(20, 30, testTimeout) // 50ms total, 5000ms ref
	// Score: 1 - 50/5000 = 0.99
	// logit(0.99) = ln(0.99/0.01) = ln(99) ≈ 4.595
	if yr < 4.0 {
		t.Errorf("fast response (50ms) should give high yr: got %.4f", yr)
	}
}

func TestExtractResponseQuality_Slow(t *testing.T) {
	yr := extractResponseQuality(2000, 2000, testTimeout) // 4000ms total
	// Score: 1 - 4000/5000 = 0.2
	// logit(0.2) = ln(0.2/0.8) = ln(0.25) = -1.386
	if yr > 0.0 {
		t.Errorf("slow response (4000ms) should give negative yr: got %.4f", yr)
	}
}

func TestExtractResponseQuality_Monotonic(t *testing.T) {
	// Faster responses should produce strictly higher yr.
	prev := math.Inf(1)
	for _, ms := range []int64{10, 50, 100, 500, 1000, 2000, 4000} {
		yr := extractResponseQuality(ms/2, ms/2, testTimeout)
		if yr >= prev {
			t.Errorf("yr not monotonic: %dms total → %.4f >= prev %.4f", ms, yr, prev)
		}
		prev = yr
	}
}

func TestExtractResponseQuality_Clamped(t *testing.T) {
	// Zero-time response (impossible in practice, but tests clamping).
	yr := extractResponseQuality(0, 0, testTimeout)
	if math.IsInf(yr, 0) || math.IsNaN(yr) {
		t.Errorf("yr should be finite: got %v", yr)
	}
	// Score = 1.0 → clamped to 1-eps → logit(1-eps) = large but finite.
	if yr < 10.0 {
		t.Errorf("instant response should give very high yr: got %.4f", yr)
	}
}

// ── Yt (transfer quality) ───────────────────────────────────────────

func TestExtractTransferQuality_HighThroughput(t *testing.T) {
	// 80 MB/s download + 20 MB/s upload = 100 MB/s
	yt := extractTransferQuality(80, 20)
	if yt < 0.0 {
		t.Errorf("high throughput should give positive yt: got %.4f", yt)
	}
}

func TestExtractTransferQuality_LowThroughput(t *testing.T) {
	// 0.005 MB/s download + 0.005 MB/s upload = 0.01 MB/s
	yt := extractTransferQuality(0.005, 0.005)
	if yt > 0.0 {
		t.Errorf("low throughput should give negative yt: got %.4f", yt)
	}
}

func TestExtractTransferQuality_Monotonic(t *testing.T) {
	prev := -math.MaxFloat64
	for _, mbps := range []float64{0.1, 0.5, 1, 5, 10, 50, 100} {
		// throughput = download + upload = mbps
		yt := extractTransferQuality(mbps*0.7, mbps*0.3)
		if yt < prev {
			t.Errorf("yt not monotonic: %.1f MB/s → %.4f < prev %.4f", mbps, yt, prev)
		}
		prev = yt
	}
}

// ── ExtractObservations (full pipeline) ──────────────────────────────

func TestExtractObservations_BanditSample_Success(t *testing.T) {
	r := ExtractObservations(
		nil,        // no error
		30,         // connectTime ms
		20,         // latency ms
		1.0, 0.5,   // download/upload MB
		5000,        // connectionDur ms
		0, 0,        // max download/upload rate MB/s
		testTimeout,
	)

	assertBool(t, r.HasS, true, "HasS")
	assertBool(t, r.S, true, "S")
	assertBool(t, r.HasR, true, "HasR")
	// Yr should be positive (fast response: 50ms total)
	if r.Yr <= 0 {
		t.Errorf("Yr should be positive for fast response: %.4f", r.Yr)
	}
	// Yt should be present (1.5MB in 5s > minTrafficMB threshold)
	assertBool(t, r.HasT, true, "HasT")
}

func TestExtractObservations_BanditSample_Failure(t *testing.T) {
	r := ExtractObservations(
		context.DeadlineExceeded,
		0, 0, 0, 0, 0,
		0, 0,
		testTimeout,
	)

	assertBool(t, r.HasS, true, "HasS")
	assertBool(t, r.S, false, "S")
	assertBool(t, r.HasR, false, "HasR — no response, no Yr")
	assertBool(t, r.HasT, false, "HasT — no traffic, no Yt")
}

func TestExtractObservations_AlwaysExtracts(t *testing.T) {
	// Every connection that reaches recordConnectionStats gets observations
	// extracted, regardless of bandit eligibility.
	r := ExtractObservations(
		nil, 10, 5, 100, 50, 30000,
		0, 0,
		testTimeout,
	)

	// Successful connection with latency and traffic → all present.
	assertBool(t, r.HasS, true, "HasS")
	assertBool(t, r.S, true, "S")
	assertBool(t, r.HasR, true, "HasR")
	assertBool(t, r.HasT, true, "HasT — enough traffic")
	assertBool(t, r.Observed(), true, "Observed")
}

func TestExtractObservations_Cancelled_BanditSample(t *testing.T) {
	// Even if isBanditSample=true, a context.Canceled error means
	// the observation is missing (raced out).
	r := ExtractObservations(
		context.Canceled,
		0, 0, 0, 0, 0,
		0, 0,
		testTimeout,
	)

	assertBool(t, r.HasS, false, "HasS — cancelled is always missing")
	assertBool(t, r.Observed(), false, "Observed")
}

func TestExtractObservations_NoTrafficThreshold(t *testing.T) {
	// Successful but tiny traffic: Yt should NOT be present.
	r := ExtractObservations(
		nil, 30, 20,
		0.01, 0.01, // only 0.02 MB (< minTrafficMB)
		100,
		0, 0,
		testTimeout,
	)

	assertBool(t, r.HasS, true, "HasS")
	assertBool(t, r.S, true, "S")
	assertBool(t, r.HasR, true, "HasR")
	assertBool(t, r.HasT, false, "HasT — below traffic threshold")
}

func TestExtractObservations_ShortDuration_NoTraffic(t *testing.T) {
	// Enough traffic but too short: Yt should NOT be present.
	r := ExtractObservations(
		nil, 30, 20,
		0.1, 0.1, // 0.2 MB (≥ threshold)
		200,       // 200ms (< minTrafficDuration)
		0, 0,
		testTimeout,
	)

	assertBool(t, r.HasT, false, "HasT — below duration threshold")
}

// ── Observed ─────────────────────────────────────────────────────────

func TestObsResult_Observed(t *testing.T) {
	assertBool(t, ObsResult{}.Observed(), false, "empty → not observed")
	assertBool(t, ObsResult{HasS: true}.Observed(), true, "S present → observed")
	assertBool(t, ObsResult{HasR: true}.Observed(), true, "Yr present → observed")
	assertBool(t, ObsResult{HasS: true, HasR: true, HasT: true}.Observed(), true, "all present → observed")
}

// ── IsBanditSample ───────────────────────────────────────────────────

func TestIsBanditSample(t *testing.T) {
	assertBool(t, IsBanditSample(true, false), true, "first attempt")
	assertBool(t, IsBanditSample(false, true), true, "stale probe")
	assertBool(t, IsBanditSample(true, true), true, "both")
	assertBool(t, IsBanditSample(false, false), false, "neither")
}
