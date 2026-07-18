package smart

import (
	"context"
	"errors"
	"math"
	"time"
)

// ────────────────────────────────────────────────────────────
// Observation extraction from raw connection metrics
// ────────────────────────────────────────────────────────────

const (
	// Reference timeout for response-quality normalisation.
	// This is the "hard timeout" from the design doc: a dial that takes
	// this long is considered failed / maximally slow.
	referenceResponseWindow = 5000.0 // ms (= DefaultTCPTimeout)

	// Minimum traffic threshold for transfer-quality observation (Yt).
	// Only connections that transfer at least this much data are eligible.
	minTrafficMB       = 0.05  // MB
	minTrafficDuration = 200.0 // ms
)

// ObsResult bundles the extracted observations for one connection.
// Each observation has a "present" flag; when false the corresponding
// value is meaningless and must NOT be written into the bandit state.
type ObsResult struct {
	HasS bool
	S    bool    // true = success (first usable response before timeout)

	HasR bool
	Yr   float64 // logit of response-quality score, for Kalman update

	HasT bool
	Yt   float64 // logit of transfer-quality score, for Kalman update
}

// ExtractObservations extracts the bandit observations (S, Yr, Yt) from
// the raw metrics of a single connection attempt.
//
// Every connection that reaches recordConnectionStats is a legitimate
// observation (failed dials and winning-connection closes).  Race-lost
// connections are drained without the callback chain and never reach
// this function.
func ExtractObservations(
	err error,
	connectTime int64,
	latency int64,
	downloadTotal float64,
	uploadTotal float64,
	connectionDur int64,
	maxDownloadRate float64,
	maxUploadRate float64,
	hardTimeout time.Duration,
) ObsResult {
	var r ObsResult

	// ── S: success / failure ──────────────────────────────────
	r.HasS, r.S = extractSuccess(err, connectTime, latency, hardTimeout)

	// ── Yr: response quality (only when successful) ───────────
	if r.S && latency > 0 {
		r.HasR = true
		r.Yr = extractResponseQuality(connectTime, latency, hardTimeout)
	}

	// ── Yt: transfer quality (only when enough traffic) ───────
	if downloadTotal+uploadTotal >= minTrafficMB && float64(connectionDur) >= minTrafficDuration {
		r.HasT = true
		r.Yt = extractTransferQuality(maxDownloadRate, maxUploadRate)
	}

	return r
}

// ────────────────────────────────────────────────────────────
// Internal extractors
// ────────────────────────────────────────────────────────────

// extractSuccess determines the binary success S.
//
//	S = 1  – first usable response arrived before hard timeout
//	S = 0  – explicit failure or hard timeout with no response
//	hasS   – false when the observation should be treated as missing
//	         (cancelled, race-dropped, not-first-attempt)
func extractSuccess(err error, connectTime, latency int64, hardTimeout time.Duration) (hasS bool, S bool) {
	if err != nil {
		// Context cancellation means we were raced out by another proxy.
		// This is NOT a failure of this proxy — it is a missing observation.
		if errors.Is(err, context.Canceled) {
			return false, false
		}
		// Deadline exceeded: the proxy did not respond within the timeout.
		// This IS a failure.
		if errors.Is(err, context.DeadlineExceeded) {
			return true, false
		}
		// Other errors (connection refused, reset, DNS failure, etc.) are
		// also failures.
		return true, false
	}

	// No error: check whether we actually got a response.
	// If latency is zero we never received data — treat as failure.
	if latency <= 0 {
		return true, false
	}

	// Success: we connected and received data.
	return true, true
}

// extractResponseQuality computes the logit-transformed response-quality
// score Yr from connectTime and first-read latency.
//
//	rawScore = 1 - min(connectTime + latency, refWindow) / refWindow
//	Yr       = clamp(rawScore, ε, 1-ε)
//	returns  = logit(Yr)
//
// Lower total response time → higher score.
func extractResponseQuality(connectTime, latency int64, hardTimeout time.Duration) float64 {
	responseMS := float64(connectTime + latency)
	refWindow := float64(hardTimeout / time.Millisecond)
	if refWindow <= 0 {
		refWindow = referenceResponseWindow
	}

	// Normalise: 0 ms → score=1, refWindow ms → score=0.
	normalised := responseMS / refWindow
	if normalised > 1.0 {
		normalised = 1.0
	}
	rawScore := 1.0 - normalised

	// Map to (ε, 1-ε) and return logit.
	Yr := clampEps(rawScore)
	return Logit(Yr)
}

// extractTransferQuality computes the logit-transformed transfer-quality
// score Yt from the maximum recorded download and upload rates.
//
//	throughput = maxDownloadRate + maxUploadRate  (MB/s)
func extractTransferQuality(maxDownloadRate, maxUploadRate float64) float64 {
	// Throughput in MB/s — higher is better.
	throughput := maxDownloadRate + maxUploadRate

	// Log-scale score.
	rawScore := 1.0 - math.Exp(-throughput/1.5)

	Yt := clampEps(rawScore)
	return Logit(Yt)
}

// ────────────────────────────────────────────────────────────
// Bandit-eligibility helpers
// ────────────────────────────────────────────────────────────

// IsBanditSample determines whether a connection attempt qualifies as a
// bandit-learning sample according to the design contract:
//
//	✓  First serial attempt (not a parallel race)
//	✓  Stale-node probe
//	✗  Race-cancelled by a faster proxy in the same batch
//	✗  Fallback (only one proxy available)
//	✗  Connections degraded by findSameConnection
//
// The caller must set `isFirstAttempt` and `isStaleProbe` based on the
// dial strategy that was used.
func IsBanditSample(isFirstAttempt, isStaleProbe bool) bool {
	return isFirstAttempt || isStaleProbe
}

// Observed returns true when at least one observation (S, Yr, or Yt) is
// present in the result.
func (r ObsResult) Observed() bool {
	return r.HasS || r.HasR || r.HasT
}
