package smart

import (
	"math"
)

// ────────────────────────────────────────────────────────────
// Constants for the OU + Thompson Sampling state model
// ────────────────────────────────────────────────────────────

const (
	// DefaultH is the default half-life in hours for the OU process.
	// After H hours without observations, the state is halfway between
	// its current value and the long-run mean.
	DefaultH = 3.0

	// DefaultVfS is the stationary process-noise variance for the success
	// log-odds head (zs).  When a node is unused for a long time, the
	// marginal variance converges to this value.
	DefaultVfS = 1.0

	// DefaultVfR is the stationary process-noise variance for the response-
	// quality logit head (zr).
	DefaultVfR = 0.25

	// DefaultVfT is the stationary process-noise variance for the transfer-
	// quality logit head (zt).
	DefaultVfT = 0.25

	// DefaultVnR is the observation-noise variance for response-quality
	// observations (yr).
	DefaultVnR = 0.5

	// DefaultVnT is the observation-noise variance for transfer-quality
	// observations (yt).
	DefaultVnT = 0.5

	// Eps is the safety margin for logit / inverse-logit computations
	// so that logit(p) and its gradient stay finite.
	Eps = 1e-6

	// StaleThreshold is the multiplier on H: a node that has not received
	// a bandit update for more than StaleThreshold * H hours is considered
	// stale and should be prioritised for probing.
	StaleThreshold = 2.0

	// Numerical parameters for the Laplace approximation.
	maxNewtonIter = 20
	newtonTol     = 1e-6

	// Minimum allowed posterior variance for any head.
	minP = 1e-4

	// Number of Gauss–Hermite points used for integrating the logistic-
	// normal expectation E[σ(z+ε)].
	nGHPoints = 7
)

// sqrtPi is √π (≈ 1.772453850905516).  math.SqrtPi was added in Go 1.22;
// we keep a local copy for Go 1.20 compatibility.
const sqrtPi = 1.772453850905516

// ────────────────────────────────────────────────────────────
// CellState – persistent posterior state for one cell
// ────────────────────────────────────────────────────────────

// CellState holds the posterior state for one (node, ASN, protocol) cell.
// It tracks three independent OU processes:
//
//	zs  – success log-odds        observed as S  ~ Bernoulli(σ(zs))
//	zr  – response-quality logit  observed as yr ~ N(zr, VnR)
//	zt  – transfer-quality logit  observed as yt ~ N(zt, VnT)
//
// The state can be serialised to JSON for persistence in bbolt.
type CellState struct {
	// Success log-odds head
	MuS float64 `json:"mu_s"`
	PS  float64 `json:"p_s"`

	// Response-quality logit head
	MuR float64 `json:"mu_r"`
	PR  float64 `json:"p_r"`

	// Transfer-quality logit head
	MuT float64 `json:"mu_t"`
	PT  float64 `json:"p_t"`

	// LastUpdateTime is the Unix timestamp (seconds) of the most recent
	// observation that was applied to this cell.
	LastUpdateTime int64 `json:"last_update_time"`

	// ── Fixed-lag checkpoint buffer ──────────────────────────
	// The checkpoint stores the cell state at CheckpointTime.
	// EventsAfterCP holds observations that arrived after the checkpoint,
	// in chronological order.  When an out-of-order event arrives we can
	// roll back to the checkpoint and replay.
	CheckpointTime int64       `json:"cp_time"`
	CheckpointMuS  float64     `json:"cp_mu_s"`
	CheckpointPS   float64     `json:"cp_p_s"`
	CheckpointMuR  float64     `json:"cp_mu_r"`
	CheckpointPR   float64     `json:"cp_p_r"`
	CheckpointMuT  float64     `json:"cp_mu_t"`
	CheckpointPT   float64     `json:"cp_p_t"`
	EventsAfterCP  []CellEvent `json:"events_after_cp,omitempty"`
}

// CellEvent records a single observation for checkpoint replay.
type CellEvent struct {
	Time int64   `json:"time"` // Unix seconds
	HasS bool    `json:"has_s"`
	S    bool    `json:"s"`
	HasR bool    `json:"has_r"`
	Yr   float64 `json:"yr,omitempty"`
	HasT bool    `json:"has_t"`
	Yt   float64 `json:"yt,omitempty"`
}

// ────────────────────────────────────────────────────────────
// CellPrior – immutable hierarchical prior
// ────────────────────────────────────────────────────────────

// CellPrior holds the immutable prior / hyper-parameters for a cell.
// It encodes the long-run mean-reversion targets and the noise levels.
type CellPrior struct {
	MS  float64 `json:"m_s"`  // long-run mean for zs
	MR  float64 `json:"m_r"`  // long-run mean for zr
	MT  float64 `json:"m_t"`  // long-run mean for zt
	H   float64 `json:"h"`    // half-life, hours
	VfS float64 `json:"vf_s"` // stationary process variance for zs
	VfR float64 `json:"vf_r"` // stationary process variance for zr
	VfT float64 `json:"vf_t"` // stationary process variance for zt
	VnR float64 `json:"vn_r"` // observation noise variance for yr
	VnT float64 `json:"vn_t"` // observation noise variance for yt
}

// DefaultPrior returns a CellPrior with neutral (uninformative) defaults.
func DefaultPrior() CellPrior {
	return CellPrior{
		MS:  0.0, // logit(0.5) – neutral success probability
		MR:  0.0, // neutral response quality
		MT:  0.0, // neutral transfer quality
		H:   DefaultH,
		VfS: DefaultVfS,
		VfR: DefaultVfR,
		VfT: DefaultVfT,
		VnR: DefaultVnR,
		VnT: DefaultVnT,
	}
}

// NewCellState creates a CellState initialised from a prior with inflated
// variance (×4), reflecting high initial uncertainty.
func NewCellState(now int64, prior CellPrior) *CellState {
	return &CellState{
		MuS:            prior.MS,
		PS:             prior.VfS * 4.0,
		MuR:            prior.MR,
		PR:             prior.VfR * 4.0,
		MuT:            prior.MT,
		PT:             prior.VfT * 4.0,
		LastUpdateTime: now,
	}
}

// DerivedPrior returns a CellPrior whose long-run means are taken from the
// current cell state (the "parent") and whose variance is inflated by the
// given factor.  This is used when creating an ASN-specific child from a
// node-level parent.
func (cs *CellState) DerivedPrior(inflation float64) CellPrior {
	p := DefaultPrior()
	p.MS = cs.MuS
	p.MR = cs.MuR
	p.MT = cs.MuT
	p.VfS = cs.PS * inflation
	p.VfR = cs.PR * inflation
	p.VfT = cs.PT * inflation
	if p.VfS < DefaultVfS {
		p.VfS = DefaultVfS
	}
	if p.VfR < DefaultVfR {
		p.VfR = DefaultVfR
	}
	if p.VfT < DefaultVfT {
		p.VfT = DefaultVfT
	}
	return p
}

// IsStale returns true when the cell has not received an observation for
// more than StaleThreshold * H hours.
func (cs *CellState) IsStale(now int64, prior CellPrior) bool {
	hoursSince := float64(now-cs.LastUpdateTime) / 3600.0
	return hoursSince > StaleThreshold*prior.H
}

// ────────────────────────────────────────────────────────────
// Core OU transition
// ────────────────────────────────────────────────────────────

// halfLifeToLength converts a half-life (hours) to the OU length-scale l
// (seconds).  l = H / ln(2).
func halfLifeToLength(h float64) float64 {
	return h * 3600.0 / math.Ln2
}

// PredictToNow advances all three heads from LastUpdateTime to now using
// the OU transition.  This is the "time update" (prediction step).
//
//	μ₋ = m + ρ·(μ - m)
//	P₋ = ρ²·P + Vf·(1 - ρ²)
//	ρ  = exp(-Δ / l)      l = H / ln(2)
func (cs *CellState) PredictToNow(now int64, prior CellPrior) {
	d := float64(now - cs.LastUpdateTime)
	if d <= 0 {
		return
	}

	l := halfLifeToLength(prior.H)
	rho := math.Exp(-d / l)
	rho2 := rho * rho
	oneMinusRho2 := 1.0 - rho2

	cs.MuS = prior.MS + rho*(cs.MuS-prior.MS)
	cs.PS = clampMinP(rho2*cs.PS + prior.VfS*oneMinusRho2)

	cs.MuR = prior.MR + rho*(cs.MuR-prior.MR)
	cs.PR = clampMinP(rho2*cs.PR + prior.VfR*oneMinusRho2)

	cs.MuT = prior.MT + rho*(cs.MuT-prior.MT)
	cs.PT = clampMinP(rho2*cs.PT + prior.VfT*oneMinusRho2)

	cs.LastUpdateTime = now
}

// ────────────────────────────────────────────────────────────
// Observation updates
// ────────────────────────────────────────────────────────────

// UpdateSuccess applies a Bernoulli observation S to the zs head using
// a Laplace approximation (find MAP, then match curvature).
//
//	Prior:     zs ~ N(μ₋, P₋)
//	Likelihood: S ~ Bernoulli(σ(zs))
//	Posterior: zs ≈ N(μ₊, P₊)
func (cs *CellState) UpdateSuccess(now int64, prior CellPrior, success bool) {
	cs.PredictToNow(now, prior)
	cs.saveCheckpoint(now)

	mu := cs.MuS
	P := cs.PS

	// Newton-Raphson: maximise log-posterior
	// L(z) = S·log σ(z) + (1-S)·log(1-σ(z)) - ½(z-μ)²/P
	// L'(z) = S - σ(z) - (z-μ)/P
	// L''(z) = -σ(z)(1-σ(z)) - 1/P
	sFloat := boolToFloat64(success)
	z := mu
	for i := 0; i < maxNewtonIter; i++ {
		sigma := sigmoid(z)
		grad := sFloat - sigma - (z-mu)/P
		hess := -sigma*(1.0-sigma) - 1.0/P

		delta := grad / hess
		z -= delta

		if math.Abs(delta) < newtonTol {
			break
		}
	}

	// Posterior variance = negative inverse Hessian at MAP
	sigma := sigmoid(z)
	H := sigma*(1.0-sigma) + 1.0/P
	cs.PS = clampMinP(1.0 / H)
	cs.MuS = z
}

// UpdateResponse applies a response-quality observation yr ~ N(zr, VnR)
// using a standard Kalman update.
func (cs *CellState) UpdateResponse(now int64, prior CellPrior, yr float64) {
	cs.PredictToNow(now, prior)
	cs.saveCheckpoint(now)

	cs.MuR, cs.PR = kalmanUpdate(cs.MuR, cs.PR, yr, prior.VnR)
}

// UpdateTransfer applies a transfer-quality observation yt ~ N(zt, VnT)
// using a standard Kalman update.
func (cs *CellState) UpdateTransfer(now int64, prior CellPrior, yt float64) {
	cs.PredictToNow(now, prior)
	cs.saveCheckpoint(now)

	cs.MuT, cs.PT = kalmanUpdate(cs.MuT, cs.PT, yt, prior.VnT)
}

// ApplyObservation is a convenience method that applies a CellEvent.
// It handles out-of-order arrival by rolling back to the checkpoint when
// necessary and replaying buffered events in chronological order.
func (cs *CellState) ApplyObservation(now int64, prior CellPrior, ev CellEvent) {
	// Common case: event arrives in order (or this is the first event).
	if ev.Time >= cs.LastUpdateTime {
		cs.applyEventInPlace(now, prior, ev)
		return
	}

	// Out-of-order: if the event is older than the checkpoint we cannot
	// replay perfectly.  Fall back to a direct application at event time
	// followed by predict-to-now (the "approximate" path).
	if cs.CheckpointTime == 0 || ev.Time < cs.CheckpointTime {
		// No usable checkpoint – approximate.
		cs.forceApplyAtEventTime(now, prior, ev)
		return
	}

	// Roll back to checkpoint, replay events up to ev, apply ev,
	// replay the rest, then predict to now.
	cs.restoreCheckpoint()

	inserted := false
	newEvents := make([]CellEvent, 0, len(cs.EventsAfterCP)+1)
	for _, e := range cs.EventsAfterCP {
		if !inserted && ev.Time < e.Time {
			cs.applyEventInPlace(ev.Time, prior, ev)
			cs.setCheckpoint(ev.Time)
			inserted = true
		}
		cs.applyEventInPlace(e.Time, prior, e)
		cs.setCheckpoint(e.Time)
		newEvents = append(newEvents, e)
	}
	if !inserted {
		cs.applyEventInPlace(ev.Time, prior, ev)
		cs.setCheckpoint(ev.Time)
	}
	// Rebuild the event buffer (all events are now "after" the new checkpoint).
	cs.EventsAfterCP = newEvents
	cs.PredictToNow(now, prior)
}

// ────────────────────────────────────────────────────────────
// Internal helpers
// ────────────────────────────────────────────────────────────

// applyEventInPlace applies an event at (or after) the current state time.
// Caller must ensure ev.Time >= cs.LastUpdateTime.
func (cs *CellState) applyEventInPlace(now int64, prior CellPrior, ev CellEvent) {
	cs.PredictToNow(ev.Time, prior)
	cs.setCheckpoint(ev.Time)

	if ev.HasS {
		cs.UpdateSuccess(ev.Time, prior, ev.S)
	}
	if ev.HasR {
		cs.UpdateResponse(ev.Time, prior, ev.Yr)
	}
	if ev.HasT {
		cs.UpdateTransfer(ev.Time, prior, ev.Yt)
	}

	if now > ev.Time {
		cs.PredictToNow(now, prior)
	}
}

// forceApplyAtEventTime is the fallback for out-of-order events that
// cannot be replayed from a checkpoint.
func (cs *CellState) forceApplyAtEventTime(now int64, prior CellPrior, ev CellEvent) {
	savedTime := cs.LastUpdateTime
	cs.LastUpdateTime = ev.Time
	if cs.LastUpdateTime < 0 {
		cs.LastUpdateTime = 0
	}

	if ev.HasS {
		// Use the prior mean as a rough stand-in for the state at ev.Time.
		cs.MuS = prior.MS
		cs.PS = prior.VfS * 2.0
		cs.UpdateSuccess(ev.Time, prior, ev.S)
	}
	if ev.HasR {
		cs.MuR = prior.MR
		cs.PR = prior.VfR * 2.0
		cs.UpdateResponse(ev.Time, prior, ev.Yr)
	}
	if ev.HasT {
		cs.MuT = prior.MT
		cs.PT = prior.VfT * 2.0
		cs.UpdateTransfer(ev.Time, prior, ev.Yt)
	}

	cs.LastUpdateTime = savedTime
	cs.PredictToNow(now, prior)
}

// saveCheckpoint snapshots the current state as a checkpoint and appends
// an event placeholder.  The actual event is stored separately via
// appendEvent.
func (cs *CellState) saveCheckpoint(atTime int64) {
	if cs.CheckpointTime == 0 {
		cs.setCheckpoint(atTime)
	}
}

// setCheckpoint records the current state at the given time.
func (cs *CellState) setCheckpoint(atTime int64) {
	cs.CheckpointTime = atTime
	cs.CheckpointMuS = cs.MuS
	cs.CheckpointPS = cs.PS
	cs.CheckpointMuR = cs.MuR
	cs.CheckpointPR = cs.PR
	cs.CheckpointMuT = cs.MuT
	cs.CheckpointPT = cs.PT
}

// restoreCheckpoint rolls the state back to the saved checkpoint.
func (cs *CellState) restoreCheckpoint() {
	cs.LastUpdateTime = cs.CheckpointTime
	cs.MuS = cs.CheckpointMuS
	cs.PS = cs.CheckpointPS
	cs.MuR = cs.CheckpointMuR
	cs.PR = cs.CheckpointPR
	cs.MuT = cs.CheckpointMuT
	cs.PT = cs.CheckpointPT
}

// kalmanUpdate performs a single Kalman-filter measurement update.
//
//	K = P / (P + Vn)
//	μ₊ = μ + K·(y - μ)
//	P₊ = (1 - K)·P
func kalmanUpdate(mu, P, y, vn float64) (float64, float64) {
	K := P / (P + vn)
	muNew := mu + K*(y-mu)
	PNew := clampMinP((1.0 - K) * P)
	return muNew, PNew
}

func clampMinP(p float64) float64 {
	if p < minP {
		return minP
	}
	return p
}

// ────────────────────────────────────────────────────────────
// Numerical utilities
// ────────────────────────────────────────────────────────────

// sigmoid returns the standard logistic function 1 / (1 + exp(-x)).
// It is numerically stable for large |x|.
func sigmoid(x float64) float64 {
	if x >= 0 {
		return 1.0 / (1.0 + math.Exp(-x))
	}
	expX := math.Exp(x)
	return expX / (1.0 + expX)
}

// Logit returns log(p / (1-p)), clamped to [Eps, 1-Eps].
func Logit(p float64) float64 {
	p = clampEps(p)
	return math.Log(p / (1.0 - p))
}

// InverseLogit returns 1 / (1 + exp(-x)), identical to sigmoid.
func InverseLogit(x float64) float64 {
	return sigmoid(x)
}

func clampEps(v float64) float64 {
	if v < Eps {
		return Eps
	}
	if v > 1.0-Eps {
		return 1.0 - Eps
	}
	return v
}

func boolToFloat64(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

// ────────────────────────────────────────────────────────────
// Gauss–Hermite quadrature for E[σ(z + ε)]  where  ε ~ N(0, vn)
// ────────────────────────────────────────────────────────────

// Gauss–Hermite nodes and weights for the integral
//
//	∫_{-∞}^{∞} f(x)·exp(-x²) dx  ≈  Σ w_i · f(x_i)
//
// 7-point rule.  The weights sum to √π ≈ 1.7724538509.
//
// Nodes are the roots of the physicist's Hermite polynomial H_7(x).
// Weights computed as w_i = 2^{n-1}·n!·√π / (n²·[H_{n-1}(x_i)]²).
//
// Source: Abramowitz & Stegun, Table 25.10.
var ghNodes = [nGHPoints]float64{
	-2.651961356835233,
	-1.673551628767471,
	-0.816287882858965,
	0.0,
	0.816287882858965,
	1.673551628767471,
	2.651961356835233,
}

var ghWeights = [nGHPoints]float64{
	0.0009717812450995,
	0.0545155828191279,
	0.4256072526101278,
	0.8102646175568073,
	0.4256072526101278,
	0.0545155828191279,
	0.0009717812450995,
}

// SigmaGaussHermite computes E[σ(z + ε)] where ε ~ N(0, vn), using
// Gauss–Hermite quadrature.
//
//	E[σ(z+ε)] = (1/√π) · Σ w_i · σ(z + x_i·√(2·vn))
//
// This is the quantity g(z) that appears in the Thompson-sampling utility:
// after sampling zr/zt from the posterior, the expected response/transfer
// quality is g(zr) or g(zt), integrating out the observation noise.
func SigmaGaussHermite(z float64, vn float64) float64 {
	if vn <= 0 {
		return sigmoid(z)
	}

	scale := math.Sqrt(2.0 * vn)
	sum := 0.0
	for i := 0; i < nGHPoints; i++ {
		sum += ghWeights[i] * sigmoid(z+scale*ghNodes[i])
	}
	return sum / sqrtPi
}
