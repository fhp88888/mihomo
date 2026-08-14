package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/tcpstats"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

const (
	smartBestProxyFreshness = 5 * time.Second
	smartTCPFallbackStagger = 200 * time.Millisecond
	// smartBestExclusiveWindow is how long the current best proxy gets to win on
	// its own before the fast-path hands off into the staggered race.
	smartBestExclusiveWindow = 600 * time.Millisecond
	// smartEarlyDeathLatencyLimit bounds firstReadLatency for a connection to
	// be classified as "died before any data flowed". A connection that fails
	// before the first byte within this window is very likely a dead proxy,
	// not a target-level rejection that simply took longer to surface.
	smartEarlyDeathLatencyLimit = 5 * time.Second
	// exploreBatch is the number of top-quality candidates dialed first in a
	// discovery wave.  When the candidate set is larger than this, the
	// quality-best candidates are shuffled before dialing so we don't always
	// dial the same proxy first.
	exploreBatch = 16
	// highLossThreshold is the aggregated pkg-loss EMA above which a proxy is
	// treated as low-quality for discovery ordering (deferred to the end).
	highLossThreshold = 0.1
)

// routeKey returns the route table key for a connection's metadata.
// Format: "ASN:<number>" when ASN is available and valid,
// otherwise "TARGET:<effective-target>".
func routeKey(metadata *C.Metadata) string {
	asn := smart.AsnOf(metadata.DstIPASN)
	if asn != "0" {
		return "ASN:" + asn
	}

	target := metadata.SmartTarget
	if target == "" {
		target = smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
		metadata.SmartTarget = target
	}
	return "TARGET:" + target
}

// tcpRoute implements the TCP routing strategy using the route table and probe coordinator.
func (s *Smart) tcpRoute(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	key := routeKey(metadata)
	proxies := s.GetProxies(true)

	log.Debugln("[Smart] tcpRoute ENTER key=%s host=%s proxies=%d", key, metadata.Host, len(proxies))

	// If manually selected, use that proxy directly
	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				log.Debugln("[Smart] tcpRoute key=%s MANUAL-SELECT proxy=%s", key, s.selected)
				dialCtx, dialCancel := context.WithTimeout(ctx, C.DefaultTCPTimeout)
				defer dialCancel()
				return s.dialAndWrap(dialCtx, p, metadata, key)
			}
		}
	}

	// Fast path: known route with TCP-probed best proxy.
	// 4% of requests intentionally skip the fast path to trigger re-discovery
	if s.routeTable.IsTCPProbed(key) && rand.Intn(100)%25 != 0 {
		conn, err := s.serialTcpConn(ctx, metadata, key, proxies)
		if conn != nil || err != nil {
			return conn, err
		}
	}

	// Fallback, Cold start, 4% re-discover, or all serial fallbacks exhausted:
	// full parallel discovery.
	return s.discoverAndRoute(ctx, metadata, key, proxies)
}

func (s *Smart) serialTcpConn(ctx context.Context, metadata *C.Metadata, key string, proxies []C.Proxy) (C.Conn, error) {
	if bestName, ok := s.routeTable.GetBestProxyIfFresh(key, smartBestProxyFreshness); ok {
		for _, p := range proxies {
			if p.Name() == bestName && p.AliveForTestUrl(s.testUrl) {
				// Best-first race: best gets a smartBestExclusiveWindow
				// head-start, then the rest ranked by score race at the normal
				// stagger.  Whoever connects first wins — including a slow best.
				// Losers are drained in the background via probeCoordinator.
				ordered := s.rankCandidates(key, proxies, p)
				log.Infoln("[Smart] tcpRoute key=%s BEST-RACE ranks=%v (best first)", key, namesOf(ordered))
				conn, err := s.raceAndWrap(ctx, metadata, key, ordered,
					smartBestExclusiveWindow, &s.probeCoordinator.wg)
				if conn != nil || err != nil {
					return conn, err
				}
				log.Infoln("[Smart] tcpRoute key=%s best-race-exhausted, falling to discovery", key)
				return nil, nil
			}
		}
	}

	// Best proxy is stale, unavailable, or failed. Race the alive proxies by
	// score with a short stagger.
	ordered := s.rankCandidates(key, proxies, nil)
	if len(ordered) == 0 {
		return nil, nil
	}
	log.Infoln("[Smart] tcpRoute key=%s STAGGERED-FALLBACK ranked: %v", key, namesOf(ordered))
	conn, err := s.raceAndWrap(ctx, metadata, key, ordered, smartTCPFallbackStagger, nil)
	if conn != nil || err != nil {
		return conn, err
	}
	log.Infoln("[Smart] tcpRoute key=%s staggered-fallback-exhausted, falling to discovery", key)
	return nil, nil
}

// rankCandidates filters proxies to alive ones (excluding best when given),
// refreshes their scores, and returns the score-ranked order with best (if any)
// anchored first.
func (s *Smart) rankCandidates(key string, proxies []C.Proxy, best C.Proxy) []C.Proxy {
	over := make([]string, 0, len(proxies))
	proxyMap := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		if best != nil && p.Name() == best.Name() {
			continue
		}
		if p.AliveForTestUrl(s.testUrl) {
			over = append(over, p.Name())
			proxyMap[p.Name()] = p
		}
	}
	if best != nil {
		proxyMap[best.Name()] = best
	}

	// Refresh with best included so its score stays current for the race, but
	// rank only the non-best proxies — best is already anchored at the front.
	refresh := over
	if best != nil {
		refresh = append(append([]string{}, over...), best.Name())
	}
	s.routeTable.RefreshScores(key, refresh)
	ranked := s.routeTable.RankByScore(over, func(proxyName string) uint16 {
		if p, ok := proxyMap[proxyName]; ok {
			return p.LastDelayForTestUrl(s.testUrl)
		}
		return 0xffff
	}, key)

	if best != nil {
		return append([]C.Proxy{best}, orderByNames(proxies, ranked)...)
	}
	return orderByNames(proxies, ranked)
}

// raceAndWrap runs a staggered race over ordered and wraps the winner.
// firstStagger is the gap before the 2nd candidate (ordered[0] is dialed
// immediately); later candidates follow at smartTCPFallbackStagger.  wg != nil
// makes loser draining asynchronous so the caller returns the winner
// immediately while late successful losers still sample latency in the
// background.
func (s *Smart) raceAndWrap(ctx context.Context, metadata *C.Metadata, key string,
	ordered []C.Proxy, firstStagger time.Duration, wg *sync.WaitGroup) (C.Conn, error) {
	winner, conn, _, err := raceStaggered(ctx, key, ordered, wg, firstStagger,
		// Race dials go through dialTCP, which records a genuine dial failure
		// as MarkFailed.
		func(dialCtx context.Context, proxy C.Proxy) (C.Conn, int64, error) {
			return s.dialTCP(dialCtx, proxy, metadata, key)
		},
		// onConnect: successful dials (winner + late successful losers) each
		// contribute a latency sample.
		func(proxyName string, connectTime int64) {
			s.routeTable.UpdateLatency(key, proxyName, connectTime)
		},
		// onFail: dialTCP already handles MarkFailed for genuine failures, so
		// the race has nothing to add for failures.
		nil,
		// onWinner: promote the winner to best proxy and mark the route
		// probed.  The winner's latency is already sampled via onConnect.
		func(proxy C.Proxy, connectTime int64) {
			s.routeTable.IncrementUseCount(key, proxy.Name())
			s.routeTable.SetBestProxy(key, proxy.Name())
			s.routeTable.SetTCPProbed(key)
		},
	)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, nil
	}
	return s.wrapTCPConn(conn, winner, metadata), nil
}

func raceStaggered(ctx context.Context, key string, ordered []C.Proxy, wg *sync.WaitGroup,
	firstStagger time.Duration,
	dial func(context.Context, C.Proxy) (C.Conn, int64, error),
	onConnect func(proxyName string, connectTime int64),
	onFail func(proxy C.Proxy, err error),
	onWinner func(proxy C.Proxy, connectTime int64),
) (C.Proxy, C.Conn, int64, error) {
	if len(ordered) == 0 {
		return nil, nil, 0, nil
	}

	raceCtx, cancelRace := context.WithCancel(ctx)

	// keepLosersAlive: 只对 discovery 异步路径（wg != nil）的 winner 分支放开。
	// 置位后 deferred cancelRace 被跳过，让在途 loser 拨号跑到各自的 2s 超时，
	// 由后台 drain goroutine 采集真实 connectTime 并更新 EMA（onConnect →
	// UpdateLatency）。fallback 同步路径（wg == nil）及 ShouldStopRetry /
	// ctx.Done 返回路径保持立刻取消，避免调用方被最慢的 loser 阻塞。
	keepLosersAlive := false
	defer func() {
		if !keepLosersAlive {
			cancelRace()
		}
	}()

	results := make(chan dialResult, len(ordered))
	var workers sync.WaitGroup
	launched, received := 0, 0
	// firstFailed: the first candidate (best) failed inside its head-start
	// window — collapse it and dial the next candidate immediately.
	firstFailed := false

	launch := func(proxy C.Proxy) {
		launched++
		workers.Add(1)
		log.Debugln("[Smart] tcpRoute key=%s STAGGERED-TRY proxy=%s", key, proxy.Name())
		go func() {
			defer workers.Done()
			dialCtx, dialCancel := context.WithTimeout(raceCtx, C.DefaultTCPTimeout)
			conn, connectTime, err := dial(dialCtx, proxy)
			dialCancel()
			results <- dialResult{proxy: proxy, conn: conn, connectTime: connectTime, err: err}
		}()
	}

	// drain consumes n results, closing successful connections and feeding
	// onConnect.  It runs from the select-loop goroutine (sync drain) or the
	// stopAndDrain background goroutine (async discovery drain).  Successful
	// loser samples are aggregated and logged once per batch.
	drain := func(n int) {
		samples := make([]string, 0, n)
		for i := 0; i < n; i++ {
			result := <-results
			if result.err == nil && result.conn != nil {
				onConnect(result.proxy.Name(), result.connectTime)
				samples = append(samples, fmt.Sprintf("%s=%dms", result.proxy.Name(), result.connectTime))
				result.conn.Close()
			}
		}
		if len(samples) > 0 {
			log.Debugln("[Smart] tcpRoute key=%s LOSER-SAMPLED %s", key, strings.Join(samples, " "))
		}
	}

	// stopAndDrain cancels in-flight dials and clears the remaining results.
	// With wg nil it does so synchronously (deterministic, fallback).  With wg
	// set it hands the drain to a background goroutine so the caller can return
	// the winner immediately (discovery hot path).  On that path the goroutine
	// skips cancelRace until all losers finish, so late successful losers still
	// contribute a latency sample via onConnect before their conn is closed.
	stopAndDrain := func() {
		remaining := launched - received
		if wg == nil {
			cancelRace()
			drain(remaining)
			workers.Wait()
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Cancel raceCtx when the drain settles — even if drain or
			// workers.Wait panics, the deferred cancelRace fires so in-flight
			// dial contexts are never leaked.
			defer cancelRace()
			drain(remaining)
			workers.Wait()
		}()
	}

	launch(ordered[0])
	next := 1
	var timer *time.Timer
	var timerC <-chan time.Time
	if next < len(ordered) {
		timer = time.NewTimer(firstStagger)
		timerC = timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for received < launched || next < len(ordered) {
		select {
		case result := <-results:
			received++
			if result.err == nil {
				log.Infoln("[Smart] tcpRoute key=%s STAGGERED-OK proxy=%s", key, result.proxy.Name())
				onConnect(result.proxy.Name(), result.connectTime)
				if onWinner != nil {
					onWinner(result.proxy, result.connectTime)
				}
				// Winner found: on the async discovery path let the in-flight
				// losers finish dialing so their connectTime is sampled, then
				// hand them to the background drain goroutine.  The deferred
				// cancelRace is skipped (keepLosersAlive) and the goroutine
				// cancels after all losers settle.
				if wg != nil {
					keepLosersAlive = true
				}
				stopAndDrain()
				return result.proxy, result.conn, result.connectTime, nil
			}
			log.Debugln("[Smart] tcpRoute key=%s STAGGERED-FAIL proxy=%s err=%v", key, result.proxy.Name(), result.err)
			if onFail != nil {
				onFail(result.proxy, result.err)
			}
			if tunnel.ShouldStopRetry(result.err) {
				stopAndDrain()
				return nil, nil, 0, result.err
			}
			// Collapse the head-start if the first candidate (best) failed
			// early, so fallback starts immediately instead of at 600ms.
			if !firstFailed && result.proxy == ordered[0] && next < len(ordered) {
				firstFailed = true
				if timer != nil {
					if !timer.Stop() {
						select { case <-timer.C: default: }
					}
				}
				timerC = nil
				launch(ordered[next])
				next++
				if next < len(ordered) {
					timer = time.NewTimer(smartTCPFallbackStagger)
					timerC = timer.C
				}
			}
		case <-timerC:
			launch(ordered[next])
			next++
			if next < len(ordered) {
				timer.Reset(smartTCPFallbackStagger)
			} else {
				timerC = nil
			}
		case <-ctx.Done():
			stopAndDrain()
			return nil, nil, 0, ctx.Err()
		}
	}

	return nil, nil, 0, nil
}

// dialTCP dials a known proxy and records a genuine dial failure.
func (s *Smart) dialTCP(ctx context.Context, proxy C.Proxy, metadata *C.Metadata, key string) (C.Conn, int64, error) {
	start := time.Now()
	conn, err := proxy.DialContext(ctx, metadata)
	connectTime := time.Since(start).Milliseconds()
	if connectTime < 1 {
		connectTime = 1
	}

	if err != nil {
		log.Debugln("[Smart] dialAndWrap key=%s proxy=%s FAIL connectTime=%dms err=%v",
			key, proxy.Name(), connectTime, err)
		if !tunnel.ShouldStopRetry(err) && !errors.Is(err, context.Canceled) {
			s.routeTable.MarkFailed(key, proxy.Name(), 1.0)
		}
	}

	return conn, connectTime, err
}

// dialAndWrap dials a known proxy and wraps the connection for metrics collection.
func (s *Smart) dialAndWrap(ctx context.Context, proxy C.Proxy, metadata *C.Metadata, key string) (C.Conn, error) {
	conn, connectTime, err := s.dialTCP(ctx, proxy, metadata, key)
	if err != nil {
		return nil, err
	}

	// Write connectTime to the route table so that all paths (fast-path,
	// serial fallback, discovery) contribute the same metric. TTFB varies
	// by connection lifetime and is not available for losers.
	s.routeTable.UpdateLatency(key, proxy.Name(), connectTime)
	s.routeTable.IncrementUseCount(key, proxy.Name())
	s.routeTable.SetBestProxy(key, proxy.Name())
	s.routeTable.SetTCPProbed(key)

	return s.wrapTCPConn(conn, proxy, metadata), nil
}

// discoverAndRoute performs pre-rank + concurrent discovery for a new or failed route.
func (s *Smart) discoverAndRoute(ctx context.Context, metadata *C.Metadata, key string, proxies []C.Proxy) (C.Conn, error) {
	// Filter proxies: must be alive
	available := make([]C.Proxy, 0, len(proxies))
	for _, p := range proxies {
		if p.AliveForTestUrl(s.testUrl) {
			available = append(available, p)
		}
	}

	if len(available) == 0 {
		log.Infoln("[Smart] discoverAndRoute key=%s NO-ALIVE-PROXIES (total=%d)", key, len(proxies))
		return nil, errors.New("no alive proxies available")
	}

	// Build the exploration order.  The aggregated per-proxy quality (latency,
	// speed, pkg loss, failed count, jitter fused into one Score by
	// AggregateByProxy) decides who gets dialed first, with a randomized
	// shuffle inside the best candidates so we don't always dial the same
	// proxy first.  Proxies with no aggregation samples yet (e.g. a quality
	// node that rarely wins and thus is never sampled) are kept in the
	// candidate pool with a neutral score so they still get discovered.
	names := namesOf(available)
	ordered := s.exploreOrder(available, proxies, key)

	log.Infoln("[Smart] discoverAndRoute key=%s EXPLORE-ORDER %v", key, namesOf(ordered))

	// Refresh scores and dump them for decision traceability
	s.routeTable.RefreshScores(key, names)
	log.Infoln("[Smart] discoverAndRoute key=%s SCORES %s",
		key, s.routeTable.DebugDumpScores(key))

	// Concurrent discovery through probe coordinator
	proxy, conn, connectTime, err := s.probeCoordinator.Discover(
		ctx, key, available, metadata, namesOf(ordered),
		func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
			dialCtx, dialCancel := context.WithTimeout(ctx, C.DefaultTCPTimeout)
			conn, err := p.DialContext(dialCtx, m)
			dialCancel()
			elapsed := time.Since(start).Milliseconds()
			return conn, elapsed, err
		},
		s.routeTable,
	)

	if err != nil {
		log.Infoln("[Smart] discoverAndRoute key=%s DISCOVERY-FAILED err=%v", key, err)
		return nil, err
	}

	log.Infoln("[Smart] discoverAndRoute key=%s DISCOVERY-WINNER proxy=%s connectTime=%dms",
		key, proxy.Name(), connectTime)

	// Refresh scores again with winner latency, then dump final score state
	s.routeTable.RefreshScores(key, names)
	log.Infoln("[Smart] discoverAndRoute key=%s POST-DISCOVERY %s",
		key, s.routeTable.DebugDumpScores(key))

	// Note: probeBatch already wrote the winner's connectTime to the route table
	// (smart_probe.go:189). Do NOT write it again here — that would double-count
	// the sample.
	s.routeTable.IncrementUseCount(key, proxy.Name())
	s.routeTable.SetBestProxy(key, proxy.Name())
	s.routeTable.SetTCPProbed(key)

	return s.wrapTCPConn(conn, proxy, metadata), nil
}

func (s *Smart) exploreOrder(available []C.Proxy, proxies []C.Proxy, key string) []C.Proxy {
	attrs := s.routeTable.ProxyAttrsSnapshot()
	if len(attrs) == 0 {
		names := namesOf(available)
		preRanked := s.routeTable.PreRankLatency(names, func(proxyName string) uint16 {
			for _, p := range proxies {
				if p.Name() == proxyName {
					return p.LastDelayForTestUrl(s.testUrl)
				}
			}
			return 0xffff
		}, key)
		return orderByNames(available, preRanked)
	}

	// Neutral score: median of the sampled proxies' Scores.
	var sampled []float64
	for _, a := range attrs {
		sampled = append(sampled, a.Score)
	}
	median := median(sampled)

	type cand struct {
		proxy    C.Proxy
		score    float64
		deferred bool
	}
	cands := make([]cand, 0, len(available))
	for _, p := range available {
		a, ok := attrs[p.Name()]
		score := median
		if ok {
			score = a.Score
		}
		cands = append(cands, cand{
			proxy:    p,
			score:    score,
			deferred: ok && (a.FailedCount > 0 || a.PkgLoss > highLossThreshold),
		})
	}

	// Sort: deferred last; otherwise Score descending; ties by name.
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].deferred != cands[j].deferred {
			return !cands[i].deferred
		}
		if cands[i].score != cands[j].score {
			return cands[i].score > cands[j].score
		}
		return cands[i].proxy.Name() < cands[j].proxy.Name()
	})

	// Shuffle the best exploreBatch non-deferred candidates so the dial order
	// within the top quality tier is randomized (exploration).  This only kicks
	// in when the pool is bigger than exploreBatch — a small pool keeps its
	// deterministic quality order.  Deferred proxies stay at the end — they are
	// the last-resort tier and must not be pulled forward by the shuffle.
	nonDeferred := 0
	for i := range cands {
		if !cands[i].deferred {
			nonDeferred++
		}
	}
	if nonDeferred > exploreBatch {
		rand.Shuffle(exploreBatch, func(i, j int) {
			cands[i], cands[j] = cands[j], cands[i]
		})
	}

	out := make([]C.Proxy, 0, len(cands))
	for i := range cands {
		out = append(out, cands[i].proxy)
	}
	return out
}

func namesOf(ps []C.Proxy) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Name()
	}
	return out
}

func orderByNames(ps []C.Proxy, names []string) []C.Proxy {
	byName := make(map[string]C.Proxy, len(ps))
	for _, p := range ps {
		byName[p.Name()] = p
	}
	out := make([]C.Proxy, 0, len(names))
	for _, name := range names {
		if p, ok := byName[name]; ok {
			out = append(out, p)
		}
	}
	return out
}

func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := append([]float64(nil), vals...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

// wrapTCPConn wraps a TCP connection with close-callbacks that collect pkg_loss
// and speed, and penalize RST / early-death failures.  Latency (dial connectTime)
// is recorded at dial time by the callers, not here.
func (s *Smart) wrapTCPConn(c C.Conn, proxy C.Proxy, metadata *C.Metadata) C.Conn {
	c.AppendToChains(s)

	start := time.Now()
	var firstReadErr atomic.TypedValue[error]
	var firstReadLatency atomic.Int64

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err != nil {
				firstReadErr.Store(err)
			}
		})
	}

	c = callback.NewFirstReadCallBackConn(c, func(err error) {
		firstReadLatency.Store(time.Since(start).Milliseconds())
		if err != nil {
			firstReadErr.Store(err)
		}
	})

	return callback.NewCloseCallbackConn(c, func() {
		key := routeKey(metadata)

		firstRead := firstReadLatency.Load()
		readErr := firstReadErr.Load()

		// Collect speed and pkg_loss from tracker
		tracker := statistic.DefaultManager.Get(metadata.UUID)
		if tracker != nil {
			info := tracker.Info()
			maxUpload := info.MaxUploadRate.Load()
			maxDownload := info.MaxDownloadRate.Load()
			upTotal := info.UploadTotal.Load()
			downTotal := info.DownloadTotal.Load()
			speed := float64(maxUpload)
			if maxDownload > maxUpload {
				speed = float64(maxDownload)
			}
			if speed > 0 {
				s.routeTable.UpdateSpeed(key, proxy.Name(), speed)
			}

			// Collect pkg_loss from TCP stats.
			// Always update when TCP stats are available — even 0% loss
			// drives the EMA back toward 0, preventing stale loss from
			// accumulating indefinitely.
			var lossRate float64
			if trackerConn, ok := tracker.(net.Conn); ok {
				stats := tcpstats.GetTCPStats(trackerConn)
				if stats != nil {
					lossRate = stats.LossRate()
					s.routeTable.UpdatePkgLoss(key, proxy.Name(), lossRate)
				}
			}

			log.Debugln("[Smart] Close key=%s proxy=%s firstReadLat=%dms maxUp=%d maxDown=%d spd=%.0f loss=%.3f upTotal=%d downTotal=%d",
				key, proxy.Name(), firstRead, maxUpload, maxDownload, speed, lossRate, upTotal, downTotal)
		} else {
			log.Debugln("[Smart] Close key=%s proxy=%s NO-TRACKER firstReadLat=%dms",
				key, proxy.Name(), firstRead)
		}

		// Log connection close error for debugging
		if readErr != nil && readErr != io.EOF {
			log.Debugln("[Smart] Connection closed with error for [%s] via [%s]: %v",
				key, proxy.Name(), readErr)
		}

		// check for TCP RST or early death and mark-failed if necessary
		s.checkResetByPeer(key, proxy.Name(), readErr)
		s.checkEarlyDeath(key, proxy.Name(), readErr, firstRead, tracker)
	})
}

// checkResetByPeer penalizes a proxy when a connection was aborted with a TCP RST
func (s *Smart) checkResetByPeer(key, proxyName string, readErr error) {
	if readErr == nil || !errors.Is(readErr, syscall.ECONNRESET) {
		return
	}
	s.routeTable.MarkFailed(key, proxyName, 0.4)
	log.Debugln("[Smart] RST mark-failed key=%s proxy=%s err=%v", key, proxyName, readErr)
}

// checkEarlyDeath penalizes a proxy when a connection failed before its first
// byte ever arrived and never completed a bidirectional flow.
func (s *Smart) checkEarlyDeath(key, proxyName string, readErr error, firstReadLatencyMs int64, tracker statistic.Tracker) {
	if readErr == nil || readErr == io.EOF {
		return
	}
	// don't double-count RST
	if errors.Is(readErr, syscall.ECONNRESET) {
		return
	}
	if firstReadLatencyMs >= smartEarlyDeathLatencyLimit.Milliseconds() {
		return
	}
	if tracker != nil {
		info := tracker.Info()
		if info.DownloadTotal.Load() > 0 {
			return
		}
	}
	s.routeTable.MarkFailed(key, proxyName, 0.8)
	log.Debugln("[Smart] EARLY-DEATH mark-failed key=%s proxy=%s firstReadLat=%dms err=%v",
		key, proxyName, firstReadLatencyMs, readErr)
}

// udpRoute implements the UDP routing strategy.
// It tries the current best proxy first, then falls through to pre-rank order.
func (s *Smart) udpRoute(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	if !s.SupportUDP() {
		return nil, errors.New("UDP not supported")
	}

	key := routeKey(metadata)
	proxies := s.GetProxies(true)

	// If manually selected, use that proxy
	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected && p.SupportUDP() {
				dialCtx, dialCancel := context.WithTimeout(ctx, C.DefaultUDPTimeout)
				defer dialCancel()
				return s.dialUDPAndWrap(dialCtx, p, metadata, key)
			}
		}
		return nil, errors.New("selected proxy not found or does not support UDP")
	}

	// Filter to UDP-capable, alive proxies
	udpProxies := make([]C.Proxy, 0, len(proxies))
	for _, p := range proxies {
		if p.SupportUDP() && p.AliveForTestUrl(s.testUrl) {
			udpProxies = append(udpProxies, p)
		}
	}
	if len(udpProxies) == 0 {
		return nil, errors.New("no UDP-capable proxies available")
	}

	// Try fresh best proxy first
	if bestName, ok := s.routeTable.GetBestProxyIfFresh(key, smartBestProxyFreshness); ok {
		for _, p := range udpProxies {
			if p.Name() == bestName {
				dialCtx, dialCancel := context.WithTimeout(ctx, C.DefaultUDPTimeout)
				pc, err := s.dialUDPAndWrap(dialCtx, p, metadata, key)
				dialCancel()
				if err == nil {
					return pc, nil
				}
				if tunnel.ShouldStopRetry(err) {
					return nil, err
				}
				s.routeTable.MarkFailed(key, bestName, 1.0)
				break
			}
		}
	}

	// Rank remaining by score
	names := make([]string, len(udpProxies))
	for i, p := range udpProxies {
		names[i] = p.Name()
	}
	s.routeTable.RefreshScores(key, names)
	ranked := s.routeTable.RankByScore(names, func(proxyName string) uint16 {
		for _, p := range proxies {
			if p.Name() == proxyName {
				return p.LastDelayForTestUrl(s.testUrl)
			}
		}
		return 0xffff
	}, key)

	// Build ordered list by score rank
	ordered := make([]C.Proxy, 0, len(ranked))
	proxyMap := make(map[string]C.Proxy, len(udpProxies))
	for _, p := range udpProxies {
		proxyMap[p.Name()] = p
	}
	for _, name := range ranked {
		if p, ok := proxyMap[name]; ok {
			ordered = append(ordered, p)
		}
	}

	// Serial try
	var lastErr error
	for _, p := range ordered {
		dialCtx, dialCancel := context.WithTimeout(ctx, C.DefaultUDPTimeout)
		pc, err := s.dialUDPAndWrap(dialCtx, p, metadata, key)
		dialCancel()
		if err == nil {
			return pc, nil
		}
		lastErr = err
		if tunnel.ShouldStopRetry(err) {
			return nil, err
		}
	}

	return nil, lastErr
}

// dialUDPAndWrap dials a UDP proxy and wraps the packet conn for latency collection.
func (s *Smart) dialUDPAndWrap(ctx context.Context, proxy C.Proxy, metadata *C.Metadata, key string) (C.PacketConn, error) {
	start := time.Now()
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	connectTime := time.Since(start).Milliseconds()

	if err != nil {
		return nil, err
	}

	s.routeTable.UpdateLatency(key, proxy.Name(), connectTime)
	s.routeTable.IncrementUseCount(key, proxy.Name())
	s.routeTable.SetBestProxy(key, proxy.Name())

	return s.wrapUDPConn(pc, proxy, metadata), nil
}

// wrapUDPConn wraps a UDP packet connection. Unlike TCP, UDP does not collect
// connectTime here — connectTime is already written by dialUDPAndWrap.
func (s *Smart) wrapUDPConn(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata) C.PacketConn {
	pc.AppendToChains(s)
	return pc
}
