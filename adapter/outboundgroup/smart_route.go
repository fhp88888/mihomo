package outboundgroup

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
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
	// smartEarlyDeathLatencyLimit bounds firstReadLatency for a connection to
	// be classified as "died before any data flowed". A connection that fails
	// before the first byte within this window is very likely a dead proxy,
	// not a target-level rejection that simply took longer to surface.
	smartEarlyDeathLatencyLimit = 5 * time.Second
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
				log.Debugln("[Smart] tcpRoute key=%s FAST-PATH best=%s", key, bestName)
				dialCtx, dialCancel := context.WithTimeout(ctx, C.DefaultTCPTimeout)
				conn, err := s.dialAndWrap(dialCtx, p, metadata, key)
				dialCancel()
				if err == nil {
					log.Debugln("[Smart] tcpRoute key=%s FAST-PATH-OK best=%s", key, bestName)
					return conn, nil
				}
				log.Infoln("[Smart] tcpRoute key=%s fast-best FAILED proxy=%s err=%v", key, bestName, err)
				if tunnel.ShouldStopRetry(err) {
					return nil, err
				}
				break
			}
		}
	}

	// Best proxy is stale, unavailable, or failed. Rank the remaining
	// per-key proxies by score and race them with a short stagger.
	log.Infoln("[Smart] tcpRoute key=%s fast-best-miss, trying staggered fallback", key)
	names := make([]string, 0, len(proxies))
	proxyMap := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		if p.AliveForTestUrl(s.testUrl) {
			names = append(names, p.Name())
			proxyMap[p.Name()] = p
		}
	}
	s.routeTable.RefreshScores(key, names)
	ranked := s.routeTable.RankByScore(names, func(proxyName string) uint16 {
		if p, ok := proxyMap[proxyName]; ok {
			return p.LastDelayForTestUrl(s.testUrl)
		}
		return 0xffff
	}, key)

	log.Infoln("[Smart] tcpRoute key=%s STAGGERED-FALLBACK %s",
		key, s.routeTable.DebugDumpScores(key))
	log.Infoln("[Smart] tcpRoute key=%s STAGGERED-FALLBACK ranked: %v", key, ranked)

	conn, err := s.staggeredTCPFallback(ctx, metadata, key, ranked, proxyMap)
	if conn != nil || err != nil {
		return conn, err
	}
	log.Infoln("[Smart] tcpRoute key=%s staggered-fallback-exhausted, falling to discovery", key)
	return nil, nil
}

func (s *Smart) staggeredTCPFallback(ctx context.Context, metadata *C.Metadata, key string, ranked []string, proxyMap map[string]C.Proxy) (C.Conn, error) {
	ordered := make([]C.Proxy, 0, len(ranked))
	for _, name := range ranked {
		if proxy, ok := proxyMap[name]; ok {
			ordered = append(ordered, proxy)
		}
	}
	if len(ordered) == 0 {
		return nil, nil
	}

	raceCtx, cancelRace := context.WithCancel(ctx)
	defer cancelRace()

	results := make(chan dialResult, len(ordered))
	var workers sync.WaitGroup
	launched, received := 0, 0

	launch := func(proxy C.Proxy) {
		launched++
		workers.Add(1)
		log.Debugln("[Smart] tcpRoute key=%s STAGGERED-TRY proxy=%s", key, proxy.Name())
		go func() {
			defer workers.Done()
			dialCtx, dialCancel := context.WithTimeout(raceCtx, C.DefaultTCPTimeout)
			conn, connectTime, err := s.dialTCP(dialCtx, proxy, metadata, key)
			dialCancel()
			results <- dialResult{proxy: proxy, conn: conn, connectTime: connectTime, err: err}
		}()
	}

	stopAndDrain := func() {
		cancelRace()
		for received < launched {
			result := <-results
			received++
			if result.err == nil && result.conn != nil {
				s.routeTable.UpdateLatency(key, result.proxy.Name(), result.connectTime)
				result.conn.Close()
			}
		}
		workers.Wait()
	}

	launch(ordered[0])
	next := 1
	var timer *time.Timer
	var timerC <-chan time.Time
	if next < len(ordered) {
		timer = time.NewTimer(smartTCPFallbackStagger)
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
				s.routeTable.UpdateLatency(key, result.proxy.Name(), result.connectTime)
				s.routeTable.IncrementUseCount(key, result.proxy.Name())
				s.routeTable.SetBestProxy(key, result.proxy.Name())
				s.routeTable.SetTCPProbed(key)
				stopAndDrain()
				return s.wrapTCPConn(result.conn, result.proxy, metadata, result.connectTime), nil
			}
			log.Debugln("[Smart] tcpRoute key=%s STAGGERED-FAIL proxy=%s err=%v", key, result.proxy.Name(), result.err)
			if tunnel.ShouldStopRetry(result.err) {
				stopAndDrain()
				return nil, result.err
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
			return nil, ctx.Err()
		}
	}

	return nil, nil
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

	return s.wrapTCPConn(conn, proxy, metadata, connectTime), nil
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

	// Pre-rank using per-key latency (not cross-row) to avoid bias from other targets
	names := make([]string, len(available))
	for i, p := range available {
		names[i] = p.Name()
	}
	preRanked := s.routeTable.PreRankLatency(names, func(proxyName string) uint16 {
		for _, p := range proxies {
			if p.Name() == proxyName {
				return p.LastDelayForTestUrl(s.testUrl)
			}
		}
		return 0xffff
	}, key)

	log.Infoln("[Smart] discoverAndRoute key=%s PRE-RANK %s preRanked=%v",
		key, s.routeTable.DebugDumpRow(key), preRanked)

	// Refresh scores and dump them for decision traceability
	s.routeTable.RefreshScores(key, names)
	log.Infoln("[Smart] discoverAndRoute key=%s SCORES %s",
		key, s.routeTable.DebugDumpScores(key))

	// Concurrent discovery through probe coordinator
	proxy, conn, connectTime, err := s.probeCoordinator.Discover(
		ctx, key, available, metadata, preRanked,
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

	return s.wrapTCPConn(conn, proxy, metadata, connectTime), nil
}

// wrapTCPConn wraps a TCP connection with close-callback to collect latency (TTFB), pkg_loss and speed.
// connectTime is the dial duration in milliseconds, passed in so the close callback can
// compute TTFB = connectTime + firstReadLatency (time from dial start to first byte).
func (s *Smart) wrapTCPConn(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64) C.Conn {
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
	s.routeTable.MarkFailed(key, proxyName, 0.2)
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
	s.routeTable.MarkFailed(key, proxyName, 0.6)
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
