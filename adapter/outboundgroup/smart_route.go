package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
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

const smartBestProxyFreshness = 10 * time.Second

// routeKey returns the route table key for a connection's metadata.
// Format: "ASN:<number>" when ASN is available and valid,
// otherwise "TARGET:<effective-target>".
func routeKey(metadata *C.Metadata) string {
	asn := metadata.DstIPASN
	if asn != "" && asn != "unknown" {
		return fmt.Sprintf("ASN:%s", asn)
	}

	target := metadata.SmartTarget
	if target == "" {
		target = smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
		metadata.SmartTarget = target
	}
	return fmt.Sprintf("TARGET:%s", target)
}

// tcpRoute implements the TCP routing strategy using the route table and probe coordinator.
func (s *Smart) tcpRoute(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	key := routeKey(metadata)
	proxies := s.GetProxies(true)

	// If manually selected, use that proxy directly
	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return s.dialAndWrap(ctx, p, metadata, key)
			}
		}
	}

	// Fast path: known route with TCP-probed best proxy.
	// 10% of requests intentionally skip the fast path to trigger re-discovery,
	// giving the pre-ranker a chance to use accumulated per-key latency data
	// from earlier loser measurements and potentially find a better proxy.
	if s.routeTable.IsTCPProbed(key) && rand.Intn(100)%10 != 0 {
		if bestName, ok := s.routeTable.GetBestProxyIfFresh(key, smartBestProxyFreshness); ok {
			for _, p := range proxies {
				if p.Name() == bestName && p.AliveForTestUrl(s.testUrl) {
					conn, err := s.dialAndWrap(ctx, p, metadata, key)
					if err == nil {
						return conn, nil
					}
					s.routeTable.MarkFailed(key, bestName)
					if tunnel.ShouldStopRetry(err) {
						return nil, err
					}
					break
				}
			}
		}

		// Best proxy is stale, unavailable, or failed. Try the remaining
		// per-key proxies serially by score, recalculated from current latency.
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

		for _, name := range ranked {
			p, ok := proxyMap[name]
			if !ok {
				continue
			}
			conn, err := s.dialAndWrap(ctx, p, metadata, key)
			if err == nil {
				return conn, nil
			}
			s.routeTable.MarkFailed(key, name)
			if tunnel.ShouldStopRetry(err) {
				return nil, err
			}
		}
	}

	// Cold start, 10% re-discover, or all serial fallbacks exhausted:
	// full parallel discovery.
	return s.discoverAndRoute(ctx, metadata, key, proxies)
}

// dialAndWrap dials a known proxy and wraps the connection for metrics collection.
func (s *Smart) dialAndWrap(ctx context.Context, proxy C.Proxy, metadata *C.Metadata, key string) (C.Conn, error) {
	start := time.Now()
	conn, err := proxy.DialContext(ctx, metadata)
	connectTime := time.Since(start).Milliseconds()

	if err != nil {
		if !tunnel.ShouldStopRetry(err) && !errors.Is(err, context.Canceled) {
			s.routeTable.MarkFailed(key, proxy.Name())
		}
		return nil, err
	}

	// Update latency in route table
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

	// Debug: dump the route table row before rerank probe
	log.Debugln("[Smart] Rerank for group [%s]: %s", s.Name(), s.routeTable.DebugDumpRow(key))

	// Concurrent discovery through probe coordinator
	proxy, conn, connectTime, err := s.probeCoordinator.Discover(
		ctx, key, available, metadata, preRanked,
		func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
			conn, err := p.DialContext(ctx, m)
			elapsed := time.Since(start).Milliseconds()
			return conn, elapsed, err
		},
		s.routeTable,
	)

	if err != nil {
		return nil, err
	}

	// Update route table with the winner
	s.routeTable.UpdateLatency(key, proxy.Name(), connectTime)
	s.routeTable.IncrementUseCount(key, proxy.Name())
	// set bestProxy to empty string to force re-rank on next request
	// this avoids reuse a 'latency only' proxy in the next several requests.
	s.routeTable.SetBestProxy(key, "")
	s.routeTable.SetTCPProbed(key)

	return s.wrapTCPConn(conn, proxy, metadata), nil
}

// wrapTCPConn wraps a TCP connection with close-callback to collect pkg_loss and speed.
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
		latency := firstReadLatency.Load()
		if latency > 0 {
			s.routeTable.UpdateLatency(key, proxy.Name(), latency)
		}

		// Collect speed and pkg_loss from tracker
		tracker := statistic.DefaultManager.Get(metadata.UUID)
		if tracker != nil {
			info := tracker.Info()
			maxUpload := info.MaxUploadRate.Load()
			maxDownload := info.MaxDownloadRate.Load()
			speed := float64(maxUpload)
			if maxDownload > maxUpload {
				speed = float64(maxDownload)
			}
			if speed > 0 {
				s.routeTable.UpdateSpeed(key, proxy.Name(), speed)
			}

			// Collect pkg_loss from TCP stats
			if trackerConn, ok := tracker.(net.Conn); ok {
				stats := tcpstats.GetTCPStats(trackerConn)
				if stats != nil {
					lossRate := stats.LossRate()
					if lossRate > 0 {
						s.routeTable.UpdatePkgLoss(key, proxy.Name(), lossRate)
					}
				}
			}
		}

		// Log connection close error for debugging
		readErr := firstReadErr.Load()
		if readErr != nil && readErr != io.EOF {
			log.Debugln("[Smart] Connection closed with error for [%s] via [%s]: %v",
				key, proxy.Name(), readErr)
		}
	})
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
				return s.dialUDPAndWrap(ctx, p, metadata, key)
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
				pc, err := s.dialUDPAndWrap(ctx, p, metadata, key)
				if err == nil {
					return pc, nil
				}
				if tunnel.ShouldStopRetry(err) {
					return nil, err
				}
				s.routeTable.MarkFailed(key, bestName)
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
		pc, err := s.dialUDPAndWrap(ctx, p, metadata, key)
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

// wrapUDPConn wraps a UDP packet connection to record first-response latency.
func (s *Smart) wrapUDPConn(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata) C.PacketConn {
	pc.AppendToChains(s)

	pc = callback.NewFirstReadCallBackPacketConn(pc, func(latency int64) {
		key := routeKey(metadata)
		s.routeTable.UpdateLatency(key, proxy.Name(), latency)
	})

	return pc
}
