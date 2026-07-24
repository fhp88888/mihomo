package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/common/xsync"
	"github.com/metacubex/mihomo/component/geodata"
	"github.com/metacubex/mihomo/component/mmdb"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
)

const (
	cleanupInterval = 120 * time.Minute
)

var (
	smartCleanupOnce sync.Once
)

type SmartOption struct {
	PolicyPriority string  `group:"policy-priority,omitempty"`
	UseLightGBM    bool    `group:"uselightgbm,omitempty"`
	CollectData    bool    `group:"collectdata,omitempty"`
	SampleRate     float64 `group:"sample-rate,omitempty"`
	PreferASN      bool    `group:"prefer-asn,omitempty"`
}

type Smart struct {
	*GroupBase

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	configName     string
	selected       string
	testUrl        string
	expectedStatus string
	disableUDP     bool

	// New in-memory routing components
	routeTable       *smart.RouteTable
	probeCoordinator *ProbeCoordinator

	// Policy priority (retained for compatibility)
	policyPriority []priorityRule
	priorityCache  xsync.Map[string, float64]
	sampleRate     float64
	useLightGBM    bool // retained for config parsing, no-op in new impl
	collectData    bool // retained for config parsing, no-op in new impl
	preferASN      bool
}

type priorityRule struct {
	pattern string
	regex   *regexp2.Regexp
	factor  float64
	isRegex bool
}

func getConfigFilename() string {
	configFile := C.Path.Config()
	baseName := filepath.Base(configFile)
	filename := strings.TrimSuffix(baseName, filepath.Ext(baseName))
	return filename
}

func NewSmart(option GroupCommonOption, smartOption SmartOption, emptyFallback C.Proxy, providers []provider.ProxyProvider) (*Smart, error) {
	if option.URL == "" {
		option.URL = C.DefaultTestURL
	}

	configName := getConfigFilename()

	routeTable := smart.NewRouteTable(smart.DefaultMaxRows)

	s := &Smart{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.Smart,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			EmptyFallback:  emptyFallback,
			Providers:      providers,
		}),
		testUrl:          option.URL,
		expectedStatus:   option.ExpectedStatus,
		configName:       configName,
		disableUDP:       option.DisableUDP,
		policyPriority:   make([]priorityRule, 0),
		sampleRate:       1,
		useLightGBM:      smartOption.UseLightGBM,
		collectData:      smartOption.CollectData,
		preferASN:        smartOption.PreferASN,
		routeTable:       routeTable,
		probeCoordinator: NewProbeCoordinator(),
	}

	if smartOption.SampleRate > 0 && smartOption.SampleRate <= 1 {
		s.sampleRate = smartOption.SampleRate
	}

	if smartOption.PolicyPriority != "" {
		applyPolicyPriority(s, smartOption.PolicyPriority)
	}

	// Restore persisted route table cells from the database.
	s.restoreRouteTable(routeTable, configName)

	s.InitSmart()

	return s, nil
}

// restoreRouteTable loads persisted route cells from the bbolt store into the
// in-memory route table.  Cells that were loaded are marked clean so the
// periodic flush won't re-persist them until they actually change.
func (s *Smart) restoreRouteTable(rt *smart.RouteTable, configName string) {
	store := cachefile.GetSmartStore()
	if store == nil {
		return
	}

	rawCells, err := store.LoadRouteCells(configName, s.Name())
	if err != nil {
		log.Debugln("[Smart] No persisted route data for group [%s]: %v", s.Name(), err)
		return
	}

	if len(rawCells) == 0 {
		return
	}

	log.Infoln("[Smart] Loaded %d persisted route cells from cache.db for group [%s], restoring...", len(rawCells), s.Name())

	loaded := 0
	for keyProxy, data := range rawCells {
		var pc smart.PersistedCell
		if json.Unmarshal(data, &pc) != nil {
			continue
		}
		// keyProxy format: {routeKey}/{proxyName}
		slash := strings.LastIndex(keyProxy, "/")
		if slash < 0 {
			continue
		}
		key := keyProxy[:slash]
		proxy := keyProxy[slash+1:]
		rt.RestoreRow(key, proxy, pc)
		loaded++
	}
	if loaded > 0 {
		log.Infoln("[Smart] Restored %d persisted route cells for group [%s]", loaded, s.Name())
	}
}

func (s *Smart) GetConfigFilename() string {
	return s.configName
}

func (s *Smart) InitSmart() {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	smartCleanupOnce.Do(func() {
		s.startTimedTask(5*time.Minute, cleanupInterval, "Global orphaned groups Clean up", s.cleanupOrphanedGroups, true)
	})
	// try load ASN database for any smart group that needs it
	if s.preferASN {
		if err := geodata.InitASN(); err != nil {
			log.Warnln("[Smart] Failed to load ASN database: %v", err)
		}
	}

	// Periodic route table persistence: every 10 minutes, iterate the route
	// table and enqueue dirty cells to the bbolt batch queue.
	s.startTimedTask(10*time.Minute, 10*time.Minute, "Route table persistence", s.persistRouteTable, false)
	s.startTimedTask(10*time.Minute, 10*time.Minute, "FailedCount decay", s.decayFailedCounts, false)
	s.startTimedTask(10*time.Minute, cleanupInterval, "Group orphaned nodes clean up", s.cleanupOrphanedNodeCache, true)
}

// ── Public proxy methods ────────────────────────────────────

func (s *Smart) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if metadata.SmartTarget == "" {
		metadata.SmartTarget = smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
	}
	// Resolve ASN so routeKey can use ASN:<n> prefix
	s.getASNCode(metadata)
	return s.tcpRoute(ctx, metadata)
}

func (s *Smart) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	if metadata.SmartTarget == "" {
		metadata.SmartTarget = smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
	}
	s.getASNCode(metadata)
	return s.udpRoute(ctx, metadata)
}

func (s *Smart) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxies := s.GetProxies(touch)
	if len(proxies) == 0 {
		return s.EmptyFallback()
	}

	if metadata == nil {
		return proxies[0]
	}

	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return p
			}
		}
	}

	if metadata.SmartTarget == "" {
		metadata.SmartTarget = smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
	}
	s.getASNCode(metadata)

	key := routeKey(metadata)
	if bestName, ok := s.routeTable.GetBestProxyIfFresh(key, smartBestProxyFreshness); ok {
		for _, p := range proxies {
			if p.Name() == bestName && p.AliveForTestUrl(s.testUrl) {
				return p
			}
		}
	}

	// Fallback: rank by score
	names := make([]string, 0, len(proxies))
	for _, p := range proxies {
		if p.AliveForTestUrl(s.testUrl) {
			names = append(names, p.Name())
		}
	}
	if len(names) == 0 {
		return proxies[0]
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

	for _, name := range ranked {
		for _, p := range proxies {
			if p.Name() == name {
				return p
			}
		}
	}

	return proxies[0]
}

func (s *Smart) IsL3Protocol(metadata *C.Metadata) bool {
	return s.Unwrap(metadata, false).IsL3Protocol(metadata)
}

func (s *Smart) SupportUDP() bool {
	return !s.disableUDP
}

func (s *Smart) Set(name string) error {
	var p C.Proxy
	for _, proxy := range s.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}

	if p == nil {
		return errors.New("proxy not exist")
	}

	s.ForceSet(name)
	return nil
}

func (s *Smart) ForceSet(name string) {
	s.selected = name
}

func (s *Smart) Now() string {
	if s.selected != "" {
		for _, p := range s.GetProxies(false) {
			if p.Name() == s.selected {
				return p.Name()
			}
		}
		s.selected = ""
	}

	return "Smart - Select"
}

func (s *Smart) MarshalJSON() ([]byte, error) {
	proxies := s.GetProxies(false)
	all := make([]string, len(proxies))
	for i, proxy := range proxies {
		all[i] = proxy.Name()
	}

	var policyPriorityBuf strings.Builder
	for i, rule := range s.policyPriority {
		if i > 0 {
			policyPriorityBuf.WriteByte(';')
		}
		fmt.Fprintf(&policyPriorityBuf, "%s:%.2f", rule.pattern, rule.factor)
	}

	return json.Marshal(map[string]any{
		"type":            s.Type().String(),
		"now":             s.Now(),
		"all":             all,
		"testUrl":         s.testUrl,
		"expectedStatus":  s.expectedStatus,
		"fixed":           s.selected,
		"hidden":          s.Hidden(),
		"icon":            s.Icon(),
		"emptyFallback":   s.EmptyFallback().Name(),
		"policy-priority": policyPriorityBuf.String(),
		"useLightGBM":     s.useLightGBM,
		"collectData":     s.collectData,
		"sampleRate":      s.sampleRate,
		"preferASN":       s.preferASN,
	})

}

func (s *Smart) Providers() []provider.ProxyProvider {
	return s.providers
}

func (s *Smart) Proxies() []C.Proxy {
	return s.GetProxies(false)
}

func (s *Smart) Touch() {
	s.GetProxies(true)
}

func (s *Smart) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	return s.GroupBase.URLTest(ctx, url, expectedStatus)
}

// ── REST API ────────────────────────────────────────────────

// RouteTableSnapshot returns a read-only snapshot of the route table for the REST API.
func (s *Smart) RouteTableSnapshot() smart.TableSnapshot {
	return s.routeTable.Snapshot(s.Name())
}

// ── Lifecycle ───────────────────────────────────────────────

func (s *Smart) Close() error {
	if s.cancel != nil {
		s.cancel()
	}

	s.wg.Wait()

	// Close the probe coordinator first so that all in-flight dials
	// and drain goroutines finish before the final persistence pass.
	// This ensures drain-produced latency updates are included in the
	// final snapshot.
	if s.probeCoordinator != nil {
		s.probeCoordinator.Close()
	}

	// Final persistence: snapshot remaining dirty cells and force-write
	// the global queue so that small route tables (< batch threshold)
	// are not lost on shutdown or config reload.
	s.persistRouteTable()
	var flushErr error
	if store := cachefile.GetSmartStore(); store != nil {
		flushErr = store.FlushQueue(true)
	}

	return flushErr
}

// ── Background tasks ────────────────────────────────────────

// startTimedTask runs a periodic task after tunnel is running.
func (s *Smart) startTimedTask(initialDelay, interval time.Duration, taskName string, task func(), runOnce bool) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		waitTicker := time.NewTicker(100 * time.Millisecond)
		for tunnel.Status() != tunnel.Running {
			select {
			case <-waitTicker.C:
			case <-s.ctx.Done():
				waitTicker.Stop()
				return
			}
		}
		waitTicker.Stop()

		select {
		case <-time.After(initialDelay):
		case <-s.ctx.Done():
			return
		}

		if tunnel.Status() == tunnel.Running {
			task()
		}

		if runOnce {
			log.Debugln("[Smart] Task [%s] completed", taskName)
			return
		}

		log.Debugln("[Smart] Task [%s] for group [%s] started, interval: %s", taskName, s.Name(), interval.Round(time.Second).String())

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if tunnel.Status() == tunnel.Running {
					task()
				}
			case <-s.ctx.Done():
				return
			}
		}
	}()
}

func (s *Smart) cleanupOrphanedGroups() {
	allProxies := tunnel.Proxies()
	existingSmartGroups := make(map[string]bool)

	for name, proxy := range allProxies {
		if proxy.Type() == C.Smart {
			existingSmartGroups[name] = true
		}
	}

	// Route table is in-memory only — no orphaned DB groups to clean.
	// Keep this for future extensibility.
	_ = existingSmartGroups
}

func (s *Smart) cleanupOrphanedNodeCache() {
	proxies := s.GetProxies(true)
	proxyMap := make(map[string]bool, len(proxies))
	for _, proxy := range proxies {
		proxyMap[proxy.Name()] = true
	}

	// Remove proxies from route table that are no longer in the provider
	snapshot := s.routeTable.Snapshot(s.Name())
	for _, row := range snapshot.Rows {
		for proxyName := range row.Proxies {
			if !proxyMap[proxyName] {
				s.routeTable.RemoveProxy(proxyName)
				log.Debugln("[Smart] Removed orphaned proxy [%s] from route table", proxyName)
			}
		}
	}
}

// persistRouteTable atomically snapshots dirty cells and enqueues them
// to the global bbolt batch queue.  The snapshot-and-clear is a single
// lock window, so no dirty flag is lost to concurrent mutations.
func (s *Smart) persistRouteTable() {
	dirty := s.routeTable.SnapshotAndClearDirty()
	if len(dirty) == 0 {
		return
	}

	store := cachefile.GetSmartStore()

	// Check whether the underlying DB is available.  If not, re-mark the
	// cells dirty so the next cycle retries — otherwise we'd silently
	// discard data when the bbolt file can't be opened.
	if !store.IsDBAvailable() {
		for cellKey := range dirty {
			if idx := strings.IndexByte(cellKey, 0); idx >= 0 {
				s.routeTable.MarkDirty(cellKey[:idx], cellKey[idx+1:])
			}
		}
		log.Infoln("[Smart] DB unavailable, re-marked %d dirty route cells for group [%s]", len(dirty), s.Name())
		return
	}

	for cellKey, pc := range dirty {
		// cellKey format: {routeKey}\x00{proxyName}
		if idx := strings.IndexByte(cellKey, 0); idx >= 0 {
			key := cellKey[:idx]
			proxy := cellKey[idx+1:]

			data, err := json.Marshal(pc)
			if err != nil {
				continue
			}

			store.AppendToGlobalQueue(smart.StoreOperation{
				Type:   smart.OpSaveRoute,
				Group:  s.Name(),
				Config: s.configName,
				Target: key + "/" + proxy,
				Data:   data,
			})
		}
	}

	log.Infoln("[Smart] Enqueued %d dirty route cells for group [%s]", len(dirty), s.Name())
}

// decayFailedCounts reduces every route cell's FailedCount by 0.1 (floor 0).
func (s *Smart) decayFailedCounts() {
	count := s.routeTable.DecayFailedCounts()
	if count > 0 {
		log.Debugln("[Smart] Decayed FailedCount for %d cells in group [%s]", count, s.Name())
	}
}

// ── Status test ─────────────────────────────────────────────

func (s *Smart) StatusTest(proxy C.Proxy, host string) (uint16, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), C.DefaultTCPTimeout)
	defer cancel()
	url := "https://" + host + "/?z=" + strconv.FormatInt(randInt63(), 10)
	return proxy.StatusTest(ctx, url)
}

func randInt63() int64 {
	// Simple fast random for URL cache-busting
	return time.Now().UnixNano()
}

// ── Policy priority ─────────────────────────────────────────

func (s *Smart) getPriorityFactor(proxyName string) float64 {
	if len(s.policyPriority) == 0 {
		return 1.0
	}
	if v, ok := s.priorityCache.Load(proxyName); ok {
		return v
	}
	factor := 1.0
	for _, rule := range s.policyPriority {
		if rule.isRegex && rule.regex != nil {
			if matched, _ := rule.regex.MatchString(proxyName); matched {
				factor = rule.factor
				break
			}
		} else if strings.Contains(proxyName, rule.pattern) {
			factor = rule.factor
			break
		}
	}
	s.priorityCache.Store(proxyName, factor)
	return factor
}

func applyPolicyPriority(s *Smart, policyPriority string) {
	lastUnescapedColon := func(str string) int {
		for i := len(str) - 1; i >= 0; i-- {
			if str[i] == ':' {
				bs := 0
				j := i - 1
				for j >= 0 && str[j] == '\\' {
					bs++
					j--
				}
				if bs%2 == 0 {
					return i
				}
			}
		}
		return -1
	}

	unescapePattern := func(p string) string {
		var b strings.Builder
		for i := 0; i < len(p); i++ {
			if p[i] == '\\' && i+1 < len(p) {
				b.WriteByte(p[i+1])
				i++
			} else {
				b.WriteByte(p[i])
			}
		}
		return b.String()
	}

	pairs := strings.Split(policyPriority, ";")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		idx := lastUnescapedColon(pair)
		if idx <= 0 || idx == len(pair)-1 {
			log.Warnln("[Smart] Invalid policy-priority rule: [%s], must be in 'pattern:factor' format and factor is required", pair)
			continue
		}

		patternRaw := strings.TrimSpace(pair[:idx])
		factorStr := strings.TrimSpace(pair[idx+1:])

		factor, err := strconv.ParseFloat(factorStr, 64)
		if err != nil {
			log.Warnln("[Smart] Invalid priority factor format for pattern [%s:%v]", patternRaw, err)
			continue
		}
		if factor <= 0 {
			log.Warnln("[Smart] Invalid priority factor [%.2f] for pattern [%s], factor must be positive", factor, patternRaw)
			continue
		}

		rule := priorityRule{
			pattern: unescapePattern(patternRaw),
			factor:  factor,
		}

		if re, err := regexp2.Compile(rule.pattern, regexp2.None); err == nil {
			rule.regex = re
			rule.isRegex = true
		}

		s.policyPriority = append(s.policyPriority, rule)
	}
}

// ── ASN resolution ──────────────────────────────────────────

func (s *Smart) getASNCode(metadata *C.Metadata) string {
	if metadata.DstIPASN == "unknown" {
		return ""
	}

	if metadata.DstIPASN == "" {
		if !s.preferASN {
			return ""
		}
		var ip netip.Addr
		if metadata.Host != "" && !metadata.Resolved() {
			ctx, cancel := context.WithTimeout(context.Background(), resolver.DefaultDNSTimeout)
			defer cancel()
			var err error
			ip, err = resolver.ResolveIP(ctx, metadata.Host)
			if err != nil {
				log.Debugln("[DNS] resolve %s error: %s", metadata.Host, err.Error())
				metadata.DstIPASN = "unknown"
				return ""
			}
			log.Debugln("[DNS] %s --> %s", metadata.Host, ip.String())
			if !ip.IsValid() {
				metadata.DstIPASN = "unknown"
				return ""
			}
		} else {
			ip = metadata.DstIP
		}

		if !geodata.ASNEnable() {
			if err := geodata.InitASN(); err != nil {
				log.Warnln("[Smart] ASN not initialized: %v", err)
				metadata.DstIPASN = "unknown"
				return ""
			}
		}
		asn, aso := mmdb.ASNInstance().LookupASN(ip.AsSlice())
		if asn == "" {
			metadata.DstIPASN = "unknown"
		} else {
			metadata.DstIPASN = asn + " " + aso
		}
		return asn
	}

	if idx := strings.IndexByte(metadata.DstIPASN, ' '); idx >= 0 {
		return metadata.DstIPASN[:idx]
	}
	return metadata.DstIPASN
}
