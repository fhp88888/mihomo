package outboundgroup

import (
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"syscall"
	"testing"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// fakeTracker implements statistic.Tracker by embedding the nil interface and
// only overriding Info(). markEarlyDeath only calls Info(), so the embedded
// methods are never invoked.
type fakeTracker struct {
	statistic.Tracker
	info *statistic.TrackerInfo
}

func (f *fakeTracker) Info() *statistic.TrackerInfo { return f.info }

func newFakeTracker(upload, download int64) *fakeTracker {
	return &fakeTracker{
		info: &statistic.TrackerInfo{
			UploadTotal:   atomic.NewInt64(upload),
			DownloadTotal: atomic.NewInt64(download),
		},
	}
}

func TestCheckEarlyDeath(t *testing.T) {
	const key = "TARGET:example.com"
	const proxyName = "p1"

	setup := func() (*Smart, float64) {
		rt := smart.NewRouteTable(smart.DefaultMaxRows)
		rt.RestoreRow(key, "example.com", proxyName, smart.PersistedCell{})
		s := &Smart{routeTable: rt}
		return s, routeFailedCount(t, rt, key, "example.com", proxyName)
	}

	t.Run("early death marks failed", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, "example.com", proxyName, errors.New("connection reset by peer"), 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before+0.8 {
			t.Fatalf("FailedCount = %v, want %v", got, before+0.8)
		}
	})

	t.Run("EOF is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, "example.com", proxyName, io.EOF, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("RST is left to checkResetByPeer", func(t *testing.T) {
		s, before := setup()
		// RST is the primary signal handled by checkResetByPeer (0.4); early
		// death must not add its 0.8 on top.
		err := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
		s.checkEarlyDeath(key, "example.com", proxyName, err, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("nil error is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, "example.com", proxyName, nil, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("bidirectional data is ignored", func(t *testing.T) {
		s, before := setup()
		// A full request/response exchange (both upload and download) means the
		// connection survived its first byte — not an early death.
		s.checkEarlyDeath(key, "example.com", proxyName, errors.New("connection reset by peer"), 100, newFakeTracker(1024, 512))
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("one-way data is still early death", func(t *testing.T) {
		s, before := setup()
		// Only upload flowed, no download — the response never arrived, so the
		// connection died before completing the exchange.
		s.checkEarlyDeath(key, "example.com", proxyName, errors.New("connection reset by peer"), 100, newFakeTracker(1024, 0))
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before+0.8 {
			t.Fatalf("FailedCount = %v, want %v", got, before+0.8)
		}
	})

	t.Run("slow failure is ignored", func(t *testing.T) {
		s, before := setup()
		slow := smartEarlyDeathLatencyLimit.Milliseconds() + 1000
		s.checkEarlyDeath(key, "example.com", proxyName, errors.New("connection reset by peer"), slow, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})
}

func TestCheckResetByPeer(t *testing.T) {
	const key = "TARGET:example.com"
	const proxyName = "p1"

	setup := func() (*Smart, float64) {
		rt := smart.NewRouteTable(smart.DefaultMaxRows)
		rt.RestoreRow(key, "example.com", proxyName, smart.PersistedCell{})
		s := &Smart{routeTable: rt}
		return s, routeFailedCount(t, rt, key, "example.com", proxyName)
	}

	t.Run("ECONNRESET marks failed", func(t *testing.T) {
		s, before := setup()
		// Realistic error chain: *net.OpError wrapping *os.SyscallError wrapping syscall.ECONNRESET.
		err := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
		s.checkResetByPeer(key, "example.com", proxyName, err)
		// RST carries a lighter 0.4 penalty, not the full early-death 0.8.
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before+0.4 {
			t.Fatalf("FailedCount = %v, want %v", got, before+0.4)
		}
	})

	t.Run("non-reset error is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkResetByPeer(key, "example.com", proxyName, errors.New("connection closed"))
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("EOF is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkResetByPeer(key, "example.com", proxyName, io.EOF)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("nil error is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkResetByPeer(key, "example.com", proxyName, nil)
		if got := routeFailedCount(t, s.routeTable, key, "example.com", proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})
}

// TestRouteKey verifies the route table key selection rules:
//   - ASN available and valid: "ASN:<number> <org>" (e.g. "ASN:2497 KDDI")
//   - ASN lookup failed ("0" or "unknown"): "TARGET:<effective-target>"
func TestRouteKey(t *testing.T) {
	mkMeta := func(host string, dstIP string, asn string) *C.Metadata {
		ip, err := netip.ParseAddr(dstIP)
		if err != nil {
			t.Fatalf("bad test dstIP %q: %v", dstIP, err)
		}
		return &C.Metadata{
			Host:        host,
			DstIP:       ip,
			DstIPASN:    asn,
			SmartTarget: "",
		}
	}

	// The target cache may already be initialised by a previous test run.
	// Assert the route-key contract against the effective target, not cache state.
	effectiveTarget := smart.GetEffectiveTarget("www.example.com", "1.2.3.4")
	t.Run("regular ASN keeps org name in key", func(t *testing.T) {
		// A non-CDN ASN (e.g. a residential ISP) shares one row across all
		// targets in that ASN — the domain is not part of the key, but the
		// org name is stored with the ASN so the route table surfaces the
		// full "2497 KDDI" identity.
		m := mkMeta("www.example.com", "1.2.3.4", "2497 "+"KDDI")
		if got := routeKey(m); got != "ASN:2497 KDDI" {
			t.Fatalf("routeKey = %q, want %q", got, "ASN:2497 KDDI")
		}
	})

	t.Run("asn lookup failed becomes TARGET", func(t *testing.T) {
		// getASNCode writes "0" when resolution fails; routeKey must fall back
		// to the TARGET form keyed by the effective target.
		m := mkMeta("www.example.com", "1.2.3.4", "0")
		if got := routeKey(m); got != "TARGET:"+effectiveTarget {
			t.Fatalf("routeKey = %q, want %q", got, "TARGET:"+effectiveTarget)
		}
		// SmartTarget should be populated so the close callback (which re-derives
		// the key) agrees with the route-time key.
		if m.SmartTarget != effectiveTarget {
			t.Fatalf("SmartTarget = %q, want %q", m.SmartTarget, effectiveTarget)
		}
	})

	t.Run("legacy unknown sentinel also becomes TARGET", func(t *testing.T) {
		// rules/common/ipasn.go still writes "unknown" when the ASN rule
		// matches nothing; routeKey treats it the same as "0".
		m := mkMeta("www.example.com", "1.2.3.4", "unknown")
		if got := routeKey(m); got != "TARGET:"+effectiveTarget {
			t.Fatalf("routeKey = %q, want %q", got, "TARGET:"+effectiveTarget)
		}
	})

	t.Run("rule descriptor SmartTarget still keys by effective target", func(t *testing.T) {
		// The tunnel pre-populates SmartTarget with a rule descriptor (e.g.
		// "DomainSuffix [example.com]"). routeKey must key the row by the
		// effective target, not the descriptor, so the same site reached via
		// different rules shares one row.
		m := mkMeta("www.example.com", "1.2.3.4", "0")
		m.SmartTarget = "DomainSuffix [example.com]"
		if got := routeKey(m); got != "TARGET:"+effectiveTarget {
			t.Fatalf("routeKey = %q, want %q", got, "TARGET:"+effectiveTarget)
		}
		// The descriptor must be preserved (not overwritten) — it feeds stats.
		if m.SmartTarget != "DomainSuffix [example.com]" {
			t.Fatalf("SmartTarget = %q, want rule descriptor preserved", m.SmartTarget)
		}
	})

	t.Run("cdn ASN is not special-cased", func(t *testing.T) {
		// 13335 = Cloudflare, listed in CdnASNs. The CDN key form is gone, so
		// Cloudflare targets key by ASN+org like any other ASN.
		m := mkMeta("www.cloudflare.com", "1.2.3.4", "13335 Cloudflare")
		if got := routeKey(m); got != "ASN:13335 Cloudflare" {
			t.Fatalf("routeKey = %q, want %q", got, "ASN:13335 Cloudflare")
		}
	})

	t.Run("ip-only traffic falls back to the ip", func(t *testing.T) {
		// No host: GetEffectiveTarget passes the IP through, and the key still
		// carries the TARGET form when ASN resolution failed.
		m := mkMeta("", "1.2.3.4", "0")
		if got := routeKey(m); got != "TARGET:1.2.3.4" {
			t.Fatalf("routeKey = %q, want %q", got, "TARGET:1.2.3.4")
		}
	})
}

// TestRouteDomain verifies that the per-domain key is the full hostname,
// not the broader effective target or the rule descriptor in SmartTarget.
// This must match the conn-size bucket written by wrapTCPConn's close callback
// so routing state and conn-size land in the same domainCell.
func TestRouteDomain(t *testing.T) {
	ip, err := netip.ParseAddr("1.2.3.4")
	if err != nil {
		t.Fatal(err)
	}

	// Rule-matched traffic: SmartTarget is a rule descriptor, but the domain
	// key remains the actual hostname.
	m := &C.Metadata{Host: "www.example.com", DstIP: ip, SmartTarget: "DomainSuffix [example.com]"}
	if got := routeDomain(m); got != "www.example.com" {
		t.Fatalf("routeDomain = %q, want %q", got, "www.example.com")
	}

	// Hosts that share an effective target must retain independent Best cells.
	rt := smart.NewRouteTable(10)
	key := "TARGET:*.gov-tw.test"
	for host, best := range map[string]string{
		"gov-tw.test":             "tw-ss-1",
		"WWW.gov-tw.test.":        "sg-ss-2",
		"data.gov-tw.test":        "tw-ss-2",
		"www.service.gov-tw.test": "jp-ss-1",
	} {
		m.Host = host
		rt.SetBestProxy(key, routeDomain(m), best)
	}
	for host, want := range map[string]string{
		"gov-tw.test":             "tw-ss-1",
		"www.gov-tw.test":         "sg-ss-2",
		"data.gov-tw.test":        "tw-ss-2",
		"www.service.gov-tw.test": "jp-ss-1",
	} {
		m.Host = host
		if got, ok := rt.GetBestProxy(key, routeDomain(m)); !ok || got != want {
			t.Fatalf("Best for %s = %q, %v; want %q", host, got, ok, want)
		}
	}
	if smart.DomainTreeSimilarity("www.gov-tw.test", "data.gov-tw.test") == 0 {
		t.Fatal("related hosts should still share domain-tree prior evidence")
	}

	// IP-only traffic falls back to the IP.
	m = &C.Metadata{Host: "", DstIP: ip, SmartTarget: ""}
	if got := routeDomain(m); got != "1.2.3.4" {
		t.Fatalf("routeDomain(ip-only) = %q, want %q", got, "1.2.3.4")
	}
}
