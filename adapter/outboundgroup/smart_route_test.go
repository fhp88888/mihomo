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
		rt.RestoreRow(key, proxyName, smart.PersistedCell{})
		s := &Smart{routeTable: rt}
		return s, routeFailedCount(t, rt, key, proxyName)
	}

	t.Run("early death marks failed", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, proxyName, errors.New("connection reset by peer"), 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before+0.6 {
			t.Fatalf("FailedCount = %v, want %v", got, before+0.6)
		}
	})

	t.Run("EOF is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, proxyName, io.EOF, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("RST is left to checkResetByPeer", func(t *testing.T) {
		s, before := setup()
		// RST is the primary signal handled by checkResetByPeer (0.3); early
		// death must not add its 1.0 on top.
		err := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
		s.checkEarlyDeath(key, proxyName, err, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("nil error is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, proxyName, nil, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("bidirectional data is ignored", func(t *testing.T) {
		s, before := setup()
		// A full request/response exchange (both upload and download) means the
		// connection survived its first byte — not an early death.
		s.checkEarlyDeath(key, proxyName, errors.New("connection reset by peer"), 100, newFakeTracker(1024, 512))
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("one-way data is still early death", func(t *testing.T) {
		s, before := setup()
		// Only upload flowed, no download — the response never arrived, so the
		// connection died before completing the exchange.
		s.checkEarlyDeath(key, proxyName, errors.New("connection reset by peer"), 100, newFakeTracker(1024, 0))
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before+0.6 {
			t.Fatalf("FailedCount = %v, want %v", got, before+0.6)
		}
	})

	t.Run("slow failure is ignored", func(t *testing.T) {
		s, before := setup()
		slow := smartEarlyDeathLatencyLimit.Milliseconds() + 1000
		s.checkEarlyDeath(key, proxyName, errors.New("connection reset by peer"), slow, nil)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})
}

func TestCheckResetByPeer(t *testing.T) {
	const key = "TARGET:example.com"
	const proxyName = "p1"

	setup := func() (*Smart, float64) {
		rt := smart.NewRouteTable(smart.DefaultMaxRows)
		rt.RestoreRow(key, proxyName, smart.PersistedCell{})
		s := &Smart{routeTable: rt}
		return s, routeFailedCount(t, rt, key, proxyName)
	}

	t.Run("ECONNRESET marks failed", func(t *testing.T) {
		s, before := setup()
		// Realistic error chain: *net.OpError wrapping *os.SyscallError wrapping syscall.ECONNRESET.
		err := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
		s.checkResetByPeer(key, proxyName, err)
		// RST carries a lighter 0.2 penalty, not the full 1.0.
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before+0.2 {
			t.Fatalf("FailedCount = %v, want %v", got, before+0.2)
		}
	})

	t.Run("non-reset error is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkResetByPeer(key, proxyName, errors.New("connection closed"))
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("EOF is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkResetByPeer(key, proxyName, io.EOF)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("nil error is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkResetByPeer(key, proxyName, nil)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})
}

// TestRouteKey verifies the route table key selection rules:
//   - ASN available and valid: "ASN:<number>"
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

	t.Run("regular ASN uses ASN only", func(t *testing.T) {
		// A non-CDN ASN (e.g. a residential ISP) shares one row across all
		// targets in that ASN — the domain is not part of the key.
		m := mkMeta("www.example.com", "1.2.3.4", "2497 "+"KDDI")
		if got := routeKey(m); got != "ASN:2497" {
			t.Fatalf("routeKey = %q, want %q", got, "ASN:2497")
		}
	})

	t.Run("asn lookup failed becomes TARGET", func(t *testing.T) {
		// getASNCode writes "0" when resolution fails; routeKey must fall back
		// to the TARGET form keyed by the effective target.
		m := mkMeta("www.example.com", "1.2.3.4", "0")
		if got := routeKey(m); got != "TARGET:www.example.com" {
			t.Fatalf("routeKey = %q, want %q", got, "TARGET:www.example.com")
		}
		// SmartTarget should be populated so the close callback (which re-derives
		// the key) agrees with the route-time key.
		if m.SmartTarget != "www.example.com" {
			t.Fatalf("SmartTarget = %q, want %q", m.SmartTarget, "www.example.com")
		}
	})

	t.Run("legacy unknown sentinel also becomes TARGET", func(t *testing.T) {
		// rules/common/ipasn.go still writes "unknown" when the ASN rule
		// matches nothing; AsnOf treats it the same as "0".
		m := mkMeta("www.example.com", "1.2.3.4", "unknown")
		if got := routeKey(m); got != "TARGET:www.example.com" {
			t.Fatalf("routeKey = %q, want %q", got, "TARGET:www.example.com")
		}
	})

	t.Run("cdn ASN is not special-cased", func(t *testing.T) {
		// 13335 = Cloudflare, listed in CdnASNs. The CDN key form is gone, so
		// Cloudflare targets key by ASN alone like any other ASN.
		m := mkMeta("www.cloudflare.com", "1.2.3.4", "13335 Cloudflare")
		if got := routeKey(m); got != "ASN:13335" {
			t.Fatalf("routeKey = %q, want %q", got, "ASN:13335")
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
