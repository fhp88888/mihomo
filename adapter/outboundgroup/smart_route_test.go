package outboundgroup

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/component/smart"
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
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before+1 {
			t.Fatalf("FailedCount = %v, want %v", got, before+1)
		}
	})

	t.Run("EOF is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, proxyName, io.EOF, 100, nil)
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

	t.Run("transferred data is ignored", func(t *testing.T) {
		s, before := setup()
		s.checkEarlyDeath(key, proxyName, errors.New("connection reset by peer"), 100, newFakeTracker(1024, 0))
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
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
		// RST carries a lighter 0.3 penalty, not the full 1.0.
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before+0.3 {
			t.Fatalf("FailedCount = %v, want %v", got, before+0.3)
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
