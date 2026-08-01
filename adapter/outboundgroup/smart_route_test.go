package outboundgroup

import (
	"errors"
	"io"
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

func TestMarkEarlyDeath(t *testing.T) {
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
		s.markEarlyDeath(key, proxyName, errors.New("connection reset by peer"), 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before+1 {
			t.Fatalf("FailedCount = %v, want %v", got, before+1)
		}
	})

	t.Run("EOF is ignored", func(t *testing.T) {
		s, before := setup()
		s.markEarlyDeath(key, proxyName, io.EOF, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("nil error is ignored", func(t *testing.T) {
		s, before := setup()
		s.markEarlyDeath(key, proxyName, nil, 100, nil)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("transferred data is ignored", func(t *testing.T) {
		s, before := setup()
		s.markEarlyDeath(key, proxyName, errors.New("connection reset by peer"), 100, newFakeTracker(1024, 0))
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})

	t.Run("slow failure is ignored", func(t *testing.T) {
		s, before := setup()
		slow := smartEarlyDeathLatencyLimit.Milliseconds() + 1000
		s.markEarlyDeath(key, proxyName, errors.New("connection reset by peer"), slow, nil)
		if got := routeFailedCount(t, s.routeTable, key, proxyName); got != before {
			t.Fatalf("FailedCount = %v, want %v", got, before)
		}
	})
}
