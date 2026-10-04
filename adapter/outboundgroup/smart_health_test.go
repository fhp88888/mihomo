package outboundgroup

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type recoveryHealthProxy struct {
	C.Proxy
	healthy       atomic.Bool
	checks        atomic.Int32
	recoverHealth bool
}

func (p *recoveryHealthProxy) AliveForTestUrl(string) bool { return p.healthy.Load() }
func (p *recoveryHealthProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	p.checks.Add(1)
	p.healthy.Store(p.recoverHealth)
	return 1, nil
}

func TestSmartProviderHealthRecoveryPreservesHealthAdmission(t *testing.T) {
	for _, recoverHealth := range []bool{false, true} {
		t.Run(fmt.Sprintf("recover=%t", recoverHealth), func(t *testing.T) {
			var dials atomic.Int32
			node := &recoveryHealthProxy{Proxy: &stubProxy{name: "node", dial: func(context.Context, *C.Metadata) (C.Conn, error) { dials.Add(1); return &stubConn{}, nil }}, recoverHealth: recoverHealth}
			hc := provider.NewHealthCheck([]C.Proxy{node}, "test", 1000, 0, false, nil)
			pd, err := provider.NewCompatibleProvider("shared", []C.Proxy{node}, hc)
			if err != nil {
				t.Fatal(err)
			}
			defer pd.Close()
			s, _, pc := newBestRaceSmart()
			defer pc.Close()
			s.GroupBase = NewGroupBase(GroupBaseOption{Name: "smart", Type: C.Smart, Providers: []P.ProxyProvider{pd}})
			metadata := &C.Metadata{Host: "example.com"}
			conn, err := s.tcpRoute(context.Background(), metadata)
			if recoverHealth {
				if err != nil || conn == nil {
					t.Fatalf("recovered TCP route: %v", err)
				}
				_ = conn.Close()
			} else {
				if err == nil || conn != nil {
					t.Fatal("unhealthy node must not be dialed")
				}
				// A second Smart group shares the provider's deadline, not a new timer.
				other, _, otherPC := newBestRaceSmart()
				defer otherPC.Close()
				other.GroupBase = NewGroupBase(GroupBaseOption{Name: "other", Type: C.Smart, Providers: []P.ProxyProvider{pd}})
				for i := 0; i < 50; i++ {
					_, _ = other.tcpRoute(context.Background(), metadata)
				}
				if node.checks.Load() != 1 {
					t.Fatal("shared provider spawned repeated checks")
				}
			}
			pc.wg.Wait()
			want := int32(0)
			if recoverHealth {
				want = 1
			}
			if dials.Load() != want {
				t.Fatalf("destination dials = %d, want %d", dials.Load(), want)
			}
		})
	}
}

func TestSmartUDPProviderHealthRecoveryPreservesHealthAdmission(t *testing.T) {
	for _, recoverHealth := range []bool{false, true} {
		t.Run(fmt.Sprintf("recover=%t", recoverHealth), func(t *testing.T) {
			udp := &udpErrorProxy{stubProxy: &stubProxy{name: "udp"}}
			node := &recoveryHealthProxy{Proxy: udp, recoverHealth: recoverHealth}
			hc := provider.NewHealthCheck([]C.Proxy{node}, "test", 1000, 0, false, nil)
			pd, err := provider.NewCompatibleProvider("shared-udp", []C.Proxy{node}, hc)
			if err != nil {
				t.Fatal(err)
			}
			defer pd.Close()
			s, _, pc := newBestRaceSmart()
			defer pc.Close()
			s.GroupBase = NewGroupBase(GroupBaseOption{Name: "smart", Type: C.Smart, Providers: []P.ProxyProvider{pd}})
			conn, err := s.udpRoute(context.Background(), &C.Metadata{Host: "example.com"})
			if err == nil || conn != nil {
				t.Fatal("real UDP failure should propagate")
			}
			want := 0
			if recoverHealth {
				want = 1
			}
			if udp.calls != want || node.checks.Load() != 1 {
				t.Fatalf("dials=%d checks=%d", udp.calls, node.checks.Load())
			}
		})
	}
}
