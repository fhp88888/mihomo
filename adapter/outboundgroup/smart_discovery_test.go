package outboundgroup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
)

func TestDiscoverSeparatesDomains(t *testing.T) {
	pc := NewProbeCoordinator()
	defer pc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rt := smart.NewRouteTable(10)
	const key = "ASN:13335 Cloudflare"
	started := make(chan struct{})
	leaderDone := make(chan error, 1)
	proxies := makeStubProxies("a", "b")
	go func() {
		_, _, _, err := pc.Discover(ctx, key, proxies, &C.Metadata{Host: "a.example.com"}, []string{"a"},
			func(ctx context.Context, _ C.Proxy, _ *C.Metadata, _ time.Time) (C.Conn, int64, error) {
				close(started)
				<-ctx.Done()
				return nil, 0, ctx.Err()
			}, rt)
		leaderDone <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first domain did not start")
	}
	conn := &stubConn{}
	p, got, _, err := pc.Discover(ctx, key, proxies, &C.Metadata{Host: "b.example.com"}, []string{"b"},
		func(_ context.Context, p C.Proxy, _ *C.Metadata, _ time.Time) (C.Conn, int64, error) {
			if p.Name() != "b" {
				return nil, 0, errors.New("wrong domain's proxy")
			}
			return conn, 80, nil
		}, rt)
	if err != nil || p == nil || p.Name() != "b" || got != conn {
		t.Fatalf("second domain did not discover independently: proxy=%v conn=%v err=%v", p, got, err)
	}
	got.Close()
	cancel()
	<-leaderDone
	for _, row := range rt.Snapshot("").Rows {
		if row.Key == key && domainRecords(row, routeDomain(&C.Metadata{Host: "b.example.com"}))["b"].Attributes.Latency == 80 {
			return
		}
	}
	t.Fatal("second domain latency was not recorded")
}

func TestDiscoverFollowerRecordsOwnLatency(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			pc := NewProbeCoordinator()
			defer pc.Close()
			rt := smart.NewRouteTable(10)
			const key = "ASN:13335 Cloudflare"
			metadata := &C.Metadata{Host: "a.example.com"}
			domain := routeDomain(metadata)
			proxies := makeStubProxies("leader", "alternative")
			// Seed a completed leader still available to joining followers. Their
			// preferred order differs, so using leader proves same-domain merging.
			ds := &discoveryState{done: make(chan struct{}), proxy: proxies[0], leaderCancel: func() {}}
			close(ds.done)
			pc.discoveries[discoveryKey{routeKey: key, domain: domain}] = ds
			rt.UpdateLatency(key, domain, "leader", 100)
			conn := &stubConn{}
			dialErr := errors.New("follower dial failed")
			calls := 0
			p, got, latency, err := pc.Discover(context.Background(), key, proxies, metadata, []string{"alternative"},
				func(_ context.Context, p C.Proxy, m *C.Metadata, _ time.Time) (C.Conn, int64, error) {
					calls++
					if p != proxies[0] || m != metadata {
						t.Fatal("follower did not dial leader proxy with its own metadata")
					}
					if fail {
						return nil, 300, dialErr
					}
					return conn, 300, nil
				}, rt)
			if calls != 1 {
				t.Fatalf("dial calls = %d, want 1", calls)
			}
			wantLatency := int64(150) // 100*0.75 + 300*0.25, exactly one new sample.
			if fail {
				wantLatency = 100
				if !errors.Is(err, dialErr) || got != nil {
					t.Fatalf("unexpected failure result: %v %v", got, err)
				}
			} else {
				if err != nil || p != proxies[0] || got != conn || latency != 300 {
					t.Fatalf("unexpected follower result: %v %v %d %v", p, got, latency, err)
				}
				got.Close()
			}
			rows := rt.Snapshot("").Rows
			actual := domainRecords(rows[0], domain)["leader"].Attributes.Latency
			if actual != wantLatency {
				t.Fatalf("latency = %d, want %d", actual, wantLatency)
			}
		})
	}
}
