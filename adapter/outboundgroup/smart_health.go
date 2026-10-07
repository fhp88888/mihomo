package outboundgroup

import (
	"context"

	P "github.com/metacubex/mihomo/constant/provider"
)

func (s *Smart) hasHealthyProxy() bool {
	for _, p := range s.GetProxies(false) {
		if p.AliveForTestUrl(s.testUrl) {
			return true
		}
	}
	return false
}

func (s *Smart) waitForHealthRecovery(ctx context.Context) error {
	for _, p := range s.providers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if recovery, ok := p.(P.HealthCheckRecoveryProvider); ok {
			if err := recovery.RecoverHealthCheck(ctx, s.testUrl, namesOf(s.GetProxies(false))); err != nil {
				return err
			}
		}
	}
	return nil
}
