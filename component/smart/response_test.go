package smart

import (
	"github.com/metacubex/http"
	C "github.com/metacubex/mihomo/constant"
	"testing"
	"time"
)

func TestAlphaResponseClassificationAndHostStopLoss(t *testing.T) {
	for _, tc := range []struct {
		status int
		header http.Header
		body   string
		action VerdictAction
	}{
		{200, nil, "ok", VerdictReachable},
		{403, nil, "", VerdictRecord},
		{429, http.Header{"Retry-After": []string{"120"}}, "", VerdictRecord},
		{200, http.Header{"Cf-Mitigated": []string{"challenge"}}, "", VerdictRecord},
		{302, http.Header{"Location": []string{"https://login.example.com/"}}, "", VerdictIgnore},
		{421, nil, "", VerdictIgnore},
		{405, nil, "", VerdictIgnore},
	} {
		v := ClassifyResponse(tc.status, tc.header, []byte(tc.body), time.Now())
		if v.Action != tc.action {
			t.Fatalf("status=%d verdict=%+v", tc.status, v)
		}
		if tc.status == 429 && v.TTL != 2*time.Minute {
			t.Fatalf("Retry-After ignored: %+v", v)
		}
	}
	var throttle ProbeThrottle
	now := time.Now()
	refusal := ClassifyResponse(403, nil, nil, now)
	for i := 0; i < probeHostBlindThreshold-1; i++ {
		if !throttle.AllowHostRecord("host", refusal, now) {
			t.Fatal("premature stop-loss")
		}
	}
	if throttle.AllowHostRecord("host", refusal, now) || !throttle.Blind("host", now) {
		t.Fatal("site-wide refusals must suspend probing")
	}
	success := ClassifyResponse(200, nil, nil, now)
	if !throttle.AllowHostRecord("host", success, now) || throttle.Blind("host", now) {
		t.Fatal("successful control must restore host")
	}
	meta := &C.Metadata{Host: "host", DstPort: 443}
	if !ResponseProbeEligible(meta, 0.01, false) || ResponseProbeEligible(meta, 1, false) || ResponseProbeEligible(meta, 0, true) {
		t.Fatal("probe eligibility changed")
	}
	meta.Type = C.INNER
	if ResponseProbeEligible(meta, 0, false) {
		t.Fatal("internal probe recursion")
	}
}
