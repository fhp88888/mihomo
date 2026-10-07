package smart

// Probe helpers for the general proxy adapter. Beta's Smart routing does not
// use Alpha's exit watcher or response-based candidate selection.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"strings"
)

const (
	ExitProbeBodyLimit      = 2048
	ReasonRegionUnavailable = "region unavailable"
)

var ExitTraceURLs = []string{
	"https://www.cloudflare.com/cdn-cgi/trace",
	"https://api.ip.sb/geoip",
	"https://ipwho.is/",
}

type ExitProbeResult struct {
	Region string
	ASN    string
	Key    string
}

// ParseExitAnswer reads the region and address out of any of the exit endpoints; the
// address is only for the caller's own lookup and must not be stored.
func ParseExitAnswer(body []byte) (region string, ip netip.Addr) {
	if region, ip = ParseExitTrace(body); region != "" {
		return region, ip
	}
	var answer struct {
		CountryCode string `json:"country_code"`
		Country     string `json:"country"`
		IP          string `json:"ip"`
	}
	if json.Unmarshal(body, &answer) != nil {
		return "", netip.Addr{}
	}
	code := answer.CountryCode
	if code == "" && len(answer.Country) == 2 {
		code = answer.Country
	}
	if code = strings.ToLower(strings.TrimSpace(code)); len(code) != 2 {
		return "", netip.Addr{}
	}
	if parsed, err := netip.ParseAddr(strings.TrimSpace(answer.IP)); err == nil {
		ip = parsed
	}
	return code, ip
}

func ParseExitTrace(body []byte) (region string, ip netip.Addr) {
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch key {
		case "loc":
			if v := strings.ToLower(strings.TrimSpace(value)); len(v) == 2 {
				region = v
			}
		case "ip":
			if v, err := netip.ParseAddr(strings.TrimSpace(value)); err == nil {
				ip = v
			}
		}
	}
	return region, ip
}

// IsRegionUnavailableLocation tells a page that blames the region from an ordinary
// relocation: only the former may record a block for the whole region. A region word next
// to a refusal word counts as one; either word alone is an ordinary page.
func IsRegionUnavailableLocation(location string) bool {
	if location == "" {
		return false
	}
	location = strings.ToLower(location)
	for _, pattern := range []string{
		"app-unavailable-in-region",
		"/welcome/unavailable",
		"unavailable-in-region",
		"region-unavailable",
		"not-available-in-your-region",
		"unsupported-region",
	} {
		if strings.Contains(location, pattern) {
			return true
		}
	}
	region := false
	for _, word := range []string{"region", "country", "location", "geo-block", "geoblock", "geo-restrict", "georestrict"} {
		if strings.Contains(location, word) {
			region = true
			break
		}
	}
	if !region {
		return false
	}
	for _, word := range []string{"unavailable", "not-available", "notavailable", "unsupported", "blocked", "restricted", "denied"} {
		if strings.Contains(location, word) {
			return true
		}
	}
	return false
}

// IsRegionUnavailableText recognizes a refusal that is only stated in the page body.
func IsRegionUnavailableText(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	text := strings.ReplaceAll(strings.ToLower(string(body)), "_", " ")
	for _, phrase := range []string{
		"not available in your region",
		"not available in your country",
		"not available in your area",
		"not available in your location",
		"not available in your territory",
		"unavailable in your region",
		"unavailable in your country",
		"not available in this region",
		"not available in this country",
		"not available in the region",
		"unsupported country",
		"unsupported region",
		"region is not supported",
		"country is not supported",
		"not supported in your country",
		"not supported in your region",
		"blocked in your region",
		"blocked in your country",
		"geo-blocked",
		"geoblocked",
		"geo-restricted",
		"georestricted",
		"地区不可用",
		"所在地区不可用",
		"不支持您所在的",
		"您所在的地区",
		"お住まいの地域",
		"お住まいの国",
		"地域ではご利用",
	} {
		if strings.Contains(text, phrase) {
			return true
		}
	}
	return false
}

// ExitKey derives a pseudonymous identity of an exit address: nodes behind the same
// machine share it, and the address itself is never kept.
func ExitKey(ip netip.Addr) string {
	sum := sha256.Sum256(ip.AsSlice())
	return hex.EncodeToString(sum[:6])
}
