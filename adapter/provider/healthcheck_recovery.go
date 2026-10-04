package provider

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/dlclark/regexp2"
	C "github.com/metacubex/mihomo/constant"
	"golang.org/x/sync/errgroup"
)

// All automatic provider checks share this execution budget, including checks
// for different URLs and providers. Each task also retains its local limit.
var healthCheckBudget = make(chan struct{}, 10)

type healthCheckTask struct {
	scopes       map[string][]string
	done         chan struct{}
	recovering   bool
	next         time.Time
	delay        time.Duration
	lastFinished time.Time
	timer        *time.Timer
}

// taskInputLocked snapshots mutable provider configuration. Recovery is scoped
// to the registered URL and its filters, not to the whole provider.
func (hc *HealthCheck) taskInputLocked(url string) ([]C.Proxy, *extraOption, bool) {
	option := &extraOption{expectedStatus: hc.expectedStatus}
	if url != hc.url {
		extra, ok := hc.extra[url]
		if !ok {
			return nil, nil, false
		}
		option.expectedStatus = extra.expectedStatus
		option.filters = make(map[string]struct{}, len(extra.filters))
		for filter := range extra.filters {
			option.filters[filter] = struct{}{}
		}
	}
	var filterReg *regexp2.Regexp
	if len(option.filters) > 0 {
		filters := make([]string, 0, len(option.filters))
		for filter := range option.filters {
			filters = append(filters, filter)
		}
		filterReg = regexp2.MustCompile(strings.Join(filters, "|"), regexp2.None)
	}
	proxies := make([]C.Proxy, 0, len(hc.proxies))
	for _, p := range hc.proxies {
		if filterReg != nil {
			match, err := filterReg.MatchString(p.Name())
			if err != nil || !match {
				continue
			}
		}
		proxies = append(proxies, p)
	}
	// Proxies are already filtered; execute must not compile/match them again.
	option.filters = nil
	return proxies, option, true
}

func anyHealthy(proxies []C.Proxy, url string) bool {
	for _, p := range proxies {
		if p.AliveForTestUrl(url) {
			return true
		}
	}
	return false
}

func (hc *HealthCheck) recover(ctx context.Context, url string, scope ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ch := hc.requestCheck(strings.TrimSpace(url), true, scope...)
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-hc.ctx.Done():
		return hc.ctx.Err()
	}
}

func (hc *HealthCheck) requestCheck(url string, recovery bool, scope ...string) <-chan struct{} {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if hc.closed || url == "" {
		return nil
	}
	proxies, option, registered := hc.taskInputLocked(url)
	if !registered || len(proxies) == 0 {
		return nil
	}
	task := hc.tasks[url]
	if task == nil {
		task = &healthCheckTask{}
		hc.tasks[url] = task
	}
	if task.recovering && recoveryScopesHealthy(task, proxies, url) {
		hc.resetRecoveryLocked(task)
	}
	if recovery {
		names := make([]string, 0, len(proxies))
		wanted := make(map[string]bool, len(scope))
		for _, name := range scope {
			wanted[name] = true
		}
		var relevant []C.Proxy
		for _, p := range proxies {
			if len(scope) == 0 || wanted[p.Name()] {
				names = append(names, p.Name())
				relevant = append(relevant, p)
			}
		}
		if len(relevant) == 0 {
			return nil
		}
		if !anyHealthy(relevant, url) {
			sort.Strings(names)
			key, _ := json.Marshal(names)
			if task.scopes == nil {
				task.scopes = make(map[string][]string)
			}
			task.scopes[string(key)] = names
		}
	}
	if recovery && len(task.scopes) > 0 && !task.recovering {
		task.recovering = true
		// A just-completed periodic/API check already supplies this failure.
		// Start its backoff instead of issuing an immediate duplicate check.
		if task.done == nil && time.Since(task.lastFinished) < time.Second {
			task.delay = hc.retryInitial
			task.next = task.lastFinished.Add(task.delay)
			task.timer = time.AfterFunc(time.Until(task.next), func() { hc.requestCheck(url, false) })
		}
	}
	if task.done != nil {
		return task.done
	}
	if recovery && !task.recovering {
		return nil
	}
	now := time.Now()
	if task.recovering && now.Before(task.next) {
		return nil
	}
	// Keep the existing one-second coalescing window for ordinary requests.
	if !task.recovering && now.Sub(task.lastFinished) < time.Second {
		return nil
	}
	return hc.startTaskLocked(url, task, proxies, option)
}

func (hc *HealthCheck) startTaskLocked(url string, task *healthCheckTask, proxies []C.Proxy, option *extraOption) <-chan struct{} {
	if task.timer != nil {
		task.timer.Stop()
		task.timer = nil
	}
	task.done = make(chan struct{})
	hc.wg.Add(1)
	go func() {
		defer hc.wg.Done()
		b := new(errgroup.Group)
		b.SetLimit(10)
		hc.execute(b, url, "scheduled", option, proxies)
		_ = b.Wait()
		hc.mu.Lock()
		defer hc.mu.Unlock()
		task.lastFinished = time.Now()
		close(task.done)
		task.done = nil
		// Re-read the current provider/filter set before deciding to retry.
		current, _, _ := hc.taskInputLocked(url)
		if hc.closed || len(current) == 0 || recoveryScopesHealthy(task, current, url) {
			hc.resetRecoveryLocked(task)
			return
		}
		if !task.recovering {
			return
		}
		if task.delay == 0 {
			task.delay = hc.retryInitial
		} else {
			task.delay = nextHealthRetryDelay(task.delay, hc.retryMaximum)
		}
		task.next = task.lastFinished.Add(task.delay)
		task.timer = time.AfterFunc(task.delay, func() { hc.requestCheck(url, false) })
	}()
	return task.done
}

func (hc *HealthCheck) resetRecoveryLocked(task *healthCheckTask) {
	if task.timer != nil {
		task.timer.Stop()
		task.timer = nil
	}
	task.scopes = nil
	task.recovering = false
	task.delay = 0
	task.next = time.Time{}
}

func nextHealthRetryDelay(delay, maximum time.Duration) time.Duration {
	if delay >= maximum/2 {
		return maximum
	}
	return delay * 2
}

// A provider can be shared by groups with different node filters. Recovery
// ends only when each requesting subset has a healthy candidate (or no longer
// exists), even if unrelated nodes were healthy throughout the outage.
func recoveryScopesHealthy(task *healthCheckTask, proxies []C.Proxy, url string) bool {
	for _, names := range task.scopes {
		wanted := make(map[string]bool, len(names))
		for _, name := range names {
			wanted[name] = true
		}
		matched, healthy := false, false
		for _, p := range proxies {
			if wanted[p.Name()] {
				matched = true
				healthy = healthy || p.AliveForTestUrl(url)
			}
		}
		if matched && !healthy {
			return false
		}
	}
	return true
}
