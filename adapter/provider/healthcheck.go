package provider

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"golang.org/x/sync/errgroup"
)

type HealthCheckOption struct {
	URL      string
	Interval uint
}

type extraOption struct {
	expectedStatus utils.IntRanges[uint16]
	filters        map[string]struct{}
}

type HealthCheck struct {
	ctx                        context.Context
	ctxCancel                  context.CancelFunc
	url                        string
	extra                      map[string]*extraOption
	mu                         sync.Mutex
	proxies                    []C.Proxy
	interval                   time.Duration
	lazy                       bool
	expectedStatus             utils.IntRanges[uint16]
	lastTouch                  atomic.TypedValue[time.Time]
	tasks                      map[string]*healthCheckTask
	wg                         sync.WaitGroup
	closed                     bool
	retryInitial, retryMaximum time.Duration
	timeout                    time.Duration
}

func (hc *HealthCheck) process() {
	hc.mu.Lock()
	interval := hc.interval
	hc.mu.Unlock()
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	go hc.check()
	for {
		select {
		case <-ticker.C:
			lastTouch := hc.lastTouch.Load()
			since := time.Since(lastTouch)
			if !hc.lazy || since < interval {
				hc.check()
			} else {
				log.Debugln("Skip once health check because we are lazy")
			}
		case <-hc.ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (hc *HealthCheck) setProxies(proxies []C.Proxy) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.proxies = proxies
}

func (hc *HealthCheck) registerHealthCheckTask(url string, expectedStatus utils.IntRanges[uint16], filter string, interval uint) {
	url = strings.TrimSpace(url)
	if len(url) == 0 || url == hc.url {
		log.Debugln("ignore invalid health check url: %s", url)
		return
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()

	// if the provider has not set up health checks, then modify it to be the same as the group's interval
	if hc.interval == 0 {
		hc.interval = time.Duration(interval) * time.Second
	}

	if hc.extra == nil {
		hc.extra = make(map[string]*extraOption)
	}

	// prioritize the use of previously registered configurations, especially those from provider
	if _, ok := hc.extra[url]; ok {
		// provider default health check does not set filter
		if url != hc.url && len(filter) != 0 {
			splitAndAddFiltersToExtra(filter, hc.extra[url])
		}

		log.Debugln("health check url: %s exists", url)
		return
	}

	option := &extraOption{filters: map[string]struct{}{}, expectedStatus: expectedStatus}
	splitAndAddFiltersToExtra(filter, option)
	hc.extra[url] = option
}

func splitAndAddFiltersToExtra(filter string, option *extraOption) {
	filter = strings.TrimSpace(filter)
	if len(filter) != 0 {
		for _, regex := range strings.Split(filter, "`") {
			regex = strings.TrimSpace(regex)
			if len(regex) != 0 {
				option.filters[regex] = struct{}{}
			}
		}
	}
}

func (hc *HealthCheck) auto() bool {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return hc.interval != 0
}

func (hc *HealthCheck) touch() {
	hc.lastTouch.Store(time.Now())
}

func (hc *HealthCheck) check() {
	hc.mu.Lock()
	urls := make([]string, 0, len(hc.extra)+1)
	urls = append(urls, hc.url)
	for url := range hc.extra {
		urls = append(urls, url)
	}
	hc.mu.Unlock()
	// Launch through the same scheduler as recovery; do not let a slow default
	// URL prevent another URL's check from starting.
	var done []<-chan struct{}
	for _, url := range urls {
		if ch := hc.requestCheck(url, false); ch != nil {
			done = append(done, ch)
		}
	}
	for _, ch := range done {
		select {
		case <-ch:
		case <-hc.ctx.Done():
			return
		}
	}
}

func (hc *HealthCheck) execute(b *errgroup.Group, url, uid string, option *extraOption, proxies []C.Proxy) {
	url = strings.TrimSpace(url)
	if len(url) == 0 {
		log.Debugln("Health Check has been skipped due to testUrl is empty, {%s}", uid)
		return
	}

	var expectedStatus utils.IntRanges[uint16]
	if option != nil {
		expectedStatus = option.expectedStatus
	}

	for _, proxy := range proxies {
		p := proxy
		b.Go(func() error {
			// The shared budget includes all automatic provider checks. Acquire
			// before starting the timeout so waiting for a slot is not a failure.
			select {
			case healthCheckBudget <- struct{}{}:
				defer func() { <-healthCheckBudget }()
			case <-hc.ctx.Done():
				return nil
			}
			ctx, cancel := context.WithTimeout(hc.ctx, hc.timeout)
			defer cancel()
			log.Debugln("Health Checking, proxy: %s, url: %s, id: {%s}", p.Name(), url, uid)
			_, _ = p.URLTest(ctx, url, expectedStatus)
			log.Debugln("Health Checked, proxy: %s, url: %s, alive: %t, delay: %d ms uid: {%s}", p.Name(), url, p.AliveForTestUrl(url), p.LastDelayForTestUrl(url), uid)
			return nil
		})
	}
}

func (hc *HealthCheck) close() {
	hc.mu.Lock()
	if !hc.closed {
		hc.closed = true
		hc.ctxCancel()
		for _, task := range hc.tasks {
			if task.timer != nil {
				task.timer.Stop()
			}
		}
	}
	hc.mu.Unlock()
	hc.wg.Wait()
}

func NewHealthCheck(proxies []C.Proxy, url string, timeout uint, interval uint, lazy bool, expectedStatus utils.IntRanges[uint16]) *HealthCheck {
	if url == "" {
		expectedStatus = nil
		interval = 0
	}
	if timeout == 0 {
		timeout = 5000
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &HealthCheck{
		ctx:            ctx,
		ctxCancel:      cancel,
		proxies:        proxies,
		url:            url,
		timeout:        time.Duration(timeout) * time.Millisecond,
		extra:          map[string]*extraOption{},
		interval:       time.Duration(interval) * time.Second,
		lazy:           lazy,
		expectedStatus: expectedStatus,
		tasks:          make(map[string]*healthCheckTask),
		retryInitial:   10 * time.Second,
		retryMaximum:   5 * time.Minute,
	}
}
