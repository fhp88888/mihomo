package smart

import (
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

func TestHostAnswerExpiryPersistenceAndRecovery(t *testing.T) {
	store, path := newTestStore(t)
	defer closeTestStore(t, path)
	cfg := filepath.Base(path)
	meta := &C.Metadata{Host: "api.example.com"}
	t.Cleanup(func() { hostStatusCache.Delete(FormatDBKey(KeyTypeHostFailures, cfg, "group", meta.Host)) })
	store.UpdateHostStatus("group", cfg, meta.Host, meta, "bad", 5, 5, true, true, 2, 2*time.Second)
	nodes, blocked := store.GetHostStatus("group", cfg, meta.Host, 5)
	if nodes["bad"] != 2 || blocked {
		t.Fatalf("answer must exclude node without blocking target: %v %v", nodes, blocked)
	}
	t.Cleanup(func() {
		hostStatusCache.Delete(FormatDBKey(KeyTypeHostFailures, cfg, "group", "other.example.com"))
	})
	other, _ := store.GetHostStatus("group", cfg, "other.example.com", 5)
	if len(other) != 0 {
		t.Fatal("failure leaked to another target")
	}
	if err := store.FlushQueue(true); err != nil {
		t.Fatal(err)
	}
	hostStatusCache.Delete(FormatDBKey(KeyTypeHostFailures, cfg, "group", meta.Host))
	nodes, _ = store.GetHostStatus("group", cfg, meta.Host, 5)
	if nodes["bad"] != 2 {
		t.Fatal("answer not restored from DB")
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(nodes) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		nodes, _ = store.GetHostStatus("group", cfg, meta.Host, 5)
	}
	if len(nodes) != 0 {
		t.Fatal("cached view kept an expired refusal")
	}
	store.UpdateHostStatus("group", cfg, meta.Host, meta, "bad", 5, 5, true, true, 2, time.Minute)
	store.UpdateHostStatus("group", cfg, meta.Host, meta, "bad", 5, 5, false, true, 0, 0)
	if err := store.FlushQueue(true); err != nil {
		t.Fatal(err)
	}
	hostStatusCache.Delete(FormatDBKey(KeyTypeHostFailures, cfg, "group", meta.Host))
	nodes, _ = store.GetHostStatus("group", cfg, meta.Host, 5)
	if len(nodes) != 0 {
		t.Fatal("successful recheck did not clear persisted answer")
	}
}

func TestHostAnswerBackoffSurvivesRecheckScan(t *testing.T) {
	store, path := newTestStore(t)
	defer closeTestStore(t, path)
	cfg := filepath.Base(path)
	meta := &C.Metadata{Host: "reject.example.com"}
	t.Cleanup(func() { hostStatusCache.Delete(FormatDBKey(KeyTypeHostFailures, cfg, "g", meta.Host)) })
	for i := 0; i < 3; i++ {
		store.UpdateHostStatus("g", cfg, meta.Host, meta, "node", 5, 5, true, true, 2, time.Minute)
	}
	key := FormatDBKey(KeyTypeHostFailures, cfg, "g", meta.Host)
	hs, _ := hostStatusCache.Get(key)
	if remaining := time.Until(time.Unix(hs.Codes[2].Nodes["node"], 0)); remaining < 59*time.Minute {
		t.Fatalf("Alpha repeat-refusal backoff missing: %v", remaining)
	}
	checks, err := store.CheckHostStatus("g", cfg, 5)
	if err != nil {
		t.Fatal(err)
	}
	if checks[meta.Host]["node"] != meta.Host {
		t.Fatal("legitimate one-hour backoff discarded as a legacy 24h entry")
	}
	nodes, blocked := store.GetHostStatus("g", cfg, meta.Host, 0)
	if nodes["node"] != 2 || blocked {
		t.Fatal("answer must not trigger target stop-loss")
	}
}

func TestHostAnswerConcurrentRechecks(t *testing.T) {
	store, path := newTestStore(t)
	defer closeTestStore(t, path)
	cfg := filepath.Base(path)
	meta := &C.Metadata{Host: "concurrent.example.com"}
	t.Cleanup(func() { hostStatusCache.Delete(FormatDBKey(KeyTypeHostFailures, cfg, "g", meta.Host)) })
	var wg sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				switch worker {
				case 0:
					store.UpdateHostStatus("g", cfg, meta.Host, meta, "node", 5, 5, true, true, 2, time.Minute)
				case 1:
					store.GetHostStatus("g", cfg, meta.Host, 5)
				case 2:
					if _, err := store.CheckHostStatus("g", cfg, 5); err != nil {
						t.Error(err)
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	if err := store.FlushQueue(true); err != nil {
		t.Fatal(err)
	}
	data, err := store.DBViewGetItem(FormatDBKey(KeyTypeHostFailures, cfg, "g", meta.Host))
	if err != nil {
		t.Fatal(err)
	}
	var hs HostStatus
	if err := json.Unmarshal(data, &hs); err != nil {
		t.Fatal(err)
	}
	if hs.Codes[2].FailCounts["node"] != 40 {
		t.Fatal("concurrent rechecks lost answer history")
	}
}
