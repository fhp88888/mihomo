package smart

import (
	"encoding/json"
	"github.com/metacubex/bbolt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestExitInferenceNeedsIndependentFailuresAndForeignControl(t *testing.T) {
	w := NewExitWatcher(ExitWatcherOptions{WantASN: func() bool { return true }})
	defer w.Close()
	now := time.Now()
	for node, info := range map[string]ExitInfo{
		"a": {Region: "US", ASN: "1", Key: "same"}, "alias": {Region: "US", ASN: "1", Key: "same"},
		"b": {Region: "US", ASN: "1", Key: "other"}, "control": {Region: "JP", ASN: "2", Key: "control"},
		"untried": {Region: "US", ASN: "1", Key: "untried"},
	} {
		w.Store(node, info, now)
	}
	w.NoteSuccess("api.example", "control")
	w.Note("api.example", "a")
	w.Note("api.example", "alias")
	if w.Suspected("api.example", "untried") {
		t.Fatal("same IP aliases counted twice")
	}
	w.Note("api.example", "b")
	if !w.Suspected("api.example", "untried") {
		t.Fatal("independent exits and control did not confirm")
	}
	if w.Suspected("other.example", "untried") || w.Suspected("api.example", "control") {
		t.Fatal("inference escaped target/class")
	}
	w.Clear("api.example", "untried")
	if w.Suspected("api.example", "a") {
		t.Fatal("recovery did not clear inference")
	}

	var outage SuspectTracker
	outage.Note("down.example", "US", "a", now)
	outage.Note("down.example", "US", "b", now)
	if outage.Active() {
		t.Fatal("site outage without control condemned a class")
	}
	outage.NoteSuccess("down.example", "US", "c", now)
	if outage.Active() {
		t.Fatal("same-region control confirmed region restriction")
	}
	outage.NoteSuccess("down.example", "JP", "a", now)
	if outage.Active() {
		t.Fatal("same physical exit used as independent control")
	}
	if len(outage.NoteSuccess("down.example", "JP", "d", now)) != 1 {
		t.Fatal("late foreign control lost pending failures")
	}
}

func TestExitEvidenceWindowAndBoundedRecovery(t *testing.T) {
	var tracker SuspectTracker
	now := time.Now()
	tracker.NoteSuccess("api", "JP", "control", now)
	tracker.Note("api", "US", "a", now)
	if tracker.Note("api", "US", "b", now.Add(suspectConfirmWindow+time.Second)) {
		t.Fatal("expired failures/control confirmed suspicion")
	}
	tracker.NoteSuccess("api", "JP", "newcontrol", now.Add(suspectConfirmWindow+time.Second))
	raisedAt := now.Add(suspectConfirmWindow + 2*time.Second)
	if !tracker.Note("api", "US", "c", raisedAt) {
		t.Fatal("fresh evidence did not confirm")
	}
	if !tracker.Defer("api", "US", "untried", raisedAt, time.Second) {
		t.Fatal("active class was not deferred")
	}
	if !tracker.TryHalfOpen("api", "US", "untried", raisedAt, time.Second) || tracker.TryHalfOpen("api", "US", "another", raisedAt, time.Second) {
		t.Fatal("fallback not bounded to one owner")
	}
	expired := raisedAt.Add(probeRegionalBlockTTL + time.Second)
	if tracker.Defer("api", "US", "recovery", expired, time.Second) {
		t.Fatal("expired class never gets a recovery trial")
	}
	if !tracker.Defer("api", "US", "other", expired, time.Second) {
		t.Fatal("parallel recovery not bounded")
	}
	if tracker.Defer("api", "US", "next", expired.Add(2*time.Second), time.Second) {
		t.Fatal("lease failed to release")
	}
	tracker.Clear("api", "US")
	if tracker.Defer("api", "US", "other", expired, time.Second) {
		t.Fatal("clear retained recovery gate")
	}
}

func TestExitNodeStateSurvivesQueueAndRestart(t *testing.T) {
	store, path := newTestStore(t)
	defer closeTestStore(t, path)
	cfg := filepath.Base(path)
	store.UpdateNodeState("g", cfg, "a", func(s *NodeState) { s.BlockedUntil = 123; s.ThresholdGrade = 2 })
	w := NewExitWatcher(ExitWatcherOptions{Name: "g", Config: cfg, Store: store})
	defer w.Close()
	w.Store("a", ExitInfo{Region: "US", ASN: "42", Key: "hash"}, time.Now())
	w.storeExitState("a", w.Info("a"))
	w.storeExitFailure("a", time.Now())
	raw, ok := store.NodeStateBytes("g", cfg, "a")
	var state NodeState
	if !ok || json.Unmarshal(raw, &state) != nil || state.BlockedUntil != 123 || state.ThresholdGrade != 2 || state.ExitKey != "hash" {
		t.Fatalf("queued update lost fields: %s", raw)
	}
	if err := store.FlushQueue(true); err != nil {
		t.Fatal(err)
	}
	reopened := &Store{} // a fresh store reads persisted data, not the latest-write cache
	restart := NewExitWatcher(ExitWatcherOptions{Name: "g", Config: cfg, Store: reopened})
	defer restart.Close()
	restart.EnsureLoaded("a")
	if restart.Info("a").Key != "hash" || restart.Due("a", exitRegionTTL, exitProbeRetryGap, time.Now()) {
		t.Fatal("restart lost fresh exit or retry gap")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.UpdateNodeState("g", cfg, "a", func(s *NodeState) { s.ThresholdGrade++ })
		}()
	}
	wg.Wait()
	raw, _ = store.NodeStateBytes("g", cfg, "a")
	json.Unmarshal(raw, &state)
	if state.ThresholdGrade != 22 || state.ExitKey != "hash" {
		t.Fatal("concurrent updates lost state")
	}
}

func TestExitASNInferenceHonorsOption(t *testing.T) {
	w := NewExitWatcher(ExitWatcherOptions{})
	defer w.Close()
	now := time.Now()
	w.Store("a", ExitInfo{Region: "US", ASN: "1", Key: "a"}, now)
	w.Store("b", ExitInfo{Region: "JP", ASN: "1", Key: "b"}, now)
	w.Store("c", ExitInfo{Region: "SG", ASN: "2", Key: "c"}, now)
	w.Store("d", ExitInfo{Region: "DE", ASN: "1", Key: "d"}, now)
	w.NoteSuccess("api", "c")
	w.Note("api", "a")
	w.Note("api", "b")
	if w.Suspected("api", "d") {
		t.Fatal("disabled ASN inference active")
	}
	enabled := NewExitWatcher(ExitWatcherOptions{WantASN: func() bool { return true }})
	defer enabled.Close()
	for _, n := range []string{"a", "b", "c", "d"} {
		enabled.Store(n, w.Info(n), now)
	}
	enabled.NoteSuccess("api", "c")
	enabled.Note("api", "a")
	enabled.Note("api", "b")
	if !enabled.Suspected("api", "d") {
		t.Fatal("exit ASN did not pool across regions")
	}
}

func TestExitNodeStateVisibleWhileFlushIsInFlight(t *testing.T) {
	store, path := newTestStore(t)
	defer closeTestStore(t, path)
	cfg := filepath.Base(path)
	store.UpdateNodeState("g", cfg, "a", func(s *NodeState) { s.ThresholdGrade = 0 })
	if err := store.FlushQueue(true); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	writerDone := make(chan error, 1)
	go func() { writerDone <- db.Update(func(_ *bbolt.Tx) error { close(started); <-release; return nil }) }()
	<-started
	store.UpdateNodeState("g", cfg, "a", func(s *NodeState) { s.ThresholdGrade++ })
	flushed := make(chan error, 1)
	go func() { flushed <- store.FlushQueue(true) }()
	deadline := time.Now().Add(time.Second)
	for len(globalOperationQueue.Load()) != 0 {
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("flush did not drain queue")
		}
		time.Sleep(time.Millisecond)
	}
	store.UpdateNodeState("g", cfg, "a", func(s *NodeState) { s.ThresholdGrade++ })
	close(release)
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	raw, _ := store.NodeStateBytes("g", cfg, "a")
	var state NodeState
	json.Unmarshal(raw, &state)
	if state.ThresholdGrade != 2 {
		t.Fatalf("in-flight flush hid queued state: grade %d", state.ThresholdGrade)
	}
}

func TestExitHeldEvidenceKeepsLatestOutcome(t *testing.T) {
	var state ExitState
	state.WithholdSuccess("api", "a")
	state.Withhold("api", "a")
	if len(state.ReleaseSuccess("a")) != 0 || len(state.Release("a")) != 1 {
		t.Fatal("older success survived a newer refusal")
	}
	state.Withhold("api", "a")
	state.WithholdSuccess("api", "a")
	if len(state.Release("a")) != 0 || len(state.ReleaseSuccess("a")) != 1 {
		t.Fatal("older refusal survived recovery")
	}
}

func TestExitStateClearDoesNotResurrectMemoizedData(t *testing.T) {
	store, path := newTestStore(t)
	defer closeTestStore(t, path)
	cfg := filepath.Base(path)
	store.UpdateNodeState("g", cfg, "a", func(s *NodeState) { s.ExitRegion = "US" })
	if err := store.FlushByGroup("g", cfg); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.NodeStateBytes("g", cfg, "a"); ok {
		t.Fatal("cleared node state resurrected from memory")
	}
}
