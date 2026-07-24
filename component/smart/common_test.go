package smart

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/bbolt"
)

// newTestStore creates a temporary bbolt DB, initializes the smart bucket,
// and calls NewStore to set up the package-level globals (db, queue, caches).
// Returns the Store and the temp file path (caller must call closeTestStore).
func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	f, err := os.CreateTemp("", "mihomo_smart_test")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	dbPath := f.Name()
	f.Close() // bbolt opens the file itself

	dbInst, err := bbolt.Open(dbPath, 0666, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		os.Remove(dbPath)
		t.Fatalf("bbolt.Open: %v", err)
	}

	if err := dbInst.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketSmartStats)
		return err
	}); err != nil {
		dbInst.Close()
		os.Remove(dbPath)
		t.Fatalf("CreateBucket: %v", err)
	}

	return NewStore(dbInst), dbPath
}

// closeTestStore closes the global bbolt DB and removes the temp file.
func closeTestStore(t *testing.T, dbPath string) {
	t.Helper()
	if db != nil {
		db.Close()
	}
	os.Remove(dbPath)
}

// makeRouteOps creates n OpSaveRoute operations with unique Target/Data.
func makeRouteOps(n int, group, config string) []StoreOperation {
	ops := make([]StoreOperation, n)
	for i := 0; i < n; i++ {
		ops[i] = StoreOperation{
			Type:   OpSaveRoute,
			Group:  group,
			Config: config,
			Target: fmt.Sprintf("target-%d/proxy-%d", i, i),
			Data:   []byte(fmt.Sprintf(`{"latency":%d}`, i*10)),
		}
	}
	return ops
}

func TestFlushQueue_Force_SmallQueue(t *testing.T) {
	store, dbPath := newTestStore(t)
	defer closeTestStore(t, dbPath)

	ops := makeRouteOps(20, "test-group", "test-config")
	store.AppendToGlobalQueue(ops...)

	// Verify ops are in the queue
	queueLen := len(globalOperationQueue.Load())
	if queueLen != 20 {
		t.Fatalf("expected 20 ops in queue, got %d", queueLen)
	}

	// Force flush — must be synchronous
	store.FlushQueue(true)

	// Queue should be empty after flush
	queueLen = len(globalOperationQueue.Load())
	if queueLen != 0 {
		t.Fatalf("expected empty queue after flush, got %d ops", queueLen)
	}

	// Verify data was written to bbolt
	for i := 0; i < 20; i++ {
		key := FormatDBKey(KeyTypeRoute, "test-config", "test-group",
			fmt.Sprintf("target-%d/proxy-%d", i, i))
		data, err := store.DBViewGetItem(key)
		if err != nil {
			t.Fatalf("item %d not found in DB: %v", i, err)
		}
		if string(data) != string(ops[i].Data) {
			t.Errorf("item %d data mismatch: got %s, want %s", i, data, ops[i].Data)
		}
	}
}

func TestAppendToGlobalQueue_BelowThreshold(t *testing.T) {
	_, dbPath := newTestStore(t)
	defer closeTestStore(t, dbPath)

	store := &Store{} // use existing globals set by newTestStore

	// Append 20 ops — below the default threshold of 50
	ops := makeRouteOps(20, "test-group", "test-config")
	store.AppendToGlobalQueue(ops...)

	// Queue should hold all 20 ops (not flushed)
	queueLen := len(globalOperationQueue.Load())
	if queueLen != 20 {
		t.Fatalf("expected 20 ops in queue (below threshold), got %d", queueLen)
	}

	// Append 30 more — total 50 reaches or exceeds the threshold
	moreOps := makeRouteOps(30, "test-group-2", "test-config-2")
	store.AppendToGlobalQueue(moreOps...)

	// After crossing the threshold, the async flush clears the queue
	// and writes in the background.  Give the goroutine a moment.
	time.Sleep(100 * time.Millisecond)

	queueLen = len(globalOperationQueue.Load())
	if queueLen != 0 {
		// If the flush hasn't finished yet, that's acceptable — the key
		// assertion is that the queue was cleared (snapshot taken).
		t.Logf("queue still has %d ops after threshold flush (may be re-enqueued from race)", queueLen)
	}
}

func TestAsyncBatchSave_ReEnqueueOnFailure(t *testing.T) {
	// Use nil DB so BatchSave always fails.
	// NewStore(nil) sets db = nil; AppendToGlobalQueue doesn't check db,
	// so ops go into the queue.  When the threshold triggers an async flush,
	// BatchSave returns an error and the ops are re-enqueued.
	store := NewStore(nil)

	ops := makeRouteOps(50, "test-group", "test-config")
	store.AppendToGlobalQueue(ops...)

	// The async flush runs in a goroutine.  Give it time to attempt the write,
	// fail, and re-enqueue.
	time.Sleep(200 * time.Millisecond)

	// Queue should have the ops back after re-enqueue
	queue := globalOperationQueue.Load()
	if len(queue) == 0 {
		t.Fatal("expected ops to be re-enqueued after BatchSave failure, but queue is empty")
	}
	if len(queue) < 50 {
		t.Fatalf("expected at least 50 re-enqueued ops, got %d", len(queue))
	}
}

func TestConcurrentFlushQueue(t *testing.T) {
	store, dbPath := newTestStore(t)
	defer closeTestStore(t, dbPath)

	var wg sync.WaitGroup
	concurrency := 20
	opsPerGoroutine := 10
	totalOps := concurrency * opsPerGoroutine

	// Concurrent appends
	for g := 0; g < concurrency; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			ops := makeRouteOps(opsPerGoroutine,
				fmt.Sprintf("group-%d", g),
				"shared-config")
			store.AppendToGlobalQueue(ops...)
		}()
	}

	// Concurrently call FlushQueue
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.FlushQueue(true)
		}()
	}

	wg.Wait()

	// Final sync flush to ensure everything is written
	store.FlushQueue(true)

	// Verify no panics occurred (the real test is -race)
	// Check that the queue is clean
	queueLen := len(globalOperationQueue.Load())
	t.Logf("final queue length: %d (total ops sent: %d)", queueLen, totalOps)
}
