package pogreb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/akrylysov/pogreb/internal/assert"
)

// mustDB opens a fresh test DB.
func mustDB(t *testing.T) *DB {
	t.Helper()
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	return db
}

// waitAll blocks until every result is available.
func waitAll(t *testing.T, results []*Result) {
	t.Helper()
	for _, r := range results {
		assert.Nil(t, r.Err())
	}
}

// waitOne blocks until the single result is available and returns its error.
func waitOne(r *Result) error {
	<-r.Done()
	return r.Err()
}

// TestBatchConcurrentSubmits verifies operations submitted concurrently by
// multiple goroutines are merged into groups and all of them reach the DB.
func TestBatchConcurrentSubmits(t *testing.T) {
	db := mustDB(t)
	bw := NewBatchWriter(db)

	const goroutines = 16
	const perGoroutine = 200

	var wg sync.WaitGroup
	var allMu sync.Mutex
	var all []*Result
	errCh := make(chan error, goroutines)

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			local := make([]*Result, 0, perGoroutine)
			for i := 0; i < perGoroutine; i++ {
				key := []byte(fmt.Sprintf("g%d-k%d", g, i))
				// Mix Put and Delete: even iterations Put, odd Delete. Every key
				// is unique to a goroutine so cross-goroutine ordering is irrelevant.
				var r *Result
				var err error
				if i%2 == 0 {
					r, err = bw.Put(key, []byte(fmt.Sprintf("v%d", i)))
				} else {
					r, err = bw.Delete(key)
				}
				if err != nil {
					errCh <- err
					return
				}
				local = append(local, r)
			}
			allMu.Lock()
			all = append(all, local...)
			allMu.Unlock()
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("unexpected submission error: %v", err)
	}

	// No operation may stay in memory indefinitely.
	waitAll(t, all)
	assert.Equal(t, goroutines*perGoroutine, len(all))

	// Assert against the real DB contents: even-numbered keys hold the value,
	// odd-numbered keys never existed or were deleted.
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perGoroutine; i++ {
			key := []byte(fmt.Sprintf("g%d-k%d", g, i))
			v, err := db.Get(key)
			assert.Nil(t, err)
			if i%2 == 0 {
				assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), v)
			} else {
				assert.Nil(t, v)
			}
		}
	}

	assert.Nil(t, bw.Close())
	assert.Nil(t, db.Close())
}

// TestBatchSameKeyOrdering verifies that operations on the same key take
// effect in submission order.
func TestBatchSameKeyOrdering(t *testing.T) {
	db := mustDB(t)
	bw := NewBatchWriter(db)

	key := []byte("same-key")

	// Two consecutive Puts: the last value wins.
	r1, err := bw.Put(key, []byte("first"))
	assert.Nil(t, err)
	r2, err := bw.Put(key, []byte("second"))
	assert.Nil(t, err)
	assert.Nil(t, waitOne(r1))
	assert.Nil(t, waitOne(r2))
	v, err := db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, []byte("second"), v)

	// Put then Delete: the key ends up absent.
	results, err := bw.Submit(PutOp(key, []byte("again")), DeleteOp(key))
	assert.Nil(t, err)
	waitAll(t, results)
	has, err := db.Has(key)
	assert.Nil(t, err)
	assert.Equal(t, false, has)
	v, err = db.Get(key)
	assert.Nil(t, err)
	assert.Nil(t, v)

	// Delete then Put: the new value is readable.
	results, err = bw.Submit(DeleteOp(key), PutOp(key, []byte("revived")))
	assert.Nil(t, err)
	waitAll(t, results)
	has, err = db.Has(key)
	assert.Nil(t, err)
	assert.Equal(t, true, has)
	v, err = db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, []byte("revived"), v)

	assert.Nil(t, bw.Close())
	assert.Nil(t, db.Close())
}

// TestBatchFailureIsolation verifies that a single failing operation reports
// its own error without rolling back already applied operations or stopping
// subsequent groups.
func TestBatchFailureIsolation(t *testing.T) {
	db := mustDB(t)
	bw := NewBatchWriter(db)

	// An oversized key is rejected by the same validation DB.Put applies.
	badKey := make([]byte, MaxKeyLength+1)
	for i := range badKey {
		badKey[i] = 'x'
	}

	// One group, in submission order: good Put, bad Put, good Put.
	results, err := bw.Submit(
		PutOp([]byte("before"), []byte("1")),
		PutOp(badKey, []byte("nope")),
		PutOp([]byte("after"), []byte("2")),
	)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(results))

	// The failure is reported on the corresponding result only.
	assert.Nil(t, waitOne(results[0]))
	assert.Equal(t, errKeyTooLarge, waitOne(results[1]))
	assert.Nil(t, waitOne(results[2]))
	assert.Equal(t, badKey, results[1].Key())

	// Applied operations are not rolled back; operations after the failure ran.
	v, err := db.Get([]byte("before"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("1"), v)
	v, err = db.Get([]byte("after"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("2"), v)

	// The writer keeps accepting and flushing subsequent groups.
	more, err := bw.Submit(
		PutOp([]byte("next-group"), []byte("ok")),
		DeleteOp([]byte("before")),
	)
	assert.Nil(t, err)
	waitAll(t, more)
	v, err = db.Get([]byte("next-group"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("ok"), v)
	has, err := db.Has([]byte("before"))
	assert.Nil(t, err)
	assert.Equal(t, false, has)

	assert.Nil(t, bw.Close())
	assert.Nil(t, db.Close())
}

// TestBatchCloseDrainAndReject verifies that Close flushes in-flight groups,
// delivers results and rejects further submissions.
func TestBatchCloseDrainAndReject(t *testing.T) {
	db := mustDB(t)
	bw := NewBatchWriter(db)

	// Submit a group smaller than max size so it only gets flushed by Close.
	results, err := bw.Submit(
		PutOp([]byte("drained-1"), []byte("a")),
		PutOp([]byte("drained-2"), []byte("b")),
		DeleteOp([]byte("missing")),
	)
	assert.Nil(t, err)

	// Close must drain every in-flight operation before returning.
	assert.Nil(t, bw.Close())
	waitAll(t, results)

	// Real DB contents reflect the drained group.
	v, err := db.Get([]byte("drained-1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("a"), v)
	v, err = db.Get([]byte("drained-2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("b"), v)

	// New submissions are explicitly rejected, never silently dropped.
	_, err = bw.Submit(PutOp([]byte("rejected"), []byte("x")))
	assert.Equal(t, ErrBatchWriterClosed, err)
	_, err = bw.Put([]byte("rejected-2"), []byte("x"))
	assert.Equal(t, ErrBatchWriterClosed, err)
	_, err = bw.Delete([]byte("rejected-3"))
	assert.Equal(t, ErrBatchWriterClosed, err)

	has, err := db.Has([]byte("rejected"))
	assert.Nil(t, err)
	assert.Equal(t, false, has)

	// Closing twice is an explicit error too.
	assert.Equal(t, ErrBatchWriterClosed, bw.Close())

	// The underlying DB remains independently usable and closable.
	err = db.Put([]byte("direct"), []byte("still works"))
	assert.Nil(t, err)
	v, err = db.Get([]byte("direct"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("still works"), v)
	assert.Nil(t, db.Close())
}

// TestBatchIdleFlush verifies that the last partial group is flushed once the
// writer becomes idle, without relying on internal timers.
func TestBatchIdleFlush(t *testing.T) {
	db := mustDB(t)
	bw := NewBatchWriter(db)

	// One small group, well under max size; no further submissions follow.
	r, err := bw.Put([]byte("idle-key"), []byte("idle-value"))
	assert.Nil(t, err)

	// The result is delivered once the idle group is flushed. The test waits on
	// the Result instead of any internal timing.
	assert.Nil(t, waitOne(r))

	v, err := db.Get([]byte("idle-key"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("idle-value"), v)

	assert.Nil(t, bw.Close())
	assert.Nil(t, db.Close())
}

// TestBatchEmptySubmit verifies that an empty submission produces nothing.
func TestBatchEmptySubmit(t *testing.T) {
	db := mustDB(t)
	bw := NewBatchWriter(db)

	results, err := bw.Submit()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(results))
	assert.Nil(t, results)

	// A normal submission afterwards still works.
	r, err := bw.Put([]byte("after-empty"), []byte("v"))
	assert.Nil(t, err)
	assert.Nil(t, waitOne(r))
	v, err := db.Get([]byte("after-empty"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("v"), v)

	assert.Nil(t, bw.Close())
	assert.Nil(t, db.Close())
}

// TestBatchSingleLockPerGroup verifies that all operations in one group are
// applied under a single DB write lock acquisition.
func TestBatchSingleLockPerGroup(t *testing.T) {
	db := mustDB(t)
	bw := NewBatchWriter(db)

	var lockMu sync.Mutex
	groupLocks := 0
	applied := 0
	bw.beforeGroup = func(groupSize int) {
		lockMu.Lock()
		groupLocks++
		lockMu.Unlock()
	}
	bw.beforeApply = func(WriteOp) {
		lockMu.Lock()
		applied++
		lockMu.Unlock()

		// While the group lock is held, trying to take the write lock directly
		// must block: a single writer is allowed at a time.
		acquired := make(chan struct{})
		go func() {
			db.mu.Lock()
			close(acquired)
			db.mu.Unlock()
		}()
		select {
		case <-acquired:
			t.Fatal("DB write lock was available while a group was being applied")
		default:
		}
	}

	const ops = 32 // Smaller than defaultBatchMaxSize, so everything is one group.
	ops2 := make([]WriteOp, ops)
	for i := range ops2 {
		ops2[i] = PutOp([]byte(fmt.Sprintf("lock-k%d", i)), []byte("v"))
	}
	results, err := bw.Submit(ops2...)
	assert.Nil(t, err)
	waitAll(t, results)
	assert.Nil(t, bw.Close())

	lockMu.Lock()
	assert.Equal(t, 1, groupLocks)
	assert.Equal(t, ops, applied)
	lockMu.Unlock()

	assert.Nil(t, db.Close())
}

// TestBatchSyncWrites verifies that when the DB is opened with synchronous
// writes (BackgroundSyncInterval == -1), batched writes are durable with the
// same guarantee as an individual DB.Put: every group flushes through the same
// sync path, and all data is readable afterwards.
func TestBatchSyncWrites(t *testing.T) {
	opts := &Options{BackgroundSyncInterval: -1, FileSystem: testFS}
	db, err := createTestDB(opts)
	assert.Nil(t, err)
	assert.Equal(t, true, db.syncWrites)
	bw := NewBatchWriter(db)

	results, err := bw.Submit(
		PutOp([]byte("sync-1"), []byte("a")),
		PutOp([]byte("sync-2"), []byte("b")),
		DeleteOp([]byte("sync-1")),
	)
	assert.Nil(t, err)
	waitAll(t, results)

	v, err := db.Get([]byte("sync-2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("b"), v)
	has, err := db.Has([]byte("sync-1"))
	assert.Nil(t, err)
	assert.Equal(t, false, has)

	assert.Nil(t, bw.Close())
	// Close the original handle first to release the lock, then reopen and
	// verify the synchronously written data survives a fresh open.
	assert.Nil(t, db.Close())

	db2, err := Open(testDBName, &Options{FileSystem: testFS})
	assert.Nil(t, err)
	v, err = db2.Get([]byte("sync-2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("b"), v)
	has, err = db2.Has([]byte("sync-1"))
	assert.Nil(t, err)
	assert.Equal(t, false, has)
	assert.Nil(t, db2.Close())
}

// TestBatchCompactionConcurrent verifies that background compaction does not
// interfere with batched writes; the writer does not depend on segment layout.
func TestBatchCompactionConcurrent(t *testing.T) {
	opts := &Options{
		BackgroundCompactionInterval: time.Millisecond,
		FileSystem:                   testFS,
	}
	db, err := createTestDB(opts)
	assert.Nil(t, err)
	bw := NewBatchWriter(db)

	const n = 500
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		local := make([]*Result, 0, n)
		for i := 0; i < n; i++ {
			r, err := bw.Put([]byte(fmt.Sprintf("compact-k%d", i)), []byte("v"))
			if err != nil {
				errCh <- err
				return
			}
			local = append(local, r)
		}
		waitAll(t, local)
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("unexpected error during compaction: %v", err)
	}

	assert.Equal(t, uint32(n), db.Count())
	assert.Nil(t, bw.Close())
	assert.Nil(t, db.Close())
}
