package pogreb

import (
	"errors"
	"sync"
	"time"
)

const (
	// defaultBatchMaxSize is the maximum number of operations accumulated in a
	// single in-flight group before it is flushed to the DB.
	defaultBatchMaxSize = 128
	// defaultFlushInterval bounds how long submitted operations may wait in
	// memory before the group they belong to is flushed.
	defaultFlushInterval = 10 * time.Millisecond
)

// ErrBatchWriterClosed is returned by BatchWriter submission methods after the
// BatchWriter has been closed. It makes rejection explicit instead of silently
// dropping operations.
var ErrBatchWriterClosed = errors.New("pogreb: batch writer is closed")

// opType identifies the kind of a write operation.
type opType uint8

const (
	opPut opType = iota
	opDelete
)

// WriteOp is a single Put or Delete operation submitted to a BatchWriter.
// Use PutOp and DeleteOp to construct operations.
type WriteOp struct {
	op    opType
	key   []byte
	value []byte // value is nil for delete operations.
}

// PutOp creates a Put operation that sets key to value.
func PutOp(key, value []byte) WriteOp {
	return WriteOp{op: opPut, key: key, value: value}
}

// DeleteOp creates a Delete operation that removes key.
func DeleteOp(key []byte) WriteOp {
	return WriteOp{op: opDelete, key: key}
}

// Result represents the outcome of a single submitted operation.
// A Result is tied to the operation's key and does not expose any internal
// grouping information.
type Result struct {
	key  []byte
	done chan struct{}
	err  error
}

// Key returns the key of the operation the result belongs to.
func (r *Result) Key() []byte {
	return r.key
}

// Done returns a channel that is closed when the operation has been applied to
// the DB and Err is available.
func (r *Result) Done() <-chan struct{} {
	return r.done
}

// Err blocks until the operation has been applied and returns its error, or nil
// if it succeeded.
func (r *Result) Err() error {
	<-r.done
	return r.err
}

// batchEntry pairs a submitted operation with its result.
type batchEntry struct {
	op     WriteOp
	result *Result
}

// BatchWriter batches concurrent write operations (Put and Delete) against an
// already opened DB. Operations submitted concurrently are merged into groups;
// each group is applied to the DB under a single write lock.
//
// Operations on the same key take effect in their submission order within a
// group. There is no global ordering guarantee across goroutines; callers that
// need cross-key ordering should submit sequentially from the same goroutine.
//
// All BatchWriter methods are safe for concurrent use by multiple goroutines.
// A BatchWriter does not own the underlying DB: closing it does not close the
// DB.
type BatchWriter struct {
	db *DB

	mu            sync.Mutex
	queue         []*batchEntry // Operations waiting to be assembled into a group.
	closed        bool
	wake          chan struct{} // Closed to wake the worker goroutine; replaced on each signal.
	done          chan struct{} // Closed when the worker goroutine exits.
	maxSize       int
	flushInterval time.Duration
	// beforeApply, when set, runs inside the write lock before each operation
	// of a group is applied. Intended for in-package tests only.
	beforeApply func(WriteOp)
	// beforeGroup, when set, runs inside the write lock once per flushed group.
	// Intended for in-package tests only.
	beforeGroup func(groupSize int)
}

// NewBatchWriter wraps an already opened DB and returns a BatchWriter that
// batches writes to it. The DB's lifecycle is unaffected by the BatchWriter.
func NewBatchWriter(db *DB) *BatchWriter {
	bw := &BatchWriter{
		db:            db,
		wake:          make(chan struct{}),
		done:          make(chan struct{}),
		maxSize:       defaultBatchMaxSize,
		flushInterval: defaultFlushInterval,
	}
	go bw.run()
	return bw
}

// Submit enqueues one or more operations and returns one Result per operation,
// in submission order. An empty submission performs no operation and returns no
// results. After Close, Submit returns ErrBatchWriterClosed.
//
// The submitted keys and values are copied, so the caller is free to reuse the
// byte slices after Submit returns.
func (bw *BatchWriter) Submit(ops ...WriteOp) ([]*Result, error) {
	if len(ops) == 0 {
		return nil, nil
	}

	// Reject early without allocating, then re-check under the same lock that
	// guards the queue because Close may run in between.
	bw.mu.Lock()
	if bw.closed {
		bw.mu.Unlock()
		return nil, ErrBatchWriterClosed
	}
	bw.mu.Unlock()

	entries := make([]*batchEntry, len(ops))
	results := make([]*Result, len(ops))
	for i, op := range ops {
		key := append([]byte(nil), op.key...)
		e := &batchEntry{
			op:     WriteOp{op: op.op, key: key},
			result: &Result{key: key, done: make(chan struct{})},
		}
		if op.op == opPut {
			e.op.value = append([]byte(nil), op.value...)
		}
		entries[i] = e
		results[i] = e.result
	}

	bw.mu.Lock()
	if bw.closed {
		bw.mu.Unlock()
		return nil, ErrBatchWriterClosed
	}
	bw.queue = append(bw.queue, entries...)
	bw.signalLocked()
	bw.mu.Unlock()

	return results, nil
}

// Put is a convenience wrapper around Submit for a single Put operation.
func (bw *BatchWriter) Put(key, value []byte) (*Result, error) {
	results, err := bw.Submit(PutOp(key, value))
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// Delete is a convenience wrapper around Submit for a single Delete operation.
func (bw *BatchWriter) Delete(key []byte) (*Result, error) {
	results, err := bw.Submit(DeleteOp(key))
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// Close flushes all in-flight groups, delivers their results and stops the
// background worker. It does not close the underlying DB, whose lifecycle stays
// the caller's responsibility. Closing an already closed BatchWriter returns
// ErrBatchWriterClosed.
func (bw *BatchWriter) Close() error {
	bw.mu.Lock()
	if bw.closed {
		bw.mu.Unlock()
		return ErrBatchWriterClosed
	}
	bw.closed = true
	bw.signalLocked()
	bw.mu.Unlock()

	<-bw.done
	return nil
}

// signalLocked wakes the worker goroutine. The caller must hold bw.mu.
// Each signal replaces the wake channel so signals cannot be lost or merged.
func (bw *BatchWriter) signalLocked() {
	wake := bw.wake
	bw.wake = make(chan struct{})
	close(wake)
}

// run is the single worker goroutine. It assembles queued operations into
// groups and flushes them. A group is flushed when it reaches maxSize, when the
// flush interval elapses, or when the writer is closed, so operations never
// stay in memory indefinitely and the last partial group is flushed on idle.
func (bw *BatchWriter) run() {
	defer close(bw.done)

	timer := time.NewTimer(bw.flushInterval)
	defer timer.Stop()

	for {
		bw.mu.Lock()
		if bw.closed && len(bw.queue) == 0 {
			bw.mu.Unlock()
			return
		}
		wakeCh := bw.wake
		ready := bw.closed || len(bw.queue) >= bw.maxSize
		bw.mu.Unlock()

		if !ready {
			select {
			case <-timer.C:
			case <-wakeCh:
			}
		}
		bw.resetTimer(timer)
		bw.flush()
	}
}

// resetTimer rearms the flush timer, draining a possibly pending tick so stale
// ticks cannot trigger spurious flushes.
func (bw *BatchWriter) resetTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(bw.flushInterval)
}

// flush takes one group out of the queue and applies it to the DB under a
// single write lock. Each operation is executed independently: its error is
// delivered on its own result, applied operations are not rolled back and the
// remaining operations keep executing. A failing operation never stops the
// writer; later groups are handled normally.
func (bw *BatchWriter) flush() {
	bw.mu.Lock()
	if len(bw.queue) == 0 {
		bw.mu.Unlock()
		return
	}
	group := bw.queue
	bw.queue = nil
	bw.mu.Unlock()

	// One lock acquisition for the whole group; individual operations reuse
	// the same in-lock implementation as DB.Put and DB.Delete.
	bw.db.mu.Lock()
	if bw.beforeGroup != nil {
		bw.beforeGroup(len(group))
	}
	for _, entry := range group {
		if bw.beforeApply != nil {
			bw.beforeApply(entry.op)
		}
		err := bw.applyLocked(entry.op)
		entry.result.err = err
		close(entry.result.done)
	}
	bw.db.mu.Unlock()
}

// applyLocked executes a single operation while the caller holds bw.db.mu.
// It preserves the same validation and persistence semantics as DB.Put and
// DB.Delete.
func (bw *BatchWriter) applyLocked(op WriteOp) error {
	switch op.op {
	case opPut:
		if len(op.key) > MaxKeyLength {
			return errKeyTooLarge
		}
		if len(op.value) > MaxValueLength {
			return errValueTooLarge
		}
		bw.db.metrics.Puts.Add(1)
		return bw.db.putLocked(op.key, op.value)
	case opDelete:
		bw.db.metrics.Dels.Add(1)
		return bw.db.delLocked(bw.db.hash(op.key), op.key)
	default:
		return errors.New("pogreb: unknown write operation")
	}
}
