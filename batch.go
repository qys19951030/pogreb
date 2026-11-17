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
// All BatchWriter methods are safe for concurrent use by multiple goroutines.
// A BatchWriter does not own the underlying DB: closing it does not close the
// DB.
type BatchWriter struct {
	db *DB

	mu           sync.Mutex
	queue        []*batchEntry // Operations waiting to be assembled into a group.
	closed       bool
	wake         chan struct{} // Closed to wake the worker goroutine; replaced on each signal.
	done         chan struct{} // Closed when the worker goroutine exits.
	maxSize      int
	flushInterval time.Duration
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
	return bw
}

// Submit enqueues one or more operations and returns one Result per operation,
// in submission order. An empty submission performs no operation and returns no
// results. After Close, Submit returns ErrBatchWriterClosed.
func (bw *BatchWriter) Submit(ops ...WriteOp) ([]*Result, error) {
	return nil, nil
}

// Put is a convenience wrapper around Submit for a single Put operation.
func (bw *BatchWriter) Put(key, value []byte) (*Result, error) {
	return nil, nil
}

// Delete is a convenience wrapper around Submit for a single Delete operation.
func (bw *BatchWriter) Delete(key []byte) (*Result, error) {
	return nil, nil
}

// Close flushes all in-flight groups, delivers their results and stops the
// background worker. It does not close the underlying DB.
func (bw *BatchWriter) Close() error {
	return nil
}
