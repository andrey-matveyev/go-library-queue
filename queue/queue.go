package queue

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

var (
	// ErrQueueClosed is returned when an operation is attempted on a closed queue.
	ErrQueueClosed = errors.New("queue is closed")
)

// Queue represents a thread-safe generic queue for items of any type T.
type Queue[T any] struct {
	mtx       sync.Mutex
	innerChan chan struct{}
	tasks     *list.List
	closed    bool
	logger    *slog.Logger
}

// Option allows configuring the Queue with functional options.
type Option func(*options)

type options struct {
	logger *slog.Logger
}

// WithLogger sets a custom slog.Logger for the queue.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

// NewQueue creates a new generic queue with optional configuration.
func NewQueue[T any](opts ...Option) *Queue[T] {
	opt := options{
		logger: slog.Default(),
	}
	for _, o := range opts {
		o(&opt)
	}

	return &Queue[T]{
		innerChan: make(chan struct{}, 1),
		tasks:     list.New(),
		logger:    opt.logger,
	}
}

// Push adds an item to the queue and notifies consumers.
// Returns ErrQueueClosed if the queue has been closed.
func (q *Queue[T]) Push(item T) error {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.closed {
		q.logger.Warn("attempted to push to a closed queue")
		return ErrQueueClosed
	}

	q.tasks.PushBack(item)
	select {
	case q.innerChan <- struct{}{}:
	default:
	}
	return nil
}

// Pop retrieves and removes an item from the queue.
// Returns the item, true if successful, or zero value of T and false if empty.
func (q *Queue[T]) Pop() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.tasks.Len() == 0 {
		var zero T
		return zero, false
	}

	elem := q.tasks.Front()
	q.tasks.Remove(elem)

	val, ok := elem.Value.(T)
	if !ok {
		var zero T
		q.logger.Error("failed to type assert value from queue list", slog.String("type", fmtTypeName[T]()))
		return zero, false
	}
	return val, true
}

// Len returns the current number of items in the queue safely.
func (q *Queue[T]) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.tasks.Len()
}

// Close gracefully closes the queue, preventing further pushes.
func (q *Queue[T]) Close() {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.closed {
		return
	}
	q.closed = true
	close(q.innerChan)
	q.logger.Debug("queue closed")
}

// InpQueue starts a background goroutine that reads from the input channel of type T and pushes to the Queue.
func InpQueue[T any](inp chan T, opts ...Option) *Queue[T] {
	q := NewQueue[T](opts...)
	go inpProcess(inp, q)
	return q
}

func inpProcess[T any](inp chan T, q *Queue[T]) {
	defer q.Close()
	for value := range inp {
		if err := q.Push(value); err != nil {
			q.logger.Error("failed to push item from input channel", slog.Any("error", err))
			break
		}
	}
}

// OutQueue starts a background goroutine that reads from the Queue and sends items to the output channel of type T.
func OutQueue[T any](ctx context.Context, q *Queue[T]) chan T {
	out := make(chan T)
	go outProcess(ctx, q, out)
	return out
}

func outProcess[T any](ctx context.Context, q *Queue[T], out chan T) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			q.logger.Debug("outProcess cancelled by context")
			return
		case _, ok := <-q.innerChan:
			for {
				item, found := q.Pop()
				if found {
					select {
					case out <- item:
					case <-ctx.Done():
						q.logger.Debug("outProcess cancelled while sending item")
						return
					}
				} else {
					break
				}
			}
			if !ok {
				q.logger.Debug("innerChan closed, outProcess terminating")
				return
			}
		}
	}
}

func fmtTypeName[T any]() string {
	var zero T
	return fmt.Sprintf("%T", zero)
}
