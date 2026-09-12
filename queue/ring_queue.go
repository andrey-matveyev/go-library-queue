package queue

import "sync"

var _ Queue[any] = (*RingQueue[any])(nil)

// RingQueue implements a thread-safe high-performance ring buffer (circular queue).
type RingQueue[T any] struct {
	mtx       sync.Mutex
	items     []T
	head      int
	tail      int
	size      int
	innerChan chan struct{}
}

// NewRingQueue creates and initializes a new RingQueue with the specified initial capacity.
func NewRingQueue[T any](initialCapacity int) *RingQueue[T] {
	if initialCapacity <= 0 {
		initialCapacity = 8
	}
	return &RingQueue[T]{
		items:     make([]T, initialCapacity),
		innerChan: make(chan struct{}, 1), // Buffer of 1 protects Push from blocking
	}
}

// Push adds a task to the ring queue, automatically resizing the underlying buffer if necessary.
func (q *RingQueue[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.size == cap(q.items) {
		q.resize()
	}

	q.items[q.tail] = task
	q.tail = (q.tail + 1) % cap(q.items)
	q.size++

	select {
	case q.innerChan <- struct{}{}:
	default:
	}
}

// Pop removes and returns the next task from the ring queue, along with a boolean indicating success.
func (q *RingQueue[T]) Pop() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.size == 0 {
		var zero T
		return zero, false
	}

	item := q.items[q.head]

	var zero T
	q.items[q.head] = zero // Clear slot for Garbage Collection

	q.head = (q.head + 1) % cap(q.items)
	q.size--

	return item, true
}

// Len returns the current number of elements in the ring queue.
func (q *RingQueue[T]) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.size
}

// resize expands the underlying buffer capacity when the queue is full.
func (q *RingQueue[T]) resize() {
	oldCap := cap(q.items)
	var newCap int

	if oldCap < 256 {
		newCap = oldCap * 2
	} else {
		newCap = oldCap + (oldCap+3*256)/4
	}

	if newCap <= 0 {
		newCap = oldCap + 1
	}

	newItems := make([]T, newCap)
	n1 := copy(newItems, q.items[q.head:])
	copy(newItems[n1:], q.items[:q.head])

	q.items = newItems
	q.head = 0
	q.tail = oldCap
}

// InnerChan returns the internal notification channel of the ring queue.
func (q *RingQueue[T]) InnerChan() chan struct{} {
	return q.innerChan
}

