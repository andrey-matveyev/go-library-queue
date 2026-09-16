package queue

import "sync"

var (
	_ Queue[any] = (*ringQueue[any])(nil)
	_ Queue[any] = (*unsafeRingQueue[any])(nil)
)

// ListQueue implements a thread-safe FIFO queue backed by container/list.
type ringQueue[T any] struct {
	mtx sync.Mutex
	muQ Queue[T]
}

// NewListQueue creates and initializes a new instance of ListQueue.
func newRingQueue[T any](initialCapacity int) *ringQueue[T] {
	return &ringQueue[T]{
		muQ: newUnsafeRingQueue[T](initialCapacity),
	}
}

// Push implements [Queue].
func (q *ringQueue[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	q.muQ.Push(task)
}

// Pop implements [Queue].
func (q *ringQueue[T]) Pop() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.muQ.Pop()
}

// Peek implements [Queue].
func (q *ringQueue[T]) Peek() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.muQ.Peek()
}

func (q *ringQueue[T]) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.muQ.Len()
}

// -----------------------------
// unsafeRingQueue implements
// -----------------------------
// Note: If the number of elements grows beyond the maximum capacity representable
// by an integer (int overflow), a panic will occur.
type unsafeRingQueue[T any] struct {
	items []T
	head  int
	tail  int
	size  int
}

// NewRingQueue creates and initializes a new RingQueue with the specified initial capacity.
func newUnsafeRingQueue[T any](initialCapacity int) *unsafeRingQueue[T] {
	if initialCapacity <= 0 {
		initialCapacity = 2
	}
	return &unsafeRingQueue[T]{
		items: make([]T, initialCapacity),
	}
}

// Push adds a task to the ring queue, automatically resizing the underlying buffer if necessary.
func (q *unsafeRingQueue[T]) Push(task T) {
	if q.size == cap(q.items) {
		q.resize()
	}

	q.items[q.tail] = task
	q.tail = (q.tail + 1) % cap(q.items)
	q.size++
}

// Pop removes and returns the next task from the ring queue, along with a boolean indicating success.
func (q *unsafeRingQueue[T]) Pop() (T, bool) {
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

func (q *unsafeRingQueue[T]) Peek() (T, bool) {
	if q.size == 0 {
		var zero T
		return zero, false
	}

	item := q.items[q.head]

	return item, true
}

// Len returns the current number of elements in the ring queue.
func (q *unsafeRingQueue[T]) Len() int {
	return q.size
}

// resize expands the underlying buffer capacity when the queue is full.
func (q *unsafeRingQueue[T]) resize() {
	oldCap := cap(q.items)
	var newCap int

	if oldCap < 256 {
		newCap = oldCap * 2
	} else {
		newCap = oldCap + (oldCap+3*256)/4
	}

	if newCap <= 0 {
		panic("ring queue capacity overflow")
	}

	newItems := make([]T, newCap)
	n1 := copy(newItems, q.items[q.head:])
	copy(newItems[n1:], q.items[:q.head])

	q.items = newItems
	q.head = 0
	q.tail = oldCap
}
