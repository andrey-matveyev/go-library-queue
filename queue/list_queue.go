package queue

import (
	"container/list"
	"sync"
)

var (
	_ Queue[any] = (*listQueue[any])(nil)
	_ Queue[any] = (*unsafeListQueue[any])(nil)
)

// listQueue implements a thread-safe FIFO queue backed by container/list.
type listQueue[T any] struct {
	mtx sync.Mutex
	muQ Queue[T]
}

// newListQueue creates and initializes a new instance of ListQueue.
func newListQueue[T any]() *listQueue[T] {
	return &listQueue[T]{
		muQ: newUnsafeListQueue[T](),
	}
}

// Push adds a task to the back of the list queue in a thread-safe manner.
func (q *listQueue[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	q.muQ.Push(task)
}

// Pop removes and returns the task from the front of the list queue, along with a boolean indicating success.
func (q *listQueue[T]) Pop() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.muQ.Pop()
}

func (q *listQueue[T]) Peek() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.muQ.Peek()
}

// Len returns the current number of elements in the list queue.
func (q *listQueue[T]) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.muQ.Len()
}

// -----------------------------
// unsafeListQueue implements
// -----------------------------
type unsafeListQueue[T any] struct {
	items *list.List
}

// NewListQueue creates and initializes a new instance of ListQueue.
func newUnsafeListQueue[T any]() *unsafeListQueue[T] {
	return &unsafeListQueue[T]{
		items: list.New(),
	}
}

// Push adds a task to the back of the list queue in a thread-safe manner.
func (q *unsafeListQueue[T]) Push(task T) {
	q.items.PushBack(task)
}

// Pop removes and returns the task from the front of the list queue, along with a boolean indicating success.
func (q *unsafeListQueue[T]) Pop() (T, bool) {
	if q.items.Len() == 0 {
		var zero T
		return zero, false
	}

	elem := q.items.Front()
	q.items.Remove(elem)
	return elem.Value.(T), true
}

// Pop removes and returns the task from the front of the list queue, along with a boolean indicating success.
func (q *unsafeListQueue[T]) Peek() (T, bool) {
	if q.items.Len() == 0 {
		var zero T
		return zero, false
	}

	elem := q.items.Front()
	return elem.Value.(T), true
}

// Len returns the current number of elements in the list queue.
func (q *unsafeListQueue[T]) Len() int {
	return q.items.Len()
}
