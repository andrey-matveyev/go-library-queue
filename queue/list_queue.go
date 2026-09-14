package queue

import (
	"container/list"
	"sync"
)

var (
	_ Queue[any] = (*ListQueue[any])(nil)
	_ Queue[any] = (*UnsafeListQueue[any])(nil)
)

// ListQueue implements a thread-safe FIFO queue backed by container/list.
type ListQueue[T any] struct {
	mtx sync.Mutex
	muQ Queue[T]
}

// NewListQueue creates and initializes a new instance of ListQueue.
func NewListQueue[T any]() *ListQueue[T] {
	return &ListQueue[T]{
		muQ: NewUnsafeListQueue[T](),
	}
}

// Push adds a task to the back of the list queue in a thread-safe manner.
func (q *ListQueue[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	q.muQ.Push(task)
}

// Pop removes and returns the task from the front of the list queue, along with a boolean indicating success.
func (q *ListQueue[T]) Pop() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.muQ.Pop()
}

func (q *ListQueue[T]) Peek() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.muQ.Peek()
}

// Len returns the current number of elements in the list queue.
func (q *ListQueue[T]) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.muQ.Len()
}

// -----------------------------
// UnsafeListQueue implements
// -----------------------------
type UnsafeListQueue[T any] struct {
	items *list.List
}

// NewListQueue creates and initializes a new instance of ListQueue.
func NewUnsafeListQueue[T any]() *UnsafeListQueue[T] {
	return &UnsafeListQueue[T]{
		items: list.New(),
	}
}

// Push adds a task to the back of the list queue in a thread-safe manner.
func (q *UnsafeListQueue[T]) Push(task T) {
	q.items.PushBack(task)
}

// Pop removes and returns the task from the front of the list queue, along with a boolean indicating success.
func (q *UnsafeListQueue[T]) Pop() (T, bool) {
	if q.items.Len() == 0 {
		var zero T
		return zero, false
	}

	elem := q.items.Front()
	q.items.Remove(elem)
	return elem.Value.(T), true
}

// Pop removes and returns the task from the front of the list queue, along with a boolean indicating success.
func (q *UnsafeListQueue[T]) Peek() (T, bool) {
	if q.items.Len() == 0 {
		var zero T
		return zero, false
	}

	elem := q.items.Front()
	return elem.Value.(T), true
}

// Len returns the current number of elements in the list queue.
func (q *UnsafeListQueue[T]) Len() int {
	return q.items.Len()
}
