package queue

import (
	"container/list"
	"sync"
)

var _ Queue[any] = (*ListQueue[any])(nil)

// ListQueue implements a thread-safe FIFO queue backed by container/list.
type ListQueue[T any] struct {
	mtx       sync.Mutex
	items     *list.List
	innerChan chan struct{}
}

// NewListQueue creates and initializes a new instance of ListQueue.
func NewListQueue[T any]() *ListQueue[T] {
	return &ListQueue[T]{
		items:     list.New(),
		innerChan: make(chan struct{}, 1),
	}
}

// Push adds a task to the back of the list queue in a thread-safe manner.
func (q *ListQueue[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	q.items.PushBack(task)

	select {
	case q.innerChan <- struct{}{}:
	default:
	}
}

// Pop removes and returns the task from the front of the list queue, along with a boolean indicating success.
func (q *ListQueue[T]) Pop() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.items.Len() == 0 {
		var zero T
		return zero, false
	}

	elem := q.items.Front()
	q.items.Remove(elem)
	return elem.Value.(T), true
}

// Len returns the current number of elements in the list queue.
func (q *ListQueue[T]) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.items.Len()
}

// InnerChan returns the internal notification channel of the list queue.
func (q *ListQueue[T]) InnerChan() chan struct{} {
	return q.innerChan
}

