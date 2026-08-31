package queue

import (
	"container/list"
	"sync"
)

var _ Queue[any] = (*ListQueue[any])(nil)

type ListQueue[T any] struct {
	mtx       sync.Mutex
	items     *list.List
	innerChan chan struct{}
}

func NewListQueue[T any]() *ListQueue[T] {
	return &ListQueue[T]{
		items:     list.New(),
		innerChan: make(chan struct{}, 1),
	}
}

func (q *ListQueue[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	q.items.PushBack(task)

	select {
	case q.innerChan <- struct{}{}:
	default:
	}
}

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

func (q *ListQueue[T]) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.items.Len()
}

func (q *ListQueue[T]) InnerChan() chan struct{} {
	return q.innerChan
}
