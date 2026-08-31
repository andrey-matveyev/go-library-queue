package queue

import (
	"container/list"
	"sync"
)

var _ Queue[any] = (*ListQueue[any])(nil)

type ListQueue[T any] struct {
	mtx        sync.Mutex
	queueTasks *list.List
	innerChan  chan struct{}
}

func NewListQueue[T any]() *ListQueue[T] {
	return &ListQueue[T]{
		queueTasks: list.New(),
		innerChan:  make(chan struct{}, 1),
	}
}

func (q *ListQueue[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	q.queueTasks.PushBack(task)

	select {
	case q.innerChan <- struct{}{}:
	default:
	}
}

func (q *ListQueue[T]) Pop() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.queueTasks.Len() == 0 {
		var zero T
		return zero, false
	}

	elem := q.queueTasks.Front()
	q.queueTasks.Remove(elem)
	return elem.Value.(T), true
}

func (q *ListQueue[T]) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.queueTasks.Len()
}

func (q *ListQueue[T]) SignalChan() <-chan struct{} {
	return q.innerChan
}

func (q *ListQueue[T]) CloseSignal() {
	close(q.innerChan)
}
