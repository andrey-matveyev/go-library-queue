package queue

import (
	"container/list"
	"context"
	"sync"
)

type Queue[T any] struct {
	mtx        sync.Mutex
	innerChan  chan struct{}
	queueTasks *list.List
}

func NewQueue[T any]() *Queue[T] {
	item := &Queue[T]{
		innerChan: make(chan struct{}, 1),
	}
	item.queueTasks = list.New()

	return item
}

func (q *Queue[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	q.queueTasks.PushBack(task)
	select {
	case q.innerChan <- struct{}{}:
	default:
	}
}

func (q *Queue[T]) Pop() T {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.queueTasks.Len() == 0 {
		var zero T
		return zero
	}

	elem := q.queueTasks.Front()
	q.queueTasks.Remove(elem)
	return elem.Value.(T)
}

func InpQueue[T any](inp chan T) *Queue[T] {
	queue := NewQueue[T]()
	go inpProcess(inp, queue)
	return queue
}

func inpProcess[T any](inp chan T, q *Queue[T]) {
	for value := range inp {

		q.Push(value)
		select {

		case q.innerChan <- struct{}{}:
		default:
		}
	}

	close(q.innerChan)
}

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
			return
		case _, ok := <-q.innerChan:
			for {
				task := q.Pop()
				var zero T
				if any(task) != any(zero) {
					select {
					case out <- task:
					case <-ctx.Done():
						return
					}
				} else {
					break
				}
			}
			if !ok {
				return
			}
		}
	}
}

type Task struct {
	ID   int
	Data string
}

