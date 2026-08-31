package queue

import (
	"container/list"
	"context"
	"sync"
)

type Queue[T any] interface {
	Push(task T)
	Pop() (T, bool)
	Len() int
	SignalChan() <-chan struct{} // Возвращает канал для пробуждения outProcess
	CloseSignal()                // Сигнализирует о закрытии входящего потока
}

type Queue1[T any] struct {
	mtx        sync.Mutex
	innerChan  chan struct{}
	queueTasks *list.List
}

func NewQueue[T any]() *Queue1[T] {
	item := &Queue1[T]{
		innerChan: make(chan struct{}, 1),
	}
	item.queueTasks = list.New()

	return item
}

func (q *Queue1[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	q.queueTasks.PushBack(task)
	select {
	case q.innerChan <- struct{}{}:
	default:
	}
}

func (q *Queue1[T]) Pop() (T, bool) {
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

func AddQueue[T any](ctx context.Context, inp chan T) (out chan T) {
	out = OutQueue(ctx, InpQueue(inp))
	return out
}

func InpQueue[T any](inp chan T) *Queue1[T] {
	queue := NewQueue[T]()
	go inpProcess(inp, queue)
	return queue
}

func inpProcess[T any](inp chan T, q *Queue1[T]) {
	for value := range inp {
		q.Push(value)

		select {
		case q.innerChan <- struct{}{}:
		default:
		}
	}
	close(q.innerChan)
}

func OutQueue[T any](ctx context.Context, q *Queue1[T]) chan T {
	out := make(chan T)

	go outProcess(ctx, q, out)
	return out
}

func outProcess[T any](ctx context.Context, q *Queue1[T], out chan T) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-q.innerChan:
			for {
				task, hasTask := q.Pop()
				if !hasTask {
					break
				}
				select {
				case out <- task:
				case <-ctx.Done():
					return
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
