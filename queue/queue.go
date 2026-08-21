package queue

import (
	"container/list"
	"context"
	"sync"
)

// Queue представляет собой универсальную потокобезопасную очередь для элементов любого типа T.
type Queue[T any] struct {
	mtx        sync.Mutex
	innerChan  chan struct{}
	queueTasks *list.List
}

// NewQueue создает новую очередь для элементов типа T.
func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{
		innerChan:  make(chan struct{}, 1),
		queueTasks: list.New(),
	}
}

// Push добавляет элемент в очередь и нотифицирует читателей.
func (q *Queue[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	q.queueTasks.PushBack(task)
	select {
	case q.innerChan <- struct{}{}:
	default:
	}
}

// Pop извлекает элемент из очереди. Возвращает нулевое значение T и false, если очередь пуста.
func (q *Queue[T]) Pop() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.queueTasks.Len() == 0 {
		var zero T
		return zero, false
	}

	elem := q.queueTasks.Front()
	q.queueTasks.Remove(elem)

	val, ok := elem.Value.(T)
	if !ok {
		var zero T
		return zero, false
	}
	return val, true
}

// Len возвращает текущее количество элементов в очереди потокобезопасно.
func (q *Queue[T]) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.queueTasks.Len()
}

// InpQueue запускает горутину, читающую из входного канала типа T и складывающую в очередь.
func InpQueue[T any](inp chan T) *Queue[T] {
	q := NewQueue[T]()
	go inpProcess(inp, q)
	return q
}

func inpProcess[T any](inp chan T, q *Queue[T]) {
	for value := range inp {
		q.Push(value)
	}
	close(q.innerChan)
}

// OutQueue запускает горутину, читающую из очереди и отправляющую в выходной канал типа T.
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
				task, found := q.Pop()
				if found {
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
