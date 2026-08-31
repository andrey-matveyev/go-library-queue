package queue

import (
	"context"
)

type Queue[T any] interface {
	Push(task T)
	Pop() (T, bool)
	Len() int
	InnerChan() chan struct{} // Возвращает канал для пробуждения outProcess
}

func AddQueue[T any](ctx context.Context, queue Queue[T], inp chan T) (out chan T) {
	go inpProcess(inp, queue)

	out = make(chan T)

	go outProcess(ctx, queue, out)
	return out
}

func inpProcess[T any](inp chan T, q Queue[T]) {
	for value := range inp {
		q.Push(value)

		select {
		case q.InnerChan() <- struct{}{}:
		default:
		}
	}
	close(q.InnerChan())
}

func outProcess[T any](ctx context.Context, q Queue[T], out chan T) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-q.InnerChan():
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
