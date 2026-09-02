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

func Export[T any](queue Queue[T], marshalFn func(items []T) ([]byte, error)) ([]byte, error) {
	size := queue.Len()
	if size == 0 {
		return marshalFn(nil)
	}

	flatSlice := make([]T, 0, size)

	for {
		item, ok := queue.Pop()
		if !ok {
			break
		}
		flatSlice = append(flatSlice, item)
	}

	return marshalFn(flatSlice)
}

func Import[T any](queue Queue[T], data []byte, unmarshalFn func(data []byte) ([]T, error)) error {
	tempSlice, err := unmarshalFn(data)
	if err != nil {
		return err
	}

	for _, item := range tempSlice {
		queue.Push(item)
	}

	return nil
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
