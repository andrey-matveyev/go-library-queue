package queue

import (
	"context"
)

// Queue defines a thread-safe generic queue interface that supports concurrent
// pushing, popping, length inspection, and notification signaling via a channel.
type Queue[T any] interface {
	// Push adds a new task to the queue.
	Push(task T)
	// Pop removes and returns the next task from the queue along with a boolean indicating success.
	Pop() (T, bool)
	// Len returns the current number of elements in the queue.
	Len() int
	// InnerChan returns an internal channel used to signal outProcess of new items or closure.
	InnerChan() chan struct{}
}

// AddQueue embeds the queue into a concurrent processing pipeline, reading tasks from
// the input channel, storing them in the queue, and writing them out to the returned output channel.
// It respects context cancellation for stopping the processing stages.
func AddQueue[T any](ctx context.Context, queue Queue[T], inp chan T) (out chan T) {
	go inpProcess(inp, queue)

	out = make(chan T)

	go outProcess(ctx, queue, out)
	return out
}

// Export serializes all items currently stored in the queue by draining them and
// applying the provided marshal function.
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

// Import restores items into the queue by unmarshaling raw byte data using the provided
// unmarshal function and pushing each item into the queue.
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

// inpProcess reads items from the input channel and pushes them into the queue,
// notifying the inner channel on each push. It closes the inner channel when input is exhausted.
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

// outProcess reads items from the queue and sends them to the output channel
// when signaled by the inner channel. It stops when the context is cancelled or the inner channel closes.
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

