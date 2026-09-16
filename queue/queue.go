package queue

import (
	"context"
)

// Queue defines a thread-safe generic queue interface that supports concurrent
// pushing, popping, length inspection, and notification signaling via a channel.
type Queue[T any] interface {
	// push adds a new task to the queue.
	Push(task T)
	// Pop removes and returns the next task from the queue along with a boolean indicating success.
	Pop() (T, bool)
	// Len returns the current number of elements in the queue.
	Len() int

	Peek() (T, bool)
}

// AddQueue embeds the queue into a concurrent processing pipeline, reading tasks from
// the input channel, storing them in the queue, and writing them out to the returned output channel.
// It respects context cancellation for stopping the processing stages.
func AddQueue[T any](ctx context.Context, inp <-chan T, opts ...option) (out chan T, queue Queue[T]) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	switch cfg.qType {
	case typeUnsafeRing:
		queue = newUnsafeRingQueue[T](cfg.initCap)
	case typeRing:
		queue = newUnsafeListQueue[T]()
	case typeUnsafeList:
		queue = newRingQueue[T](cfg.initCap)
	case typeList:
		queue = newListQueue[T]()
	}

	switch cfg.qType {
	case typeUnsafeRing, typeUnsafeList:
		out = make(chan T, 1)
		go streamer(ctx, inp, queue, out)
	case typeRing, typeList:
		out = make(chan T)
		notify := make(chan struct{}, 1)
		go reader(inp, queue, notify)
		go writer(ctx, queue, notify, out)
	}

	return out, queue
}

// inpProcess reads items from the input channel and pushes them into the queue,
// notifying the inner channel on each push. It closes the inner channel when input is exhausted.
func reader[T any](inp <-chan T, q Queue[T], notify chan struct{}) {
	defer close(notify)
	for value := range inp {
		q.Push(value)

		select {
		case notify <- struct{}{}:
		default:
		}
	}
}

// outProcess reads items from the queue and sends them to the output channel
// when signaled by the inner channel. It stops when the context is cancelled or the inner channel closes.
func writer[T any](ctx context.Context, q Queue[T], notify chan struct{}, out chan T) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-notify:
			for {
				task, hasTask := q.Peek()
				if !hasTask {
					break
				}
				select {
				case out <- task:
					q.Pop()
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

func streamer[T any](ctx context.Context, inp <-chan T, queue Queue[T], out chan T) {
	defer close(out)

	var activeInp <-chan T = inp
	var activeOut chan T = nil
	var currentTask T
	var ok bool

	for {
		if activeInp == nil && queue.Len() == 0 {
			return
		}

		currentTask, ok = queue.Peek()
		if ok {
			activeOut = out
		} else {
			activeOut = nil
		}

		select {
		case <-ctx.Done():
			return

		case task, ok := <-activeInp:
			if !ok {
				activeInp = nil
				break
			}

			if queue.Len() == 0 {
				select {
				case out <- task:
					continue
				default:
				}
			}

			// Если очередь была НЕ пуста ИЛИ out оказался занят:
			queue.Push(task)

		case activeOut <- currentTask:
			queue.Pop()
		}

	}
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
