package queue

import (
	"context"
)

// Queue defines a thread-safe generic queue interface that supports concurrent
// pushing, popping, peeking, length inspection, and task management.
type Queue[T any] interface {
	// Push adds a new task to the queue.
	Push(task T)
	// Pop removes and returns the next task from the queue along with a boolean indicating success.
	Pop() (T, bool)
	// Len returns the current number of elements in the queue.
	Len() int
	// Peek returns the next task from the queue without removing it, along with a boolean indicating success.
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
		queue = newRingQueue[T](cfg.initCap)
	case typeUnsafeList:
		queue = newUnsafeListQueue[T]()
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

// reader reads items from the input channel and pushes them into the queue,
// notifying the inner notification channel on each push. It closes the notification channel when input is exhausted.
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

// writer reads items from the queue and sends them to the output channel
// when signaled by the notification channel. It stops when the context is cancelled or the notification channel closes.
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

// streamer manages high-performance non-thread-safe queues in a single goroutine,
// dynamically multiplexing between reading from the input channel and writing the next queued task to the output channel.
func streamer[T any](ctx context.Context, inp <-chan T, queue Queue[T], out chan T) {
	defer close(out)

	var activeInp <-chan T = inp
	var activeOut chan T = nil
	var currentTask T
	var ok bool

	for {
		// Terminate when input is fully exhausted and the queue is completely drained.
		if activeInp == nil && queue.Len() == 0 {
			return
		}

		// Dynamically enable output channel selection only when there are items available to send.
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

			// Fast path: if queue is empty, attempt to send directly to output without intermediate storage.
			if queue.Len() == 0 {
				select {
				case out <- task:
					continue
				default:
				}
			}

			// If the queue was not empty OR the output channel was blocked, push the task into the queue:
			queue.Push(task)

		case activeOut <- currentTask:
			// Task successfully sent; remove it from the queue.
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
func Import[T any](queue Queue[T], data []byte, unmarshalFn func(dt []byte) ([]T, error)) error {
	tempSlice, err := unmarshalFn(data)
	if err != nil {
		return err
	}

	for _, item := range tempSlice {
		queue.Push(item)
	}

	return nil
}
