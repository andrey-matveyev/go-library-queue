package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type Task struct {
	ID   int
	Data string
}

func TestNewQueue(t *testing.T) {
	t.Run("ListQueue", func(t *testing.T) {
		q := NewListQueue[*Task]()
		if q == nil {
			t.Errorf("NewListQueue returned nil, expected a pointer to queue")
		}
		if q.InnerChan() == nil {
			t.Errorf("innerChan was not initialized")
		}
		if cap(q.InnerChan()) != 1 {
			t.Errorf("innerChan capacity was %d, expected 1", cap(q.InnerChan()))
		}
		if q.Len() != 0 {
			t.Errorf("queue was not empty, expected 0 elements")
		}
	})

	t.Run("RingQueue", func(t *testing.T) {
		q := NewRingQueue[*Task](8)
		if q == nil {
			t.Errorf("NewRingQueue returned nil, expected a pointer to queue")
		}
		if q.InnerChan() == nil {
			t.Errorf("innerChan was not initialized")
		}
		if cap(q.InnerChan()) != 1 {
			t.Errorf("innerChan capacity was %d, expected 1", cap(q.InnerChan()))
		}
		if q.Len() != 0 {
			t.Errorf("queue was not empty, expected 0 elements")
		}
	})
}

func TestQueuePushPop(t *testing.T) {
	queues := map[string]Queue[*Task]{
		"ListQueue": NewListQueue[*Task](),
		"RingQueue": NewRingQueue[*Task](8),
	}

	for name, q := range queues {
		t.Run(name, func(t *testing.T) {
			task1 := &Task{ID: 1, Data: "Task 1"}
			task2 := &Task{ID: 2, Data: "Task 2"}

			if poppedTask, ok := q.Pop(); ok || poppedTask != nil {
				t.Errorf("Pop from empty queue returned (%v, %v), expected (nil, false)", poppedTask, ok)
			}

			q.Push(task1)
			if q.Len() != 1 {
				t.Errorf("After push, queue length was %d, expected 1", q.Len())
			}

			poppedTask, ok := q.Pop()
			if !ok || poppedTask == nil || poppedTask.ID != 1 {
				t.Errorf("Pop returned (%v, %v), expected task1", poppedTask, ok)
			}
			if q.Len() != 0 {
				t.Errorf("After pop, queue length was %d, expected 0", q.Len())
			}

			q.Push(task1)
			q.Push(task2)
			if q.Len() != 2 {
				t.Errorf("After two pushes, queue length was %d, expected 2", q.Len())
			}

			poppedTask, ok = q.Pop()
			if !ok || poppedTask == nil || poppedTask.ID != 1 {
				t.Errorf("First pop returned (%v, %v), expected task1", poppedTask, ok)
			}
			poppedTask, ok = q.Pop()
			if !ok || poppedTask == nil || poppedTask.ID != 2 {
				t.Errorf("Second pop returned (%v, %v), expected task2", poppedTask, ok)
			}
			if q.Len() != 0 {
				t.Errorf("After all pops, queue length was %d, expected 0", q.Len())
			}
		})
	}
}

func TestInpProcessBasicFlow(t *testing.T) {
	queues := map[string]Queue[*Task]{
		"ListQueue": NewListQueue[*Task](),
		"RingQueue": NewRingQueue[*Task](8),
	}

	for name, q := range queues {
		t.Run(name, func(t *testing.T) {
			inp := make(chan *Task, 5)
			go inpProcess(inp, q)

			for i := range 3 {
				inp <- &Task{ID: i}
			}
			time.Sleep(10 * time.Millisecond)

			if q.Len() != 3 {
				t.Errorf("Expected 3 tasks in queue, got %d", q.Len())
			}

			close(inp)
			time.Sleep(10 * time.Millisecond)

			select {
			case <-q.InnerChan():
			default:
			}
			select {
			case _, ok := <-q.InnerChan():
				if ok {
					t.Errorf("innerChan was not closed by inpProcess")
				}
			default:
			}
		})
	}
}

func TestOutProcessBasicFlow(t *testing.T) {
	queues := map[string]func() Queue[*Task]{
		"ListQueue": func() Queue[*Task] { return NewListQueue[*Task]() },
		"RingQueue": func() Queue[*Task] { return NewRingQueue[*Task](8) },
	}

	for name, newQ := range queues {
		t.Run(name, func(t *testing.T) {
			q := newQ()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			out := make(chan *Task)
			go outProcess(ctx, q, out)

			task1 := &Task{ID: 1}
			task2 := &Task{ID: 2}
			q.Push(task1)
			q.Push(task2)

			select {
			case q.InnerChan() <- struct{}{}:
			default:
			}

			select {
			case t1 := <-out:
				if t1 == nil || t1.ID != 1 {
					t.Errorf("Received %v, expected task1", t1)
				}
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Timeout waiting for task1")
			}

			select {
			case t2 := <-out:
				if t2 == nil || t2.ID != 2 {
					t.Errorf("Received %v, expected task2", t2)
				}
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Timeout waiting for task2")
			}
		})
	}
}

func TestAddQueuePipeline(t *testing.T) {
	queues := map[string]func() Queue[*Task]{
		"ListQueue": func() Queue[*Task] { return NewListQueue[*Task]() },
		"RingQueue": func() Queue[*Task] { return NewRingQueue[*Task](8) },
	}

	for name, newQ := range queues {
		t.Run(name, func(t *testing.T) {
			inp := make(chan *Task, 10)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			out := AddQueue(ctx, newQ(), inp)

			expectedTasks := 5
			go func() {
				for i := 1; i <= expectedTasks; i++ {
					inp <- &Task{ID: i, Data: fmt.Sprintf("Task %d", i)}
				}
				close(inp)
			}()

			receivedTasks := 0
			for task := range out {
				receivedTasks++
				if task.ID != receivedTasks {
					t.Errorf("Expected task ID %d, got %d", receivedTasks, task.ID)
				}
			}

			if receivedTasks != expectedTasks {
				t.Errorf("Expected %d tasks, got %d", expectedTasks, receivedTasks)
			}

			time.Sleep(50 * time.Millisecond)
		})
	}
}

func TestPipelineCancellation(t *testing.T) {
	queues := map[string]func() Queue[*Task]{
		"ListQueue": func() Queue[*Task] { return NewListQueue[*Task]() },
		"RingQueue": func() Queue[*Task] { return NewRingQueue[*Task](8) },
	}

	for name, newQ := range queues {
		t.Run(name, func(t *testing.T) {
			inp := make(chan *Task, 10)
			ctx, cancel := context.WithCancel(context.Background())

			q := newQ()
			out := AddQueue(ctx, q, inp)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for i := 0; i < 100; i++ {
					inp <- &Task{ID: i}
					time.Sleep(1 * time.Millisecond)
				}
				wg.Done()
			}()

			time.Sleep(50 * time.Millisecond)
			cancel()
			time.Sleep(50 * time.Millisecond)

			select {
			case _, ok := <-out:
				if ok {
					t.Errorf("out channel is still open after context cancellation")
				}
			default:
			}

			inp <- &Task{ID: 999}
			time.Sleep(10 * time.Millisecond)
			if q.Len() == 0 {
				t.Errorf("Expected some tasks to remain in queue after cancellation, got 0")
			}
			wg.Wait()
			close(inp)
			time.Sleep(10 * time.Millisecond)

			select {
			case <-q.InnerChan():
			default:
			}
			select {
			case _, ok := <-q.InnerChan():
				if ok {
					t.Errorf("innerChan was not closed")
				}
			default:
			}
		})
	}
}

func TestSlowConsumerFastProducer(t *testing.T) {
	queues := map[string]func() Queue[*Task]{
		"ListQueue": func() Queue[*Task] { return NewListQueue[*Task]() },
		"RingQueue": func() Queue[*Task] { return NewRingQueue[*Task](8) },
	}

	for name, newQ := range queues {
		t.Run(name, func(t *testing.T) {
			inp := make(chan *Task, 100)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			q := newQ()
			out := AddQueue(ctx, q, inp)

			numTasks := 20
			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				defer wg.Done()
				for i := 0; i < numTasks; i++ {
					inp <- &Task{ID: i, Data: fmt.Sprintf("Data %d", i)}
					time.Sleep(5 * time.Millisecond)
				}
				close(inp)
			}()

			receivedCount := 0
			for range out {
				receivedCount++
				time.Sleep(50 * time.Millisecond)
			}

			wg.Wait()

			if receivedCount != numTasks {
				t.Errorf("Expected %d tasks, got %d", numTasks, receivedCount)
			}

			select {
			case <-q.InnerChan():
			default:
			}
			select {
			case _, ok := <-q.InnerChan():
				if ok {
					t.Errorf("innerChan was not closed after all tasks processed")
				}
			default:
			}
		})
	}
}
