package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

func TestRingQueueResizeAndWrap(t *testing.T) {
	q := NewRingQueue[*Task](4)
	// Push more than capacity (4) to trigger resize
	for i := 1; i <= 10; i++ {
		q.Push(&Task{ID: i, Data: fmt.Sprintf("Task %d", i)})
	}

	if q.Len() != 10 {
		t.Errorf("Expected length 10 after resize, got %d", q.Len())
	}

	// Pop half and push more to test wrap around
	for i := 1; i <= 5; i++ {
		task, ok := q.Pop()
		if !ok || task.ID != i {
			t.Errorf("Pop %d failed: got (%v, %v)", i, task, ok)
		}
	}

	for i := 11; i <= 15; i++ {
		q.Push(&Task{ID: i, Data: fmt.Sprintf("Task %d", i)})
	}

	if q.Len() != 10 { // 5 remaining + 5 new
		t.Errorf("Expected length 10 after wrap-around push, got %d", q.Len())
	}

	// Pop all remaining
	expectedIDs := []int{6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	for _, expectedID := range expectedIDs {
		task, ok := q.Pop()
		if !ok || task.ID != expectedID {
			t.Errorf("Pop expected ID %d, got (%v, %v)", expectedID, task, ok)
		}
	}

	if q.Len() != 0 {
		t.Errorf("Expected length 0 at end, got %d", q.Len())
	}
}

func TestConcurrentQueueStress(t *testing.T) {
	queues := map[string]Queue[*Task]{
		"ListQueue": NewListQueue[*Task](),
		"RingQueue": NewRingQueue[*Task](4),
	}

	for name, q := range queues {
		t.Run(name, func(t *testing.T) {
			var wg sync.WaitGroup
			workers := 10
			tasksPerWorker := 100

			// Concurrent producers
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for i := 0; i < tasksPerWorker; i++ {
						q.Push(&Task{ID: workerID*1000 + i})
					}
				}(w)
			}

			wg.Wait()

			totalTasks := workers * tasksPerWorker
			if q.Len() != totalTasks {
				t.Errorf("Expected %d tasks, got %d", totalTasks, q.Len())
			}

			// Concurrent consumers
			var mu sync.Mutex
			count := 0

			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						_, ok := q.Pop()
						if !ok {
							return
						}
						mu.Lock()
						count++
						mu.Unlock()
					}
				}()
			}

			wg.Wait()

			if count != totalTasks {
				t.Errorf("Expected to pop %d tasks, got %d", totalTasks, count)
			}
		})
	}
}

func TestExportAndImportWithFileStorage(t *testing.T) {
	queues := map[string]func() Queue[*Task]{
		"ListQueue": func() Queue[*Task] { return NewListQueue[*Task]() },
		"RingQueue": func() Queue[*Task] { return NewRingQueue[*Task](8) },
	}

	tmpFileName := "test_queue_state.json"
	defer os.Remove(tmpFileName)

	for name, newQueue := range queues {
		t.Run(name, func(t *testing.T) {
			// Scenario 1: Empty queue export and save to file, then load into new queue
			t.Run("EmptyQueueExportImport", func(t *testing.T) {
				os.Remove(tmpFileName)
				q := newQueue()

				queueBytes, err := Export(q, func(items []*Task) ([]byte, error) {
					return json.Marshal(items)
				})
				if err != nil {
					t.Fatalf("Failed to export empty queue: %v", err)
				}

				if err := os.WriteFile(tmpFileName, queueBytes, 0644); err != nil {
					t.Fatalf("Failed to write empty queue bytes to file: %v", err)
				}

				// Load from file
				data, err := os.ReadFile(tmpFileName)
				if err != nil {
					t.Fatalf("Failed to read file: %v", err)
				}

				qLoaded := newQueue()
				err = Import(qLoaded, data, func(d []byte) ([]*Task, error) {
					var items []*Task
					if err := json.Unmarshal(d, &items); err != nil {
						return nil, err
					}
					return items, nil
				})
				if err != nil {
					t.Fatalf("Failed to import into empty queue: %v", err)
				}

				if qLoaded.Len() != 0 {
					t.Errorf("Expected loaded queue length to be 0, got %d", qLoaded.Len())
				}
			})

			// Scenario 2: Non-existent file load attempt
			t.Run("NonExistentFileLoad", func(t *testing.T) {
				os.Remove(tmpFileName)
				_, err := os.ReadFile(tmpFileName)
				if err == nil {
					t.Fatalf("Expected error when reading non-existent file, got nil")
				}
			})

			// Scenario 3: Empty file load attempt
			t.Run("EmptyFileLoad", func(t *testing.T) {
				if err := os.WriteFile(tmpFileName, []byte{}, 0644); err != nil {
					t.Fatalf("Failed to write empty file: %v", err)
				}

				data, err := os.ReadFile(tmpFileName)
				if err != nil {
					t.Fatalf("Failed to read empty file: %v", err)
				}

				qLoaded := newQueue()
				err = Import(qLoaded, data, func(d []byte) ([]*Task, error) {
					if len(d) == 0 {
						// Empty file/data -> empty slice or error depending on unmarshal implementation
						return nil, fmt.Errorf("empty data")
					}
					var items []*Task
					if err := json.Unmarshal(d, &items); err != nil {
						return nil, err
					}
					return items, nil
				})

				if err == nil {
					t.Errorf("Expected error when unmarshaling empty file data, got nil")
				}
			})

			// Scenario 4: Series of test tasks - order and presence check after export and import
			t.Run("SeriesOfTasksOrderAndPresence", func(t *testing.T) {
				os.Remove(tmpFileName)
				q := newQueue()

				tasks := []*Task{
					{ID: 101, Data: "Alpha"},
					{ID: 102, Data: "Beta"},
					{ID: 103, Data: "Gamma"},
					{ID: 104, Data: "Delta"},
				}

				for _, task := range tasks {
					q.Push(task)
				}

				if q.Len() != len(tasks) {
					t.Fatalf("Expected queue length %d, got %d", len(tasks), q.Len())
				}

				// Export queue using example client pattern
				queueBytes, err := Export(q, func(items []*Task) ([]byte, error) {
					return json.Marshal(items)
				})
				if err != nil {
					t.Fatalf("Failed to export queue: %v", err)
				}

				// Verify original queue is emptied after Export (since Export pops items)
				if q.Len() != 0 {
					t.Errorf("Expected original queue to be empty after Export, got length %d", q.Len())
				}

				// Save to file
				if err := os.WriteFile(tmpFileName, queueBytes, 0644); err != nil {
					t.Fatalf("Failed to save queue bytes to file: %v", err)
				}

				// Load from file into a new queue
				fileData, err := os.ReadFile(tmpFileName)
				if err != nil {
					t.Fatalf("Failed to read saved state file: %v", err)
				}

				qImported := newQueue()
				err = Import(qImported, fileData, func(d []byte) ([]*Task, error) {
					var items []*Task
					if err := json.Unmarshal(d, &items); err != nil {
						return nil, err
					}
					return items, nil
				})
				if err != nil {
					t.Fatalf("Failed to import queue from file data: %v", err)
				}

				if qImported.Len() != len(tasks) {
					t.Fatalf("Expected imported queue length %d, got %d", len(tasks), qImported.Len())
				}

				// Check exact order and values (FIFO)
				for i, expectedTask := range tasks {
					popped, ok := qImported.Pop()
					if !ok {
						t.Fatalf("Failed to pop item at index %d", i)
					}
					if popped.ID != expectedTask.ID || popped.Data != expectedTask.Data {
						t.Errorf("At index %d: got task ID=%d Data=%s, expected ID=%d Data=%s",
							i, popped.ID, popped.Data, expectedTask.ID, expectedTask.Data)
					}
				}

				if qImported.Len() != 0 {
					t.Errorf("Expected imported queue to be empty after popping all tasks, got %d", qImported.Len())
				}
			})

			// Scenario 5: Corrupted JSON / invalid data handling
			t.Run("CorruptedDataImport", func(t *testing.T) {
				corruptedData := []byte("{invalid-json-content}")
				qLoaded := newQueue()

				err := Import(qLoaded, corruptedData, func(d []byte) ([]*Task, error) {
					var items []*Task
					if err := json.Unmarshal(d, &items); err != nil {
						return nil, err
					}
					return items, nil
				})

				if err == nil {
					t.Errorf("Expected error when importing corrupted JSON data, got nil")
				}
			})
		})
	}
}

