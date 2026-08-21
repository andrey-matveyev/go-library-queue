package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type TestTask struct {
	ID   int
	Data string
}

func TestNewQueue(t *testing.T) {
	q := NewQueue[*TestTask]()

	if q == nil {
		t.Errorf("NewQueue returned nil, expected a pointer to queue")
	}
	if q.innerChan == nil {
		t.Errorf("innerChan was not initialized")
	}
	if cap(q.innerChan) != 1 {
		t.Errorf("innerChan capacity was %d, expected 1", cap(q.innerChan))
	}
	if q.tasks == nil {
		t.Errorf("tasks was not initialized")
	}
	if q.Len() != 0 {
		t.Errorf("tasks was not empty, expected 0 elements")
	}
}

func TestQueuePushPop(t *testing.T) {
	q := NewQueue[*TestTask]()
	task1 := &TestTask{ID: 1, Data: "Task 1"}
	task2 := &TestTask{ID: 2, Data: "Task 2"}

	if poppedTask, found := q.Pop(); found || poppedTask != nil {
		t.Errorf("Pop from empty queue returned %v, %v, expected nil, false", poppedTask, found)
	}

	if err := q.Push(task1); err != nil {
		t.Errorf("Unexpected error on push: %v", err)
	}
	if q.Len() != 1 {
		t.Errorf("After push, queue length was %d, expected 1", q.Len())
	}

	poppedTask, found := q.Pop()
	if !found || poppedTask == nil || poppedTask.ID != 1 {
		t.Errorf("Pop returned %v, %v, expected task1", poppedTask, found)
	}
	if q.Len() != 0 {
		t.Errorf("After pop, queue length was %d, expected 0", q.Len())
	}

	q.Push(task1)
	q.Push(task2)
	if q.Len() != 2 {
		t.Errorf("After two pushes, queue length was %d, expected 2", q.Len())
	}

	poppedTask, found = q.Pop()
	if !found || poppedTask == nil || poppedTask.ID != 1 {
		t.Errorf("First pop returned %v, expected task1", poppedTask)
	}
	poppedTask, found = q.Pop()
	if !found || poppedTask == nil || poppedTask.ID != 2 {
		t.Errorf("Second pop returned %v, expected task2", poppedTask)
	}
	if q.Len() != 0 {
		t.Errorf("After all pops, queue length was %d, expected 0", q.Len())
	}
}

func TestQueuePushClosed(t *testing.T) {
	q := NewQueue[*TestTask]()
	q.Close()

	err := q.Push(&TestTask{ID: 1})
	if err != ErrQueueClosed {
		t.Errorf("Expected ErrQueueClosed, got %v", err)
	}
}

func TestInpProcessBasicFlow(t *testing.T) {
	inp := make(chan *TestTask, 5)
	q := InpQueue(inp)

	for i := range 3 {
		inp <- &TestTask{ID: i}
	}
	time.Sleep(10 * time.Millisecond)

	if q.Len() != 3 {
		t.Errorf("Expected 3 tasks in queue, got %d", q.Len())
	}

	close(inp)
	time.Sleep(10 * time.Millisecond)

	select {
	case <-q.innerChan:
	default:
	}
	select {
	case _, ok := <-q.innerChan:
		if ok {
			t.Errorf("innerChan was not closed by inpProcess")
		}
	default:
	}
}

func TestOutProcessBasicFlow(t *testing.T) {
	q := NewQueue[*TestTask]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := OutQueue(ctx, q)

	task1 := &TestTask{ID: 1}
	task2 := &TestTask{ID: 2}
	q.Push(task1)
	q.Push(task2)

	select {
	case q.innerChan <- struct{}{}:
	default:
	}

	r1 := <-out
	if r1 == nil || r1.ID != 1 {
		t.Errorf("Expected task1, got %v", r1)
	}

	r2 := <-out
	if r2 == nil || r2.ID != 2 {
		t.Errorf("Expected task2, got %v", r2)
	}

	q.Close()
	time.Sleep(50 * time.Millisecond)

	select {
	case <-q.innerChan:
	default:
	}
	select {
	case _, ok := <-q.innerChan:
		if ok {
			t.Errorf("innerChan was not closed")
		}
	default:
	}
	select {
	case _, ok := <-out:
		if ok {
			t.Errorf("out was not closed")
		}
	default:
	}
}

func TestPipelineCancellation(t *testing.T) {
	inp := make(chan *TestTask, 10)
	ctx, cancel := context.WithCancel(context.Background())

	q := InpQueue(inp)
	out := OutQueue(ctx, q)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			inp <- &TestTask{ID: i}
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

	inp <- &TestTask{ID: 999}
	time.Sleep(10 * time.Millisecond)
	if q.Len() == 0 {
		t.Errorf("Expected some tasks to remain in queue after cancellation, got 0")
	}
	wg.Wait()
	close(inp)
	time.Sleep(10 * time.Millisecond)

	select {
	case <-q.innerChan:
	default:
	}
	select {
	case _, ok := <-q.innerChan:
		if ok {
			t.Errorf("innerChan was not closed")
		}
	default:
	}
}

func TestSlowConsumerFastProducer(t *testing.T) {
	inp := make(chan *TestTask, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := InpQueue(inp)
	out := OutQueue(ctx, q)

	numTasks := 20
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < numTasks; i++ {
			inp <- &TestTask{ID: i, Data: fmt.Sprintf("Data %d", i)}
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
	case <-q.innerChan:
	default:
	}
	select {
	case _, ok := <-q.innerChan:
		if ok {
			t.Errorf("innerChan was not closed after all tasks processed")
		}
	default:
	}
}
