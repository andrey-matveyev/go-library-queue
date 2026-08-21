package queue

import (
	"container/list"
	"context"
	"sync"
)


type Queue struct {
	mtx        sync.Mutex
	innerChan  chan struct{}
	queueTasks *list.List
}




func NewQueue() *Queue {
	item := &Queue{
		innerChan: make(chan struct{}, 1),
	}
	item.queueTasks = list.New()

	return item
}





func (q *Queue) Push(task *Task) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	q.queueTasks.PushBack(task)
	select {
	case q.innerChan <- struct{}{}:
	default:
	}
}




func (q *Queue) Pop() *Task {
	q.mtx.Lock()
	defer q.mtx.Unlock()


	if q.queueTasks.Len() == 0 {
		return nil
	}


	elem := q.queueTasks.Front()
	q.queueTasks.Remove(elem)
	return elem.Value.(*Task)
}



func InpQueue(inp chan *Task) *Queue {
	queue := NewQueue()
	go inpProcess(inp, queue)
	return queue
}


func inpProcess(inp chan *Task, q *Queue) {
	for value := range inp {


		q.Push(value)
		select {

		case q.innerChan <- struct{}{}:
		default:
		}
	}

	close(q.innerChan)
}


func OutQueue(ctx context.Context, q *Queue) chan *Task {
	out := make(chan *Task)

	go outProcess(ctx, q, out)
	return out
}


func outProcess(ctx context.Context, q *Queue, out chan *Task) {
	defer close(out)
	for {
		select {



					case <-ctx.Done():
						return
		case _, ok := <-q.innerChan:
			for {

				task := q.Pop()
				if task != nil {
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

type Task struct {
	ID   int
	Data string
}
