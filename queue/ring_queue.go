package queue

import "sync"

var _ Queue[any] = (*RingQueue[any])(nil)

type RingQueue[T any] struct {
	mtx       sync.Mutex
	items     []T
	head      int
	tail      int
	size      int
	innerChan chan struct{}
}

func NewRingQueue[T any](initialCapacity int) *RingQueue[T] {
	if initialCapacity <= 0 {
		initialCapacity = 8
	}
	return &RingQueue[T]{
		items:     make([]T, initialCapacity),
		innerChan: make(chan struct{}, 1), // Буфер 1 защищает Push от блокировки
	}
}

func (q *RingQueue[T]) Push(task T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.size == cap(q.items) {
		q.resize()
	}

	q.items[q.tail] = task
	q.tail = (q.tail + 1) % cap(q.items)
	q.size++

	select {
	case q.innerChan <- struct{}{}:
	default:
	}
}

func (q *RingQueue[T]) Pop() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.size == 0 {
		var zero T
		return zero, false
	}

	item := q.items[q.head]

	var zero T
	q.items[q.head] = zero // Очищаем ячейку для работы GC

	q.head = (q.head + 1) % cap(q.items)
	q.size--

	return item, true
}

func (q *RingQueue[T]) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.size
}

func (q *RingQueue[T]) resize() {
	oldCap := cap(q.items)
	var newCap int

	if oldCap < 256 {
		newCap = oldCap * 2
	} else {
		newCap = oldCap + (oldCap+3*256)/4
	}

	if newCap <= 0 {
		newCap = oldCap + 1
	}

	newItems := make([]T, newCap)
	n1 := copy(newItems, q.items[q.head:])
	copy(newItems[n1:], q.items[:q.head])

	q.items = newItems
	q.head = 0
	q.tail = oldCap
}

func (q *RingQueue[T]) InnerChan() chan struct{} {
	return q.innerChan
}
