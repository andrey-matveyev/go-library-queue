package queue

import (
	"context"
	"testing"
)

func benchmarkQueuePipeline(b *testing.B, newQueue func() Queue[*Task], numTasks int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		inp := make(chan *Task, 100)
		out := AddQueue(ctx, newQueue(), inp)

		go func() {
			defer close(inp)
			for j := 0; j < numTasks; j++ {
				inp <- &Task{ID: j, Data: "benchmark task"}
			}
		}()

		for range out {
		}
		cancel()
	}
}

func BenchmarkListQueue_1k(b *testing.B) {
	benchmarkQueuePipeline(b, func() Queue[*Task] { return NewListQueue[*Task]() }, 1000)
}

func BenchmarkRingQueue_1k(b *testing.B) {
	benchmarkQueuePipeline(b, func() Queue[*Task] { return NewRingQueue[*Task](16) }, 1000)
}

func BenchmarkListQueue_10k(b *testing.B) {
	benchmarkQueuePipeline(b, func() Queue[*Task] { return NewListQueue[*Task]() }, 10000)
}

func BenchmarkRingQueue_10k(b *testing.B) {
	benchmarkQueuePipeline(b, func() Queue[*Task] { return NewRingQueue[*Task](16) }, 10000)
}

func BenchmarkListQueue_100k(b *testing.B) {
	benchmarkQueuePipeline(b, func() Queue[*Task] { return NewListQueue[*Task]() }, 100000)
}

func BenchmarkRingQueue_100k(b *testing.B) {
	benchmarkQueuePipeline(b, func() Queue[*Task] { return NewRingQueue[*Task](16) }, 100000)
}
