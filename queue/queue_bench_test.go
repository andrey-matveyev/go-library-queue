package queue

import (
	"context"
	"testing"
)

// BenchmarkQueuePipeline unbuffered input channel (pure queue + pipeline overhead)
func benchmarkQueuePipelineUnbuffered(b *testing.B, newQueue func() Queue[*Task], numTasks int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		inp := make(chan *Task) // Unbuffered channel
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

// BenchmarkQueueFullDrain extreme scenario: queue is fully filled first, then fully drained
func benchmarkQueueFullDrain(b *testing.B, newQueue func() Queue[*Task], numTasks int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q := newQueue()

		// Phase 1: Fill the queue completely without consumer active
		for j := 0; j < numTasks; j++ {
			q.Push(&Task{ID: j, Data: "drain task"})
		}

		// Phase 2: Create pipeline with an open unbuffered inp channel, close it after starting
		ctx, cancel := context.WithCancel(context.Background())
		inp := make(chan *Task)
		out := AddQueue(ctx, q, inp)

		// Wake up outProcess / innerChan to start draining existing items
		select {
		case q.InnerChan() <- struct{}{}:
		default:
		}

		close(inp)

		for range out {
		}
		cancel()
	}
}

// Unbuffered pipeline benchmarks
func BenchmarkListQueue_1k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, func() Queue[*Task] { return NewListQueue[*Task]() }, 1000)
}

func BenchmarkRingQueue_1k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, func() Queue[*Task] { return NewRingQueue[*Task](16) }, 1000)
}

func BenchmarkListQueue_10k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, func() Queue[*Task] { return NewListQueue[*Task]() }, 10000)
}

func BenchmarkRingQueue_10k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, func() Queue[*Task] { return NewRingQueue[*Task](16) }, 10000)
}

func BenchmarkListQueue_100k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, func() Queue[*Task] { return NewListQueue[*Task]() }, 100000)
}

func BenchmarkRingQueue_100k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, func() Queue[*Task] { return NewRingQueue[*Task](16) }, 100000)
}

// Full Drain benchmarks
func BenchmarkListQueue_1k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewListQueue[*Task]() }, 1000)
}

func BenchmarkRingQueue_1k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewRingQueue[*Task](1000) }, 1000)
}

func BenchmarkListQueue_10k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewListQueue[*Task]() }, 10000)
}

func BenchmarkRingQueue_10k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewRingQueue[*Task](10000) }, 10000)
}

func BenchmarkListQueue_100k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewListQueue[*Task]() }, 100000)
}

func BenchmarkRingQueue_100k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewRingQueue[*Task](100000) }, 100000)
}
