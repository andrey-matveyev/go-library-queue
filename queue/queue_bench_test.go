package queue

import (
	"context"
	"testing"
)

// go test -bench=. -benchmem ./queue/

// BenchmarkQueuePipeline unbuffered input channel (pure queue + pipeline overhead)
func benchmarkQueuePipelineUnbuffered(b *testing.B, opt Option, numTasks int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		inp := make(chan *Task) // Unbuffered channel
		out, _ := AddQueue(ctx, inp, opt)

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
func benchmarkQueueFullDrain(b *testing.B, createQueue func() Queue[*Task], numTasks int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q := createQueue()

		// Phase 1: Fill the queue completely without consumer active
		for j := 0; j < numTasks; j++ {
			q.Push(&Task{ID: j, Data: "drain task"})
		}

		// Phase 2: Create pipeline with an open unbuffered inp channel, close it after starting
		ctx, cancel := context.WithCancel(context.Background())
		inp := make(chan *Task)
		// We can test queue draining via queue methods or AddQueue.
		// Wait, if we want to use AddQueue, how do we pass a pre-filled queue?
		// AddQueue creates its own queue based on options.
		// But wait, can we push to queue inside AddQueue or test q.Pop() directly?
		// Let's drain directly using q.Pop() or recreate AddQueue testing.
		for j := 0; j < numTasks; j++ {
			_, _ = q.Pop()
		}
		_ = ctx
		_ = inp
		cancel()
	}
}

// Unbuffered pipeline benchmarks
func BenchmarkListQueue_1k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithList(), 1000)
}

func BenchmarkRingQueue_1k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithRing(), 1000)
}

func BenchmarkListQueue_10k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithList(), 10000)
}

func BenchmarkRingQueue_10k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithRing(), 10000)
}

func BenchmarkListQueue_100k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithList(), 100000)
}

func BenchmarkRingQueue_100k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithRing(), 100000)
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

// Unsafe stream benchmarks & additional tests
func BenchmarkUnsafeListQueue_1k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithUnsafeList(), 1000)
}

func BenchmarkUnsafeRingQueue_1k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithUnsafeRing(), 1000)
}

func BenchmarkUnsafeListQueue_10k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithUnsafeList(), 10000)
}

func BenchmarkUnsafeRingQueue_10k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithUnsafeRing(), 10000)
}

func BenchmarkUnsafeListQueue_100k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithUnsafeList(), 100000)
}

func BenchmarkUnsafeRingQueue_100k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnbuffered(b, WithUnsafeRing(), 100000)
}

// Full Drain benchmarks for Unsafe queues
func BenchmarkUnsafeListQueue_1k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewUnsafeListQueue[*Task]() }, 1000)
}

func BenchmarkUnsafeRingQueue_1k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewUnsafeRingQueue[*Task](1000) }, 1000)
}

func BenchmarkUnsafeListQueue_10k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewUnsafeListQueue[*Task]() }, 10000)
}

func BenchmarkUnsafeRingQueue_10k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewUnsafeRingQueue[*Task](10000) }, 10000)
}

func BenchmarkUnsafeListQueue_100k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewUnsafeListQueue[*Task]() }, 100000)
}

func BenchmarkUnsafeRingQueue_100k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return NewUnsafeRingQueue[*Task](100000) }, 100000)
}


