package queue

import (
	"context"
	"testing"
)

/*
   Справка по запуск бенчмарков:

   1. Запуск всех бенчмарков:
      go test -bench=. -benchmem ./queue/

   2. Запуск только стандартных (потокобезопасных) бенчмарков по ссылке:
      go test -bench="^(BenchmarkListQueue|BenchmarkRingQueue)_" -benchmem -skip="(Value|FullDrain_Value)" ./queue/

   3. Запуск только Unsafe-бенчмарков (с буферизацией входного и выходного каналов емкостью 1):
      go test -bench=Unsafe -benchmem ./queue/

   4. Запуск бенчмарков передачи данных по значению (Value):
      go test -bench=Value -benchmem ./queue/

   5. Запуск FullDrain бенчмарков по значению:
      go test -bench=FullDrain_Value -benchmem ./queue/
*/

// BenchmarkQueuePipeline unbuffered input channel (pure queue + pipeline overhead)
func benchmarkQueuePipelineUnbuffered(b *testing.B, opt option, numTasks int) {
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

// BenchmarkQueuePipelineUnsafeBuffered input channel with capacity 1 and buffered output channel
func benchmarkQueuePipelineUnsafeBuffered(b *testing.B, opt option, numTasks int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		inp := make(chan *Task, 1) // Buffered input channel with capacity 1
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

type TaskValue struct {
	ID   int
	Data [32]byte
}

func benchmarkQueuePipelineValue(b *testing.B, opt option, numTasks int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		inp := make(chan TaskValue, 1)
		out, _ := AddQueue(ctx, inp, opt)

		go func() {
			defer close(inp)
			for j := 0; j < numTasks; j++ {
				inp <- TaskValue{ID: j, Data: [32]byte{1, 2, 3}}
			}
		}()

		for range out {
		}
		cancel()
	}
}

func benchmarkQueueFullDrainValue(b *testing.B, createQueue func() Queue[TaskValue], numTasks int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q := createQueue()

		// Phase 1: Fill the queue completely without consumer active
		for j := 0; j < numTasks; j++ {
			q.Push(TaskValue{ID: j, Data: [32]byte{1, 2, 3}})
		}

		// Phase 2: Drain completely
		for j := 0; j < numTasks; j++ {
			_, _ = q.Pop()
		}
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
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newListQueue[*Task]() }, 1000)
}

func BenchmarkRingQueue_1k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newRingQueue[*Task](1000) }, 1000)
}

func BenchmarkListQueue_10k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newListQueue[*Task]() }, 10000)
}

func BenchmarkRingQueue_10k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newRingQueue[*Task](10000) }, 10000)
}

func BenchmarkListQueue_100k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newListQueue[*Task]() }, 100000)
}

func BenchmarkRingQueue_100k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newRingQueue[*Task](100000) }, 100000)
}

// Unsafe stream benchmarks & additional tests
func BenchmarkUnsafeListQueue_1k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnsafeBuffered(b, WithUnsafeList(), 1000)
}

func BenchmarkUnsafeRingQueue_1k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnsafeBuffered(b, WithUnsafeRing(), 1000)
}

func BenchmarkUnsafeListQueue_10k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnsafeBuffered(b, WithUnsafeList(), 10000)
}

func BenchmarkUnsafeRingQueue_10k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnsafeBuffered(b, WithUnsafeRing(), 10000)
}

func BenchmarkUnsafeListQueue_100k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnsafeBuffered(b, WithUnsafeList(), 100000)
}

func BenchmarkUnsafeRingQueue_100k_Unbuffered(b *testing.B) {
	benchmarkQueuePipelineUnsafeBuffered(b, WithUnsafeRing(), 100000)
}

// Full Drain benchmarks for Unsafe queues
func BenchmarkUnsafeListQueue_1k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newUnsafeListQueue[*Task]() }, 1000)
}

func BenchmarkUnsafeRingQueue_1k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newUnsafeRingQueue[*Task](1000) }, 1000)
}

func BenchmarkUnsafeListQueue_10k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newUnsafeListQueue[*Task]() }, 10000)
}

func BenchmarkUnsafeRingQueue_10k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newUnsafeRingQueue[*Task](10000) }, 10000)
}

func BenchmarkUnsafeListQueue_100k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newUnsafeListQueue[*Task]() }, 100000)
}

func BenchmarkUnsafeRingQueue_100k_FullDrain(b *testing.B) {
	benchmarkQueueFullDrain(b, func() Queue[*Task] { return newUnsafeRingQueue[*Task](100000) }, 100000)
}

// Value-based transfer benchmarks
func BenchmarkListQueue_1k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithList(), 1000)
}

func BenchmarkRingQueue_1k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithRing(), 1000)
}

func BenchmarkUnsafeListQueue_1k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithUnsafeList(), 1000)
}

func BenchmarkUnsafeRingQueue_1k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithUnsafeRing(), 1000)
}

func BenchmarkListQueue_10k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithList(), 10000)
}

func BenchmarkUnsafeListQueue_10k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithUnsafeList(), 10000)
}

func BenchmarkUnsafeRingQueue_10k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithUnsafeRing(), 10000)
}

func BenchmarkListQueue_100k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithList(), 100000)
}

func BenchmarkRingQueue_100k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithRing(), 100000)
}

func BenchmarkUnsafeListQueue_100k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithUnsafeList(), 100000)
}

func BenchmarkUnsafeRingQueue_100k_Value(b *testing.B) {
	benchmarkQueuePipelineValue(b, WithUnsafeRing(), 100000)
}

// Value-based FullDrain benchmarks
func BenchmarkListQueue_1k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newListQueue[TaskValue]() }, 1000)
}

func BenchmarkRingQueue_1k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newRingQueue[TaskValue](1000) }, 1000)
}

func BenchmarkUnsafeListQueue_1k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newUnsafeListQueue[TaskValue]() }, 1000)
}

func BenchmarkUnsafeRingQueue_1k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newUnsafeRingQueue[TaskValue](1000) }, 1000)
}

func BenchmarkListQueue_10k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newListQueue[TaskValue]() }, 10000)
}

func BenchmarkRingQueue_10k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newRingQueue[TaskValue](10000) }, 10000)
}

func BenchmarkUnsafeListQueue_10k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newUnsafeListQueue[TaskValue]() }, 10000)
}

func BenchmarkUnsafeRingQueue_10k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newUnsafeRingQueue[TaskValue](10000) }, 10000)
}

func BenchmarkListQueue_100k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newListQueue[TaskValue]() }, 100000)
}

func BenchmarkRingQueue_100k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newRingQueue[TaskValue](100000) }, 100000)
}

func BenchmarkUnsafeListQueue_100k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newUnsafeListQueue[TaskValue]() }, 100000)
}

func BenchmarkUnsafeRingQueue_100k_FullDrain_Value(b *testing.B) {
	benchmarkQueueFullDrainValue(b, func() Queue[TaskValue] { return newUnsafeRingQueue[TaskValue](100000) }, 100000)
}
