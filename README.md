# Go Library Queue

A high-performance, concurrent pipeline queue library for Go.
It provides both `linked-list` and highly optimized `circular-buffer ring` implementations with full generics (`[T any]`) support. The library features two architectural streaming models: Inline Stream (single-goroutine sequential loop) and Pipeline Stream (split reader/writer goroutines) configured via declarative functional options.

## Features
- **Generic Support**: Built from the ground up for Go 1.18+ ([T any]).
- **Multiple Under-the-Hood Implementations**:
  - `RingQueue`: Backed by a dynamic circular slice buffer (recommended — offers superb CPU cache locality and minimal GC pressure).
  - `ListQueue`: Backed by Go's standard container/list.
- **Advanced Concurrency Models**:
  - **Inline Stream (WithUnsafeRing / WithUnsafeList)**: Lock-free processing loop in a single goroutine for maximum throughput.
  - **Pipeline Stream (WithRing / WithList)**: Asynchronous decoupled reader and writer architecture with built-in synchronization.
- **Data-Driven Persistence**: Seamless state saving and recovery (Import/Export) leveraging the natural lifecycle of Go channels without blocking the running pipeline.

## Concurrency & Safety Warning (Important!)
The library provides both **Thread-Safe** and **Non-Thread-Safe (Unsafe)** data structures:
- **Safe Queues** (`WithRing` / `WithList`): Completely thread-safe. They are protected by an internal `sync.Mutex` and are fully safe for manual method invocations or parallel access.
- **Unsafe Queues** (`WithUnsafeRing` / `WithUnsafeList`): Not thread-safe. Manual invocation of `Push()`, `Pop()`, `Len()`, or global `Export()` / `Import()` functions on these queues while they are being processed by a goroutine will cause a critical Data Race and runtime panic.

🛡️ Best Practice: 
Always interact with **Unsafe** configurations exclusively through the `AddQueue` pipeline engine. Let the internal streaming loops manage the data flow.

## Installation

```bash
go get github.com/andrey-matveyev/go-library-queue
```

## Core Scenarios & Examples

### Scenario 1: Quick Start (Standard Production Execution)

The standard asynchronous pipeline uses thread-safe queues. If the context is canceled, the pipeline shuts down immediately.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"://github.com"
)

type Task struct {
	ID   int
	Data string
}

func main() {
	// Standard context-based cancellation for instant stop
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	inpChan := make(chan Task)
	
	// Create a fully thread-safe Ring Queue pipeline stage
	outChan, _ := queue.AddQueue(ctx, inpChan, queue.WithRing(), queue.WithCapacity(32))

	// Producer
	go func() {
		defer close(inpChan)
		for i := 1; i <= 5; i++ {
			select {
			case inpChan <- Task{ID: i, Data: fmt.Sprintf("Task #%d", i)}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Consumer
	for task := range outChan {
		fmt.Printf("Processed: %s\n", task.Data)
	}
}
```

### Scenario 2: Data-Driven Graceful Shutdown & Persistence (Import/Export)

This is the **recommended production pattern** for saving uncompleted tasks on application shutdown (e.g., on OS signals) and restoring them on the next boot.

Instead of manual locking, it leverages the natural flow of Go channels: **Import** happens by pushing tasks into `inp` first, and **Export** happens by draining the remaining elements from `out` after closing `inp`.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"://github.com"
)

type Task struct {
	ID   int
	Data string
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inpChan := make(chan Task, 1) // Buffered channel for smooth data flow
	
	// Start ultra-high-performance lock-free single-goroutine streamer
	outChan, _ := queue.AddQueue(ctx, inpChan, queue.WithUnsafeRing())

	// ==========================================
	// 1. PHASE ONE: IMPORT (Restore state)
	// ==========================================
	// Read old backup bytes from disk and push them FIRST into the pipeline
	var savedTasks []Task
	backupBytes := []byte(`[{"ID":99,"Data":"Saved Task from previous run"}]`) // Simulated file read
	
	if err := json.Unmarshal(backupBytes, &savedTasks); err == nil {
		for _, task := range savedTasks {
			inpChan <- task // Initial tasks go into the front of the queue
		}
	}

	// Start standard system producers afterwards...
	go func() {
		inpChan <- Task{ID: 1, Data: "Fresh Live Task #1"}
		inpChan <- Task{ID: 2, Data: "Fresh Live Task #2"}
	}()

	// ==========================================
	// 2. PHASE TWO: CONSUMER & BACKUP STORAGE
	// ==========================================
	var isShuttingDown atomic.Bool
	var backupSlice []Task
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for task := range outChan {
			if isShuttingDown.Load() {
				// If the system is shutting down, stop execution and save tasks to backup slice
				backupSlice = append(backupSlice, task)
			} else {
				// Regular business logic execution
				fmt.Printf("Executing: %s\n", task.Data)
			}
		}
	}()

	// ==========================================
	// 3. PHASE THREE: GRACEFUL SHUTDOWN & EXPORT
	// ==========================================
	// Simulate OS termination signal (e.g., SIGTERM)
	log.Println("OS Shutdown signal received. Starting Graceful Shutdown...")
	
	isShuttingDown.Store(true) // 1. Signal the consumer to start backing up tasks
	close(inpChan)             // 2. Close the input channel. Streamer will safely flush everything left in the queue to outChan.
	
	wg.Wait()                  // 3. Wait for outChan to close naturally when the queue becomes empty.

	// 4. Serialize and save what was left in the queue to a hard storage file
	if len(backupSlice) > 0 {
		exportedBytes, err := json.Marshal(backupSlice)
		if err != nil {
			log.Fatalf("Failed to serialize backup: %v", err)
		}
		fmt.Printf("State successfully persisted! Saved %d tasks. Bytes: %s\n", len(backupSlice), string(exportedBytes))
		// os.WriteFile("backup.json", exportedBytes, 0644)
	}
}
```

You can persist queue states (for example, to save application state or backup tasks) using `Export` and `Import`:

```go
// Export queue items to bytes (e.g. JSON)
queueBytes, err := queue.Export(q, func(items []Task) ([]byte, error) {
    return json.Marshal(items)
})
if err == nil {
    _ = os.WriteFile("queue_state.json", queueBytes, 0644)
}

// Import bytes back into a queue
data, err := os.ReadFile("queue_state.json")
if err == nil {
    err = queue.Import(q, data, func(data []byte) ([]Task, error) {
        var items []Task
        err := json.Unmarshal(data, &items)
        return items, err
    })
}
```

## Under the Hood: Optimized Allocation & Ring Resize

### CPU Cache Locality vs Linked Lists

Standard linked lists (`ListQueue`) allocate a separate node object for every single pushed item. This scatters objects randomly across the heap, forcing the CPU to continuously fetch data from slow RAM due to continuous cache misses.

`RingQueue` utilizes a single continuous block of memory (slice). The CPU detects this linear pattern and prefetches entire chunks of queue items directly into its ultra-fast L1/L2 hardware caches, drastically decreasing retrieval overhead.

## Smart Amortized Resizing Strategy

Unlike standard append operations that can trigger frequent allocations, `RingQueue` implements a bifurcated amortized growth strategy during ring expansion:

- **Small buffers (< 256 items)**: Size doubles instantly to accommodate growth.
- **Large buffers (>= 256 items)**: Size increases progressively using a formula (`newCap = oldCap + (oldCap+3*256)/4`) aligned with memory allocator boundaries.

When a resize triggers, the wrapped structure (where `head` might be ahead of `tail`) is correctly unrolled and straightened out linearly into the new slice:

```go
newItems := make([]T, newCap)
n1 := copy(newItems, q.items[q.head:]) // Copy head to end of old slice
copy(newItems[n1:], q.items[:q.head])  // Copy beginning of old slice to tail
```

## Performance & Memory Optimization: Value Types vs Pointers

One of the most powerful features of Go's monomorphized generics architecture is that it avoids object boxing in runtime. If your pipeline passes tasks by value (`Task`) instead of by pointer (`*Task`), memory allocations completely vanish.

### The Impact on Heap Allocations (100k Items Benchmark Result)

- **Passing Pointers (`*Task`)**: Compilers execute escape analysis, pushing each task instantiation to the heap because it crosses goroutine boundaries. This results in ~100,000 allocs/op.
- **Passing Values (`Task`)**: Struct values are copied sequentially directly into the pre-allocated contiguous ring memory slice block. Heap allocations drop to 2 allocs/op (triggered only by internal ring extensions), entirely unburdening the Garbage Collector (GC).

```go
// HIGH ALLOCATION (Escapes to Heap)
inpChan := make(chan *Task)
inpChan <- &Task{ID: 1}

// ZERO ALLOCATION (Contiguous Block Copy)
inpChan := make(chan Task)
inpChan <- Task{ID: 1}
```

## Performance Benchmarks

Actual metrics executed on an Intel(R) Core(TM) i7-8665U CPU @ 1.90GHz:
Run performance benchmarks:
```bash
go test -bench=. -benchmem ./queue/
```

### Pure Extraction Efficiency (FullDrain Benchmark)

When isolating data structure speed without channel signaling bottlenecks, the performance shifts exponentially:

- `ListQueue` (100k Pointers): 13.8 ms total loop execution time.
- `RingQueue` (100k Pointers): 6.5 ms (2.1x faster).
- `UnsafeRingQueue` (100k Pointers): 4.6 ms (2.7x faster).
- `UnsafeRingQueue` (100k Values): 3.4 ms (A jaw-dropping 17.1 nanoseconds per single Push/Pop execution with near-zero memory footprint)!


## Running Tests

Run full package suite verification with built-in race condition tracking:
```bash
go test -v -race ./...
```
Run specialized benchmarks:
```bash
go test -bench=Ring -benchmem ./queue/
```


## More Information

Read the detailed article explaining the design (v1.0.0):  
[Building a Queue for Go Pipelines on dev.to](https://dev.to/andrey_matveyev/building-a-queue-for-go-pipelines-24b)
