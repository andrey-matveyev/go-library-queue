# Go Library Queue

A high-performance, thread-safe concurrent queue library for Go pipelines, providing both linked-list (`ListQueue`) and circular-buffer ring (`RingQueue`) implementations with generic support (`[T any]`).

![design](https://github.com/andrey-matveyev/go-sample-queue/blob/master/design.png)

## Features

- **Generic Support**: Works with any type (`[T any]`).
- **Multiple Implementations**:
  - `ListQueue`: Backed by Go's standard `container/list`.
  - `RingQueue`: Backed by a dynamic circular slice buffer (efficient cache locality, minimal GC pressure).
- **Pipeline Integration**: Seamlessly integrate into worker pipelines (`AddQueue`) using Go channels and context cancellation.
- **Race-Detector Safe**: Fully stress-tested for concurrent producers and consumers.

- **Queue Persistence (Export & Import)**: Functions to serialize and deserialize queue contents (e.g., to/from JSON or storage files) while preserving item ordering.

## Installation

```bash
go get github.com/andrey-matveyev/go-library-queue
```

## Quick Start / Example (`main.go`)

Here is how you can use `ListQueue` or `RingQueue` in a concurrent pipeline:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/andrey-matveyev/go-library-queue/queue"
)

type Task struct {
	ID   int
	Data string
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inpChan := make(chan Task)
	
	// Choose either NewListQueue[Task]() or NewRingQueue[Task](16)
	outChan := queue.AddQueue(ctx, queue.NewRingQueue[Task](16), inpChan)

	go func() {
		defer close(inpChan)
		for i := 1; i <= 5; i++ {
			inpChan <- Task{ID: i, Data: fmt.Sprintf("Task #%d", i)}
		}
	}()

	for task := range outChan {
		fmt.Printf("Processed: %s\n", task.Data)
	}
}
```

## Export and Import (Persistence)

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

## Running Tests & Benchmarks

Run unit tests with the race detector:
```bash
go test -v -race ./...
```

Run performance benchmarks:
```bash
go test -bench=. -benchmem ./queue/
```

## More Information

Read the detailed article explaining the design:  
[Building a Queue for Go Pipelines on dev.to](https://dev.to/andrey_matveyev/building-a-queue-for-go-pipelines-24b)
