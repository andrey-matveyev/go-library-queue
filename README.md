# Go Queue Library

A thread-safe, generic concurrent queue and pipeline library for Go, designed for smooth producer-consumer decoupling with backpressure handling.

## Features

- **Generic (`[T any]`)**: Type-safe queue supporting elements of any type without `interface{}` casting.
- **Concurrent & Safe**: Built with `sync.Mutex` and channels for efficient goroutine synchronization without busy-waiting.
- **Pipeline Integration**: Seamlessly connect input channels to the queue (`InpQueue`) and queue to output channels (`OutQueue`) with `context.Context` support for graceful cancellation.
- **Error Handling**: Standard Go error handling (`ErrQueueClosed`).
- **Structured Logging**: Built-in support for `log/slog` with functional options (`WithLogger`).

## Installation

```bash
go get github.com/yourusername/go-library-queue/queue
```

## Usage Example

```go
package main

import (
"context"
"fmt"
"time"

"github.com/yourusername/go-library-queue/queue"
)

type Job struct {
ID      int
Payload string
}

func main() {
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Create a typed input channel
inpChan := make(chan *Job)

// Embed queue into the pipeline
outChan := queue.OutQueue(ctx, queue.InpQueue(inpChan))

// Producer goroutine
go func() {
for i := 1; i <= 3; i++ {
job := &Job{ID: i, Payload: fmt.Sprintf("Data #%d", i)}
fmt.Printf("Producer: sending job %d\n", job.ID)
inpChan <- job
time.Sleep(100 * time.Millisecond)
}
close(inpChan) // Closing input channel triggers graceful queue shutdown
}()

// Consumer goroutine
for job := range outChan {
fmt.Printf("Consumer: received job %d with payload '%s'\n", job.ID, job.Payload)
}

fmt.Println("Pipeline finished successfully.")
}
```

## License

MIT License. See [LICENSE.txt](LICENSE.txt) for details.
