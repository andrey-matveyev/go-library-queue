package queue

type queueType int

const (
	typeUnsafeRing queueType = iota
	typeUnsafeList
	typeRing
	typeList
)

type config struct {
	qType   queueType
	initCap int
}

type option func(*config)

// defaultConfig returns the default configuration settings for the queue.
func defaultConfig() config {
	return config{
		qType:   typeUnsafeRing,
		initCap: 16,
	}
}

// WithCapacity sets the initial capacity for ring-based queues.
func WithCapacity(capacity int) option {
	return func(c *config) {
		c.initCap = capacity
	}
}

// WithRing configures the pipeline to use a thread-safe ring buffer queue.
func WithRing() option {
	return func(c *config) {
		c.qType = typeRing
	}
}

// WithList configures the pipeline to use a thread-safe linked list queue.
func WithList() option {
	return func(c *config) {
		c.qType = typeList
	}
}

// WithUnsafeRing configures the pipeline to use a high-performance non-thread-safe ring buffer queue.
func WithUnsafeRing() option {
	return func(c *config) {
		c.qType = typeUnsafeRing
	}
}

// WithUnsafeList configures the pipeline to use a high-performance non-thread-safe linked list queue.
func WithUnsafeList() option {
	return func(c *config) {
		c.qType = typeUnsafeList
	}
}
