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

type Option func(*config)

// конструктор дефолтных настроек
func defaultConfig() config {
	return config{
		qType:   typeUnsafeRing,
		initCap: 16,
	}
}

func WithCapacity(capacity int) Option {
	return func(c *config) {
		c.initCap = capacity
	}
}

func WithRing() Option {
	return func(c *config) {
		c.qType = typeRing
	}
}

func WithList() Option {
	return func(c *config) {
		c.qType = typeList
	}
}

func WithUnsafeRing() Option {
	return func(c *config) {
		c.qType = typeUnsafeRing
	}
}

func WithUnsafeList() Option {
	return func(c *config) {
		c.qType = typeUnsafeList
	}
}
