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

// конструктор дефолтных настроек
func defaultConfig() config {
	return config{
		qType:   typeUnsafeRing,
		initCap: 16,
	}
}

func WithCapacity(capacity int) option {
	return func(c *config) {
		c.initCap = capacity
	}
}

func WithRing() option {
	return func(c *config) {
		c.qType = typeRing
	}
}

func WithList() option {
	return func(c *config) {
		c.qType = typeList
	}
}

func WithUnsafeRing() option {
	return func(c *config) {
		c.qType = typeUnsafeRing
	}
}

func WithUnsafeList() option {
	return func(c *config) {
		c.qType = typeUnsafeList
	}
}
