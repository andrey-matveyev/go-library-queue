package queue

type queueType int

const (
	typeUnsafeRing queueType = iota
	typeUnsafeList
	typeRing
	typeList
)

type config struct {
	qType        queueType
	initCap      int
	initDataHook func(any)
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

func WithImportData[T any](data []byte, unmarshalFn func([]byte) ([]T, error)) option {
	return func(c *config) {
		if len(data) == 0 || unmarshalFn == nil {
			return
		}

		// Поскольку config не дженериковый, мы прячем дженерик-логику импорта в абстрактное замыкание!
		// Это позволяет структуре config оставаться простой, а AddQueue вызовет эту функцию в нужный момент.
		c.initDataHook = func(q any) {
			if queue, ok := q.(Queue[T]); ok {
				if err := Import(queue, data, unmarshalFn); err == nil {
					return
				}
				/*
					if items, err := unmarshalFn(data); err == nil {
						for _, item := range items {
							queue.Push(item)
						}
					}
				*/
			}
		}
	}
}
