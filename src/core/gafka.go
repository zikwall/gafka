package core

import (
	"context"
	"sync"
	"time"
)

func Gafka(ctx context.Context, c Configuration) *GafkaEmitter {
	gf := new(GafkaEmitter)
	gf.done = make(chan struct{})
	gf.wg = sync.WaitGroup{}

	if c.BatchSize == 0 {
		c.BatchSize = 10
	}

	if c.ReclaimInterval == 0 {
		c.ReclaimInterval = time.Second * 1
	}

	gf.config = c
	gf.context, gf.cancel = context.WithCancel(ctx)

	gf.storage = c.Storage

	// ну тут конечно полный трешак, нужен конкретный ревью
	// возможно от части можно вообще избавиться
	gf.offsets = map[string]map[string]map[int]uint64{}
	gf.topics = make(map[string]int, len(c.Topics))
	gf.messagePools = map[string]chan string{}
	gf.consumers = map[string]map[string]map[int][]int{}
	gf.freePartitions = map[string]map[string]map[int]int{}
	gf.changeNotifier = map[string]chan consumerChangeNotifier{}

	for _, topic := range c.Topics {
		gf.UNSAFE_CreateTopic(topic)
	}

	// надо подумать над целесообразностью запуска в горутине, пока вроде норм
	go gf.initBootstrappedTopicListeners()
	go gf.initConsumerGroupCoordinators()

	return gf
}
