package v2

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

type (
	Topic struct {
		Name       string
		Partitions int
	}

	GafkaEmitter struct {
		mu      sync.RWMutex
		closed  bool
		context context.Context
		cancel  context.CancelFunc

		// topic:group:unique -> message channel
		consumers map[string]map[string]map[int]chan []string

		// topic:group:partition -> offset for consumer group
		offsets map[string]map[string]map[int]uint64

		// topic:partitions
		topics map[string]int

		// topic:group:partition:unique
		partitionListeners map[string]map[string]map[int][]int

		// topic -> message pool
		messagePools map[string]chan string

		// topic -> message storage
		messages map[string]map[int][]string
	}
)

func Gafka(ctx context.Context, topics []Topic) *GafkaEmitter {
	gf := new(GafkaEmitter)
	gf.context, gf.cancel = context.WithCancel(ctx)

	gf.consumers = map[string]map[string]map[int]chan []string{}
	gf.offsets = map[string]map[string]map[int]uint64{}
	gf.topics = make(map[string]int, len(topics))
	gf.messages = map[string]map[int][]string{}
	gf.messagePools = map[string]chan string{}
	gf.partitionListeners = map[string]map[string]map[int][]int{}

	for _, topic := range topics {
		gf.topics[topic.Name] = topic.Partitions
		gf.unsafeCreateTopicPartitions(topic.Name, topic.Partitions)
		gf.consumers[topic.Name] = map[string]map[int]chan []string{}
		gf.offsets[topic.Name] = map[string]map[int]uint64{}
		gf.messagePools[topic.Name] = make(chan string)
		gf.partitionListeners[topic.Name] = map[string]map[int][]int{}
	}

	go gf.listen()
	go gf.consume()

	return gf
}

func (gf *GafkaEmitter) listen() {
	// listen all topic
	for topic, partitions := range gf.topics {
		// listen by parts
		for partition := 1; partition <= partitions; partition++ {

			// make thead listener
			go func(top string, part int) {
				ctx, cancel := context.WithCancel(gf.context)
				defer cancel()
				defer fmt.Println("Cancel topic listener: ", top, " partition: ", part)

				fmt.Println("Make topic listener: ", top, " partition: ", part)

				for {
					select {
					case <-ctx.Done():
						return
					case message := <-gf.messagePools[top]:
						gf.addMessage(top, part, message)
					}
				}

			}(topic, partition)
		}
	}
}

func (gf *GafkaEmitter) consume() {
	//
}

func (gf *GafkaEmitter) addMessage(topic string, part int, message string) {
	gf.mu.Lock()
	gf.messages[topic][part] = append(gf.messages[topic][part], message)
	gf.mu.Unlock()
}

func (gf *GafkaEmitter) Publish(topic string, message string) error {
	gf.mu.RLock()
	if gf.closed {
		gf.mu.RUnlock()

		return errors.New("Send to closed broker")
	}

	if v, ok := gf.consumers[topic]; !ok || v == nil {
		gf.mu.RUnlock()

		return errors.New("Topic not exist")
	}

	gf.mu.RUnlock()

	gf.messagePools[topic] <- message

	return nil
}

func (gf *GafkaEmitter) Subscribe(c context.Context, topic, group string, handle func([] string)) (error, func()) {
	ctx, cancel := context.WithCancel(c)

	gf.mu.RLock()

	if v, ok := gf.consumers[topic]; !ok || v == nil {
		gf.mu.RUnlock()

		return errors.New("Topic not exist"), nil
	}

	gf.mu.RUnlock()

	// temporary unique consumer id
	uniqId := rand.Intn(1000000000-1) + 1
	channel := make(chan []string, 10)

	// global one lock
	gf.mu.Lock()

	// check group already exist
	if _, ok := gf.consumers[topic][group]; !ok {
		// add new cunsumer group for topic
		gf.consumers[topic][group] = map[int]chan []string{}
	}

	// check exist consumer in consumer group
	if _, ok := gf.consumers[topic][group][uniqId]; !ok {
		// add new consumer to consumer group
		gf.consumers[topic][group][uniqId] = make(chan []string)
	}

	// check offsets for consumer group
	if _, ok := gf.offsets[topic][group]; !ok {
		// offsets not exist -> zero
		gf.offsets[topic][group] = map[int]uint64{}
	}

	// check partition listeners
	if _, ok := gf.partitionListeners[topic][group]; !ok {
		// create new listeners
		gf.partitionListeners[topic][group] = map[int][]int{}
	}

	gf.consumers[topic][group][uniqId] = channel

	gf.mu.Unlock()

	go func() {
		fmt.Println("Create consumer for topic: ", topic, " group: ", group, " id: ", uniqId)

		defer func() {
			gf.mu.Lock()
			delete(gf.consumers[topic][group], uniqId)
			gf.mu.Unlock()
		}()

		defer fmt.Println("Cancel consumer for topic: ", topic, " group: ", group, " id: ", uniqId)

		batchSize := 10
		listenPart := []int{}

		listenPart = []int{}

		defer func() {
			gf.mu.Lock()
			defer gf.mu.Unlock()

			remove := func (s []int, i int) []int {
				s[i] = s[len(s)-1]
				return s[:len(s)-1]
			}

			// remove all partition listeners
			for _, part := range listenPart {
				for k, listener := range gf.partitionListeners[topic][group][part] {
					if listener == uniqId {
						gf.partitionListeners[topic][group][part] = remove(gf.partitionListeners[topic][group][part], k)

						fmt.Println("REMOVE LISTENER ", k, uniqId, gf.partitionListeners[topic][group][part])
					}
				}
			}
		}()

	BALANCE:
		gf.mu.Lock()

		listenPart = []int{}

		countConsumerInGroup := len(gf.consumers[topic][group])
		partOneConsumer := float64(len(gf.messages[topic])) / float64(len(gf.consumers[topic][group]))

		fmt.Println(partOneConsumer)

		// 4 + 0.5 => 5
		// 3.9 + 0.5 => 4
		per := int(math.Round(partOneConsumer + 0.49))

		fmt.Println(per)

		fmt.Println("TOPIC ", topic, " GROUP ", group , gf.partitionListeners[topic][group])

		for part := 1; part <= len(gf.messages[topic]); part++ {
			listeners := gf.partitionListeners[topic][group][part]
			count := len(listeners)

			if count < per {
				count++

				listenPart = append(listenPart, part)
				gf.partitionListeners[topic][group][part] = append(gf.partitionListeners[topic][group][part], uniqId)
			}
		}

		fmt.Println("TOPIC ", topic, " GROUP ", group , gf.partitionListeners[topic][group])

		gf.mu.Unlock()

		fmt.Println("TOPIC ", topic, " GROUP ", group , "LISTEN PARTS ", listenPart)

	RETRY:
		select {
		case <-ctx.Done():
			return
		default:
		}

		gf.mu.Lock()
		if countConsumerInGroup != len(gf.consumers[topic][group]) {
			gf.mu.Unlock()

			fmt.Println("BALANCE")

			goto BALANCE
		}

		for _, part := range listenPart {
			offset := gf.unsafePeekOffsetForConsumerGroup(topic, group, part)
			count := gf.unsafePeekPartitionLength(topic, part)
			max := offset + uint64(batchSize)

			if max > count {
				max = count
			}

			messages := gf.messages[topic][part][offset:max]

			gf.offsets[topic][group][part] = max

			if len(messages) > 0 {
				handle(messages)
			}
		}

		gf.mu.Unlock()

		time.Sleep(time.Millisecond * 1000)

		fmt.Println("RETRY")
		goto RETRY
	}()

	// unsubscribe callback
	return nil, func() {
		cancel()
	}
}

func (gf GafkaEmitter) unsafePeekOffsetForConsumerGroup(topic, group string, part int) uint64 {
	return gf.offsets[topic][group][part]
}

func (gf GafkaEmitter) unsafePeekPartitionLength(topic string, partition int) uint64 {
	return uint64(len(gf.messages[topic][partition]))
}

func (gf *GafkaEmitter) unsafeCreateTopicPartitions(topic string, partitions int) {
	gf.messages[topic] = make(map[int][]string, partitions)

	for i := 1; i <= partitions; i++ {
		gf.messages[topic][i] = []string{}
	}
}
