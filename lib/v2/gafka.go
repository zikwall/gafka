package v2

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
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
		consumers map[string]map[string]map[int]chan string

		// topic:group -> offset for consumer group
		offsets map[string]map[string]uint64

		// topic:partitions
		topics map[string]int

		// topic -> message pool
		messagePools map[string]chan string

		// topic -> message storage
		messages map[string]map[int][]string
	}
)

func Gafka(ctx context.Context, topics []Topic) *GafkaEmitter {
	gf := new(GafkaEmitter)
	gf.context, gf.cancel = context.WithCancel(ctx)

	gf.consumers = map[string]map[string]map[int]chan string{}
	gf.offsets = map[string]map[string]uint64{}
	gf.topics = make(map[string]int, len(topics))
	gf.messages = map[string]map[int][]string{}
	gf.messagePools = map[string]chan string{}

	for _, topic := range topics {
		gf.topics[topic.Name] = topic.Partitions
		gf.unsafeCreateTopicPartitions(topic.Name, topic.Partitions)
		gf.consumers[topic.Name] = map[string]map[int]chan string{}
		gf.offsets[topic.Name] = map[string]uint64{}
		gf.messagePools[topic.Name] = make(chan string)
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

func (gf *GafkaEmitter) Subscribe(c context.Context, topic, group string, handle func(string)) (error, func()) {
	ctx, cancel := context.WithCancel(c)

	gf.mu.RLock()

	if v, ok := gf.consumers[topic]; !ok || v == nil {
		gf.mu.RUnlock()

		return errors.New("Topic not exist"), nil
	}

	gf.mu.RUnlock()

	// temporary unique consumer id
	uniqId := rand.Intn(1000000000-1) + 1
	channel := make(chan string, 10)

	// global one lock
	gf.mu.Lock()

	// check group already exist
	if _, ok := gf.consumers[topic][group]; !ok {
		// add new cunsumer group for topic
		gf.consumers[topic][group] = map[int]chan string{}
	}

	// check exist consumer in consumer group
	if _, ok := gf.consumers[topic][group][uniqId]; !ok {
		// add new consumer to consumer group
		gf.consumers[topic][group][uniqId] = make(chan string)
	}

	// check offsets for consumer group
	if _, ok := gf.offsets[topic][group]; !ok {
		// offsets not exist -> zero
		gf.offsets[topic][group] = 0
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

		defer close(channel)
		defer fmt.Println("Cancel consumer for topic: ", topic, " group: ", group, " id: ", uniqId)

		// todo read from storage
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-gf.messagePools[topic]:
				handle(message)
			}
		}
	}()

	// unsubscribe callback
	return nil, func() {
		cancel()
	}
}

func (gf *GafkaEmitter) unsafeCreateTopicPartitions(topic string, partitions int) {
	gf.messages[topic] = make(map[int][]string, partitions)

	for i := 1; i <= partitions; i++ {
		gf.messages[topic][i] = []string{}
	}
}
