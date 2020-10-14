package lib

import (
	"context"
	"sync"
	"time"
)

type (
	Message struct {
		topic   string
		message string
	}
	PubSub struct {
		mu      sync.RWMutex
		closed  bool
		context context.Context

		consumers   map[string]map[string]map[int]chan string
		offsets     map[string]map[string]uint64
		messagePool chan Message
		messages    map[string]map[int][]string
		topics      map[string]int
	}
	Topic struct {
		Name       string
		Partitions int
	}
)

func Gafka(ctx context.Context, topics []Topic) *PubSub {
	ps := new(PubSub)
	ps.mu = sync.RWMutex{}
	ps.context = ctx
	ps.consumers = make(map[string]map[string]map[int]chan string)
	ps.messagePool = make(chan Message)
	ps.messages = map[string]map[int][]string{}
	ps.offsets = map[string]map[string]uint64{}
	ps.topics = make(map[string]int, len(topics))

	for _, topic := range topics {
		ps.topics[topic.Name] = topic.Partitions
		ps.consumers[topic.Name] = map[string]map[int]chan string{}
		ps.offsets[topic.Name] = map[string]uint64{}

		ps.unsafeCreatePartitions(topic.Name, topic.Partitions)
	}

	go ps.process(topics)

	return ps
}

func (ps *PubSub) unsafeCreatePartitions(topic string, partitions int) {
	ps.messages[topic] = make(map[int][]string, partitions)

	// todo tmp four parts
	for i := 0; i < partitions; i++ {
		ps.messages[topic][i] = []string{}
	}
}

func (p *PubSub) process(topics []Topic) {
	for _, topic := range topics {
		// listen by partitions
		for partition := 0; partition < topic.Partitions; partition++ {

			// make threads
			go func(part int) {
				ctx, cancel := context.WithCancel(p.context)
				defer cancel()

				for {
					select {
					case <-ctx.Done():
						return
					case message := <-p.messagePool:
						p.mu.Lock()
						p.messages[message.topic][part] = append(p.messages[message.topic][part], message.message)
						p.mu.Unlock()
					}
				}
			}(partition)
		}
	}
}

func (p *PubSub) Subscribe(c context.Context, topic string, group string, handler func([]string)) error {
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	p.mu.Lock()

	// todo return error if topic not exist, no create topic
	if subs, ok := p.consumers[topic]; !ok || subs == nil {
		p.consumers[topic] = map[string]map[int]chan string{}
	}

	if _, ok := p.consumers[topic][group]; !ok {
		p.consumers[topic][group] = map[int]chan string{}
	}

	if _, ok := p.offsets[topic][group]; !ok {
		p.offsets[topic][group] = 0
	}

	// todo balance between partitions and consumer groups
	// read random partition
	partition := 3

	// todo remove
	subscriber := make(chan string, 10)
	p.consumers[topic][group][partition] = subscriber

	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.consumers[topic][group], partition)
		p.mu.Unlock()
	}()

	defer close(subscriber)

	// batch size
	batch := 20

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		p.mu.RLock()

		//fmt.Println(p.messages[topic])

		// calculate offset for consumer group
		i := p.offsets[topic][group]
		l := uint64(len(p.messages[topic][partition]))

		p.mu.RUnlock()

		j := i + uint64(batch)

		if j > l {
			j = l
		}

		if j-i > 0 {
			p.mu.Lock()
			p.offsets[topic][group] = j
			p.mu.Unlock()

			p.mu.RLock()
			// read batched messages
			messages := p.messages[topic][partition][i:j]
			p.mu.RUnlock()

			// send to cunsumer
			if len(messages) > 0 {
				handler(messages)
			}
		}

		// todo
		time.Sleep(time.Millisecond * 5)
	}
}

func (p *PubSub) Publish(topic string, message string) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	// todo add config: TOPIC AUTO CREATE with partitions default
	if v, ok := p.consumers[topic]; !ok || v == nil {
		p.mu.Lock()
		p.unsafeCreatePartitions(topic, 1)
		p.mu.Unlock()
	}

	p.messagePool <- Message{
		topic:   topic,
		message: message,
	}
}

func (p *PubSub) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.closed {
		p.closed = true
	}

	// todo
	for topic, _ := range p.consumers {
		delete(p.consumers, topic)
	}
}
