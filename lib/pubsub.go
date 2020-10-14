package lib

import (
	"context"
	"math/rand"
	"sync"
)

type (
	Publisher interface {
		Publish(topic string, message string)
	}
	Subscriber interface {
		Subscribe(ctx context.Context, topic string, handler func(string)) error
	}
	PubScriber interface {
		Publisher
		Subscriber
		Close()
	}
	PubSub struct {
		mu          sync.RWMutex
		subscribers map[string]map[int]chan string
		closed      bool
	}
)

func NewPubScriber(topics []string) *PubSub {
	ps := new(PubSub)
	ps.mu = sync.RWMutex{}
	ps.subscribers = make(map[string]map[int]chan string)

	for _, topic := range topics {
		ps.subscribers[topic] = map[int]chan string{}
	}

	return ps
}

func (p *PubSub) Subscribe(c context.Context, topic string, handler func(string)) error {
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	p.mu.Lock()

	// todo return error if topic not exist, no create topic
	if subs, ok := p.subscribers[topic]; !ok || subs == nil {
		p.subscribers[topic] = map[int]chan string{}
	}

	uniqId := rand.Int()
	subscriber := make(chan string, 10)
	p.subscribers[topic][uniqId] = subscriber

	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.subscribers[topic], uniqId)
		p.mu.Unlock()
	}()

	defer close(subscriber)

	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-subscriber:
			handler(message)
		}
	}
}

func (p *PubSub) Publish(topic string, message string) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	// todo add config: TOPIC AUTO CREATE
	if p.subscribers[topic] == nil {
		p.mu.Lock()
		p.subscribers[topic] = map[int]chan string{}
		p.mu.Unlock()
	}

	p.mu.RLock()
	subscribers := p.subscribers[topic]
	p.mu.RUnlock()

	for _, subscriber := range subscribers {
		subscriber <- message
	}
}

func (p *PubSub) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.closed {
		p.closed = true
	}

	for topic, _ := range p.subscribers {
		delete(p.subscribers, topic)
	}
}
