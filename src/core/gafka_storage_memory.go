package core

import (
	"errors"
	"fmt"
	"sync"
)

type InMemoryStorage struct {
	mu *sync.RWMutex
	// Своство хранит все сообщения
	// topic -> message storage
	messages map[string]map[int][]string
}

func NewInMemoryStorage() *InMemoryStorage {
	m := new(InMemoryStorage)
	m.mu = &sync.RWMutex{}
	m.messages = map[string]map[int][]string{}

	return m
}

func (self *InMemoryStorage) PeekLength(topic string, partition int) uint64 {
	self.mu.RLock()
	defer self.mu.RUnlock()

	return uint64(len(self.messages[topic][partition]))
}

func (self *InMemoryStorage) PeekOffset(topic string, partition int, a, b uint64) []string {
	self.mu.RLock()
	defer self.mu.RUnlock()

	return self.messages[topic][partition][a:b]
}

func (self *InMemoryStorage) InitTopic(topic string, part int) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	if _, ok := self.messages[topic]; ok {
		return errors.New(fmt.Sprintf("Topic `%s` aldready exist, not created", topic))
	}

	self.messages[topic] = make(map[int][]string, part)

	for i := 1; i <= part; i++ {
		self.messages[topic][i] = []string{}
	}

	return nil
}

func (self *InMemoryStorage) Write(topic string, part int, message string) {
	self.mu.Lock()
	self.messages[topic][part] = append(self.messages[topic][part], message)
	self.mu.Unlock()
}
