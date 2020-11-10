package core

import "sync"

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

func (self *InMemoryStorage) PeekPartitionLength(topic string, partition int) uint64 {
	self.mu.RLock()
	defer self.mu.RUnlock()

	return uint64(len(self.messages[topic][partition]))
}

func (self *InMemoryStorage) PeekMessagesByOffset(topic string, partition int, a, b uint64) []string {
	self.mu.RLock()
	defer self.mu.RUnlock()

	return self.messages[topic][partition][a:b]
}

func (self *InMemoryStorage) InitTopic(topic string, part int) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.messages[topic] = make(map[int][]string, part)

	for i := 1; i <= part; i++ {
		self.messages[topic][i] = []string{}
	}
}

func (self *InMemoryStorage) AddMessage(topic string, part int, message string) {
	self.mu.Lock()
	self.messages[topic][part] = append(self.messages[topic][part], message)
	self.mu.Unlock()
}
