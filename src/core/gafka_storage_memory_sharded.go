package core

import (
	"errors"
	"fmt"
	"sync"
)

type (
	Shard struct {
		mu     sync.RWMutex
		topics map[string]Partition
	}
	Partition struct {
		mu       sync.RWMutex
		messages map[int][]string
	}
	InMemoryShardedPartition []*Partition
	InMemorySharded          []*Shard
)

func (p Partition) GetPartition(partition int) ([]string, bool) {
	p.mu.RLock()
	val, ok := p.messages[partition]
	p.mu.RUnlock()

	return val, ok
}

func (p Partition) PushBack(partition int, message string) {
	p.mu.RLock()
	p.messages[partition] = append(p.messages[partition], message)
	p.mu.RUnlock()
}

const SHARD_COUNT int = 32

func NewInMemoryStorageSharded() InMemorySharded {
	m := make(InMemorySharded, SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		m[i] = &Shard{
			mu:     sync.RWMutex{},
			topics: map[string]Partition{},
		}
	}
	return m
}

func (self *InMemorySharded) PeekPartitionLength(topic string, partition int) uint64 {
	t, _ := self.GetTopic(topic)
	p, _ := t.GetPartition(partition)

	return uint64(len(p))
}

func (self *InMemorySharded) InitTopic(topic string, part int) error {
	if ok := self.HasTopic(topic); ok {
		return errors.New(fmt.Sprintf("Topic `%s` aldready exist, not created", topic))
	}

	self.CreateShard(topic, part)

	return nil
}

func (self *InMemorySharded) PeekMessagesByOffset(topic string, partition int, a, b uint64) []string {
	t, _ := self.GetTopic(topic)
	p, _ := t.GetPartition(partition)

	return p[a:b]
}

func (self *InMemorySharded) AddMessage(topic string, partition int, message string) {
	t, _ := self.GetTopic(topic)
	t.PushBack(partition, message)
}

func (m InMemorySharded) GetTopic(topic string) (Partition, bool) {
	shard := m.GetShard(topic)
	shard.mu.RLock()
	val, ok := shard.topics[topic]
	shard.mu.RUnlock()

	return val, ok
}

func (m InMemorySharded) HasTopic(topic string) bool {
	_, exist := m.GetTopic(topic)
	return exist
}

func (m InMemorySharded) GetShard(key string) *Shard {
	return m[uint(fnv32(key))%uint(SHARD_COUNT)]
}

func (m InMemorySharded) CreateShard(key string, partitions int) {
	shard := m.GetShard(key)
	shard.mu.Lock()
	shard.topics[key] = Partition{
		mu:       sync.RWMutex{},
		messages: map[int][]string{},
	}

	for i := 1; i <= partitions; i++ {
		shard.topics[key].messages[i] = []string{}
	}

	shard.mu.Unlock()
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
