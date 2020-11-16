package core

import (
	"errors"
	"fmt"
	"sync"
)

// hasher

type (
	Hasher interface {
		Hash(string) uint32
	}
	Fnv32Hasher struct{}
)

// Fowler–Noll–Vo is a non-cryptographic hash function created by Glenn Fowler, Landon Curt Noll, and Kiem-Phong Vo.
// see in: https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
func (f Fnv32Hasher) Hash(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// storage

type (
	Shard struct {
		mu     sync.RWMutex
		topics map[string]*Partition
	}
	Partition struct {
		mu       sync.RWMutex
		messages map[int][]string
	}
	InMemorySharded struct {
		shards []*Shard
		hasher Hasher
	}
)

func (p *Partition) GetPartition(partition int) ([]string, bool) {
	p.mu.RLock()
	val, ok := p.messages[partition]
	p.mu.RUnlock()

	return val, ok
}

func (p *Partition) PushBack(partition int, message string) {
	p.mu.Lock()
	p.messages[partition] = append(p.messages[partition], message)
	p.mu.Unlock()
}

// Example create
//  ...
//	&{{{0 0} 0 0 0 0} map[]}
//	&{{{0 0} 0 0 0 0} map[]}
//	&{{{0 0} 0 0 0 0} map[]}
//	&{{{0 0} 0 0 0 0} map[test:{{{0 0} 0 0 0 0} map[1:[message_1] 2:[message_1 message_2 message_3] 3:[message_1] 4:[message_1 message_2]]}]}
//	&{{{0 0} 0 0 0 0} map[another_topic:{{{0 0} 0 0 0 0} map[1:[] 2:[] 3:[message_1 message_2 message_3] 4:[] 5:[] 6:[]]}]}
//	&{{{0 0} 0 0 0 0} map[]}
//	&{{{0 0} 0 0 0 0} map[]}
//	&{{{0 0} 0 0 0 0} map[]}
//  ...
const SHARD_COUNT int = 32

func NewInMemoryStorageSharded(hasher Hasher) *InMemorySharded {
	memory := InMemorySharded{
		shards: make([]*Shard, SHARD_COUNT),
		hasher: hasher,
	}

	for i := 0; i < SHARD_COUNT; i++ {
		memory.shards[i] = &Shard{
			mu:     sync.RWMutex{},
			topics: map[string]*Partition{},
		}
	}

	return &memory
}

func (m *InMemorySharded) GetTopic(topic string) (*Partition, bool) {
	shard := m.GetShard(topic)
	shard.mu.RLock()
	val, ok := shard.topics[topic]
	shard.mu.RUnlock()

	return val, ok
}

func (m *InMemorySharded) HasTopic(topic string) bool {
	_, exist := m.GetTopic(topic)
	return exist
}

func (m *InMemorySharded) GetShard(key string) *Shard {
	return m.shards[uint(m.hasher.Hash(key))%uint(SHARD_COUNT)]
}

func (m InMemorySharded) CreateShard(key string, partitions int) {
	shard := m.GetShard(key)
	shard.mu.Lock()
	shard.topics[key] = &Partition{
		mu:       sync.RWMutex{},
		messages: map[int][]string{},
	}

	for i := 1; i <= partitions; i++ {
		shard.topics[key].messages[i] = make([]string, 0, 10)
	}

	shard.mu.Unlock()
}

// Gafka storage interface compatibility

func (self *InMemorySharded) PeekLength(topic string, partition int) uint64 {
	t, _ := self.GetTopic(topic)
	p, _ := t.GetPartition(partition)

	return uint64(len(p))
}

func (self *InMemorySharded) NewTopic(topic string, part int) error {
	if ok := self.HasTopic(topic); ok {
		return errors.New(fmt.Sprintf("Topic `%s` aldready exist, not created", topic))
	}

	self.CreateShard(topic, part)

	return nil
}

func (self *InMemorySharded) PeekOffset(topic string, partition int, a, b uint64) []string {
	t, _ := self.GetTopic(topic)
	p, _ := t.GetPartition(partition)

	return p[a:b]
}

func (self *InMemorySharded) Write(topic string, partition int, message string) {
	t, _ := self.GetTopic(topic)
	t.PushBack(partition, message)
}
