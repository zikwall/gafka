package lib

// использовать эти методы запределами строго воспрещается!

func (gf GafkaEmitter) UNSAFE_PeekTopicMessagesByOffset(topic string, partition int, a, b uint64) []string {
	return gf.messages[topic][partition][a:b]
}

func (gf *GafkaEmitter) UNSAFE_FlushConsumerPartitions(topic, group string) {
	gf.partitionListeners[topic][group] = map[int][]int{}
}

func (gf *GafkaEmitter) UNSAFE_TakePartition(topic, group string, part int) {
	delete(gf.freePartitions[topic][group], part)
}

func (gf *GafkaEmitter) UNSAFE_LinkConsumerToPartiton(topic, group string, consumer, part int) {
	gf.partitionListeners[topic][group][consumer] = append(gf.partitionListeners[topic][group][consumer], part)
}

func (gf *GafkaEmitter) UNSAFE_CreateFreePartitions(topic, group string) {
	gf.freePartitions[topic][group] = map[int]int{}

	for i := 1; i <= gf.topics[topic]; i++ {
		gf.freePartitions[topic][group][i] = 1
	}
}

func (gf *GafkaEmitter) UNSAFE_CommitOffset(topic, group string, part int, offset uint64) {
	gf.offsets[topic][group][part] = offset
}

func (gf GafkaEmitter) UNSAFE_PeekOffsetForConsumerGroup(topic, group string, part int) uint64 {
	return gf.offsets[topic][group][part]
}

func (gf GafkaEmitter) UNSAFE_PeekPartitionLength(topic string, partition int) uint64 {
	return uint64(len(gf.messages[topic][partition]))
}

func (gf *GafkaEmitter) UNSAFE_CreateTopic(topic Topic) {
	// не стоит сюды смотреть...
	gf.observers[topic.Name] = make(chan observer)
	gf.topics[topic.Name] = topic.Partitions
	gf.UNSAFE_CreateTopicPartitions(topic.Name, topic.Partitions)
	gf.consumers[topic.Name] = map[string]map[int]chan ReceiveMessage{}
	gf.offsets[topic.Name] = map[string]map[int]uint64{}
	gf.messagePools[topic.Name] = make(chan string)
	gf.partitionListeners[topic.Name] = map[string]map[int][]int{}
	gf.freePartitions[topic.Name] = map[string]map[int]int{}
}

func (gf *GafkaEmitter) UNSAFE_CreateTopicPartitions(topic string, partitions int) {
	gf.messages[topic] = make(map[int][]string, partitions)

	for i := 1; i <= partitions; i++ {
		gf.messages[topic][i] = []string{}
	}
}
