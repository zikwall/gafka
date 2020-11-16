package core

// использовать эти методы запределами строго воспрещается!

func (gf *GafkaEmitter) UNSAFE_FlushConsumerPartitions(topic, group string) {
	gf.consumers[topic][group] = map[int][]int{}
}

func (gf *GafkaEmitter) UNSAFE_TakePartition(topic, group string, part int) {
	delete(gf.freePartitions[topic][group], part)
}

func (gf *GafkaEmitter) UNSAFE_LinkConsumerToPartiton(topic, group string, consumer, part int) {
	gf.consumers[topic][group][consumer] = append(gf.consumers[topic][group][consumer], part)
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
