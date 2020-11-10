package core

type GafkaStorage interface {
	InitTopic(topic string, partition int)
	AddMessage(topic string, partition int, message string)
	PeekMessagesByOffset(topic string, partition int, a, b uint64) []string
	PeekPartitionLength(topic string, partition int) uint64
}
