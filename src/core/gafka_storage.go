package core

type GafkaStorage interface {
	InitTopic(topic string, partition int) error
	Write(topic string, partition int, message string)
	PeekOffset(topic string, partition int, a, b uint64) []string
	PeekLength(topic string, partition int) uint64
}
