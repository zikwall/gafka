package core

func (gf *GafkaEmitter) write(topic string, part int, message string) {
	gf.storage.Write(topic, part, message)
}
