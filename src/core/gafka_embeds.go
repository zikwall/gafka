package core

func (gf *GafkaEmitter) addMessage(topic string, part int, message string) {
	gf.storage.AddMessage(topic, part, message)
}
