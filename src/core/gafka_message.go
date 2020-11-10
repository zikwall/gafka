package core

// добавляем сообщение в конкретный РАЗДЕЛ целевой ТЕМЫ
// заменить эту херь интефейсом
func (gf *GafkaEmitter) addMessage(topic string, part int, message string) {
	gf.mu.Lock()
	gf.messages[topic][part] = append(gf.messages[topic][part], message)
	gf.mu.Unlock()
}
