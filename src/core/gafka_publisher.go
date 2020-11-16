package core

import "errors"

func (gf *GafkaEmitter) Publish(topic string, message string) error {
	gf.mu.RLock()
	if gf.closed {
		gf.mu.RUnlock()
		return errors.New("Эй! Брокер уже не работает, вывеска висит не видишь чтоле?!")
	}

	if capacity, ok := gf.topics[topic]; !ok || capacity == 0 {
		gf.mu.RUnlock()
		return errors.New("Кажется тема, куда вы хотите опубликовать сообщение не существует! Перепроверь, але!")
	}

	gf.mu.RUnlock()

	gf.messagePools[topic] <- message
	return nil
}
