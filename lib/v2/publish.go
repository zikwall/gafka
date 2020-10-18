package v2

import "errors"

func (gf *GafkaEmitter) Publish(topic string, message string) error {
	gf.mu.RLock()
	if gf.closed {
		gf.mu.RUnlock()
		return errors.New("Эй! Брокер уже не работает, вывеска висит не видишь чтоле?!")
	}

	if v, ok := gf.consumers[topic]; !ok || v == nil {
		gf.mu.RUnlock()
		return errors.New("Кажется тема, куда вы хотите опубликовать сообщение не существует! Перепроверь, але!")
	}

	gf.mu.RUnlock()

	// Собственно отправляем сообщение в канал конкретной ТЕМЫ
	// дальше там он уже распарелилится по РАЗДЕЛАМ этой ТЕМЫ
	gf.messagePools[topic] <- message

	// красава
	return nil
}
