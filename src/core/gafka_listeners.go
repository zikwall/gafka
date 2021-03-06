package core

import "context"

// назнача слушаетелей по ТЕМАМ и их РАЗДЕЛАМ
// нужно будет добавить возможность динамического формирования ТЕМ
// gafka.AddTopic("topic_name_here", 10)
// deprecated
func (gf *GafkaEmitter) initBootstrappedTopicListeners() {
	gf.mu.RLock()
	topicsSnapshot := gf.topics
	gf.mu.RUnlock()

	if len(topicsSnapshot) > 0 {
		logln("Init with bootstrapped topics")
	}

	for topic, partitions := range topicsSnapshot {
		logln("Init topic ->", topic, "with partitions ->", partitions)

		for partition := 1; partition <= partitions; partition++ {
			gf.makeListener(topic, partition)
		}
	}
}

// Распаралеливаем сообщения ТЕМЫ по разным РАЗДЕЛАМ, каждый раздел работает в своем потоке
// возможно есть варианты получше, надо думать
func (gf *GafkaEmitter) makeListener(topic string, partition int) {
	gf.wg.Add(1)

	go func(top string, part int) {
		ctx, cancel := context.WithCancel(gf.context)

		defer func() {
			cancel()
			gf.wg.Done()

			logln("Cancel topic listener: ", top, " partition: ", part)
		}()

		logln("Make topic listener: ", top, " partition: ", part)

		for {
			select {
			case <-ctx.Done():
				return
			case message := <-gf.messagePools[top]:
				gf.write(top, part, message)
			}
		}

	}(topic, partition)
}
