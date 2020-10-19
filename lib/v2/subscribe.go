package v2

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

func (gf *GafkaEmitter) Subscribe(c context.Context, topic, group string, handle func([]string)) (error, func()) {
	ctx, cancel := context.WithCancel(c)

	gf.mu.RLock()

	if v, ok := gf.consumers[topic]; !ok || v == nil {
		gf.mu.RUnlock()
		return errors.New("Кажется тема, куда вы хотите слушать не существует! Перепроверь, але!"), nil
	}

	gf.mu.RUnlock()

	// Так, тут нужно более уникальное значение
	uniqId := rand.Intn(1000000000-1) + 1
	// надо подумать над буфером, в этот канал отправляются все считанные данные
	channel := make(chan []string, 10)

	// Пока только один глобальный лок, надо подумать в целесообразности множества маленьких
	gf.mu.Lock()

	// check group already exist
	if _, ok := gf.consumers[topic][group]; !ok {
		// add new cunsumer group for topic
		gf.consumers[topic][group] = map[int]chan []string{}
	}

	// check exist consumer in consumer group
	if _, ok := gf.consumers[topic][group][uniqId]; !ok {
		// add new consumer to consumer group
		gf.consumers[topic][group][uniqId] = make(chan []string)
	}

	// check offsets for consumer group
	if _, ok := gf.offsets[topic][group]; !ok {
		// offsets not exist -> zero
		gf.offsets[topic][group] = map[int]uint64{}
	}

	// check partition listeners
	if _, ok := gf.partitionListeners[topic][group]; !ok {
		// create new listeners
		gf.partitionListeners[topic][group] = map[int][]int{}
	}

	gf.consumers[topic][group][uniqId] = channel

	gf.mu.Unlock()

	gf.changeConsumers[topic] <- direction{
		topic: topic,
		group: group,
		id:    uniqId,
		in:    InConsumer,
	}

	// слушаем лучше так, да
	go func() {
		defer logln("Слушатель также отменился")

		for {
			select {
			case <-ctx.Done():
				return
			case messages := <-channel:
				handle(messages)
			}
		}
	}()

	go func() {
		logln("У нас новый подписчик для ТЕМЫ", topic, "это чудо входит в группу", group, "его уникальный айди", uniqId)

		defer func() {
			gf.mu.Lock()
			delete(gf.consumers[topic][group], uniqId)

			logln("Подписчик ушел с ТЕМЫ", topic, "и группы", group, "его айди был", uniqId)

			// удаляем все прослушиваемые РАЗДЕЛЫ целевой ТЕМЫ, где был замечен этот хитрый слушатель
			for _, part := range gf.partitionListeners[topic][group][uniqId] {
				// добавляем РАЗДЕЛ  в раздел безхозных, которые необходимо приютить
				gf.freePartitions[topic][group][part] = 1
				logln("Метим РАЗДЕЛ", part, "для группы", group, "свободным")
				// удаляем из слушателей раздела текущего подписчика
				delete(gf.partitionListeners[topic][group], uniqId)
				logln("Удаляем слушаетля", uniqId, "из группы", group, "и раздела", part)

				// удаляем слушателя и закрываем канал
				delete(gf.consumers[topic][group], uniqId)

				gf.changeConsumers[topic] <- direction{
					topic: topic,
					group: group,
					id:    uniqId,
					in:    OutConsumer,
				}
			}

			gf.mu.Unlock()
		}()

		ticker := time.NewTicker(gf.config.ReclaimInterval)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				logln("Пробуем запросить данные...")

				gf.mu.RLock()

				// забираем данные только из тех РАЗДЕЛОВ, которые мы слушаем, КЕП, этож очевидно..
				for _, part := range gf.partitionListeners[topic][group][uniqId] {
					// вычисляем текущий офсет для того, чтобы забирать только новые данные
					currentOffset := gf.UNSAFE_PeekOffsetForConsumerGroup(topic, group, part)
					count := gf.UNSAFE_PeekPartitionLength(topic, part)

					if currentOffset == count {
						continue
					}

					newOffset := currentOffset + uint64(gf.config.BatchSize)

					if newOffset > count {
						newOffset = count
					}

					messages := gf.messages[topic][part][currentOffset:newOffset]

					gf.UNSAFE_CommitOffset(topic, group, part, newOffset)

					if len(messages) > 0 {
						logln(group, "-", uniqId, "Читаю из", topic, part)

						channel <- messages
					}
				}

				gf.mu.RUnlock()
			}
		}
	}()

	// unsubscribe callback
	return nil, func() {
		cancel()
	}
}
