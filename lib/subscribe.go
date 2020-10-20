package lib

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

func (gf *GafkaEmitter) Subscribe(c context.Context, topic, group string, handle func(message ReceiveMessage)) (error, func()) {
	ctx, cancel := context.WithCancel(c)

	gf.mu.RLock()

	if capacity, ok := gf.topics[topic]; !ok || capacity == 0 {
		gf.mu.RUnlock()
		return errors.New("Кажется тема, куда вы хотите слушать не существует! Перепроверь, але!"), nil
	}

	gf.mu.RUnlock()

	// Так, тут нужно более уникальное значение
	uniqId := rand.Intn(1000000000-1) + 1
	// надо подумать над буфером, в этот канал отправляются все считанные данные
	channel := make(chan ReceiveMessage, 10)

	gf.observers[topic] <- observer{
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
		defer func() {
			gf.observers[topic] <- observer{
				topic: topic,
				group: group,
				id:    uniqId,
				in:    OutConsumer,
			}
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
				for _, partition := range gf.consumers[topic][group][uniqId] {
					// вычисляем текущий офсет для того, чтобы забирать только новые данные
					currentOffset := gf.UNSAFE_PeekOffsetForConsumerGroup(topic, group, partition)
					count := gf.UNSAFE_PeekPartitionLength(topic, partition)

					if currentOffset == count {
						continue
					}

					newOffset := currentOffset + uint64(gf.config.BatchSize)

					if newOffset > count {
						newOffset = count
					}

					messages := gf.UNSAFE_PeekTopicMessagesByOffset(topic, partition, currentOffset, newOffset)
					gf.UNSAFE_CommitOffset(topic, group, partition, newOffset)

					if len(messages) > 0 {
						channel <- ReceiveMessage{
							Topic:     topic,
							Partition: partition,
							Messages:  messages,
						}
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
