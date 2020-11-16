package core

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

func (gf *GafkaEmitter) Subscribe(conf SubscribeConf) (error, func()) {
	ctx, cancel := context.WithCancel(gf.context)

	gf.mu.RLock()

	if capacity, ok := gf.topics[conf.Topic]; !ok || capacity == 0 {
		gf.mu.RUnlock()
		return errors.New("Кажется тема, куда вы хотите слушать не существует! Перепроверь, але!"), nil
	}

	gf.mu.RUnlock()

	// Так, тут нужно более уникальное значение
	uniqId := rand.Intn(1000000000-1) + 1
	// надо подумать над буфером, в этот канал отправляются все считанные данные
	channel := make(chan ReceiveMessage, 10)

	gf.changeNotifier[conf.Topic] <- consumerChangeNotifier{
		topic:     conf.Topic,
		group:     conf.Group,
		id:        uniqId,
		direction: consumerJoin,
	}

	// слушаем лучше так, да
	go func() {
		defer logln("Слушатель также отменился")

		for {
			select {
			case <-ctx.Done():
				return
			case messages := <-channel:
				conf.Handler(messages)
			}
		}
	}()

	go func() {
		defer func() {
			gf.changeNotifier[conf.Topic] <- consumerChangeNotifier{
				topic:     conf.Topic,
				group:     conf.Group,
				id:        uniqId,
				direction: consumerLeft,
			}
		}()

		ticker := time.NewTicker(gf.config.ReclaimInterval)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				logln(conf.Topic, conf.Group, uniqId, "->", "Пробуем запросить данные...")

				gf.mu.RLock()
				partitions := gf.consumers[conf.Topic][conf.Group][uniqId]
				gf.mu.RUnlock()

				// забираем данные только из тех РАЗДЕЛОВ, которые мы слушаем, КЕП, этож очевидно..
				for _, partition := range partitions {
					gf.mu.RLock()

					// вычисляем текущий офсет для того, чтобы забирать только новые данные
					currentOffset := gf.UNSAFE_PeekOffsetForConsumerGroup(conf.Topic, conf.Group, partition)
					count := gf.storage.PeekLength(conf.Topic, partition)

					gf.mu.RUnlock()

					if currentOffset == count {
						continue
					}

					newOffset := currentOffset + uint64(gf.config.BatchSize)

					if newOffset > count {
						newOffset = count
					}

					gf.mu.Lock()

					messages := gf.storage.PeekOffset(conf.Topic, partition, currentOffset, newOffset)
					gf.UNSAFE_CommitOffset(conf.Topic, conf.Group, partition, newOffset)

					gf.mu.Unlock()

					if len(messages) > 0 {
						channel <- ReceiveMessage{
							Topic:     conf.Topic,
							Partition: partition,
							Messages:  messages,
						}
					}
				}
			}
		}
	}()

	// unsubscribe callback
	return nil, func() {
		cancel()
	}
}
