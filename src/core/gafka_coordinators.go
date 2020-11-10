package core

import "math"

// назначается слушатель по ТЕМАМ и РАЗДЕЛАМ для возможности пере-/балансировки ПОДПИСЧИКОВ
func (gf *GafkaEmitter) initConsumerGroupCoordinators() {
	for topic, partitions := range gf.topics {
		gf.createConsumerGroupCoordinatorListener(topic, partitions)
	}
}

// балансировщик ТЕМ, РАЗДЕЛОВ и шрупп ПОТРЕБИТЕЛЕЙ
func (gf *GafkaEmitter) createConsumerGroupCoordinatorListener(topic string, partitions int) {
	gf.wg.Add(1)

	go func(t string, parts int) {
		defer gf.wg.Done()

		// тут надо подумать ибо есть некоторое дублирование
		// существует еще глобальное состояние ПОДПИСЧИКОВ
		// потом надо будет отредактировать
		consumerInGroup := map[string]map[int]int{}

		for {
			select {
			case <-gf.context.Done():
				return
			case change := <-gf.changeNotifier[t]:
				logln("Обнаружены изменения в балансе подписчиков, начинаю процес РЕБАЛАНСИРОВКИ подписчиков")

				topic := t
				group := change.group
				uniqId := change.id
				partition := parts

				if _, ok := consumerInGroup[group]; !ok {
					consumerInGroup[group] = map[int]int{}
				}

				// Пока только один глобальный лок, надо подумать в целесообразности множества маленьких
				gf.mu.Lock()

				if change.direction == consumerJoin {
					logln("У нас новый подписчик для ТЕМЫ", topic, "это чудо входит в группу", group, "его уникальный айди", uniqId)

					consumerInGroup[group][uniqId] = 1

					if _, ok := gf.offsets[topic][group]; !ok {
						gf.offsets[topic][group] = map[int]uint64{}
					}

					if _, ok := gf.consumers[topic][group]; !ok {
						gf.consumers[topic][group] = map[int][]int{}
					}

					if _, ok := gf.freePartitions[topic][group]; !ok {
						gf.UNSAFE_CreateFreePartitions(topic, group)
					}

				} else {
					delete(consumerInGroup[group], uniqId)

					logln("Подписчик ушел с ТЕМЫ", topic, "и группы", group, "его айди был", uniqId)

					// todo эта хрень вообще не нужна, вообще
					// удаляем все прослушиваемые РАЗДЕЛЫ целевой ТЕМЫ, где был замечен этот хитрый слушатель
					for _, part := range gf.consumers[topic][group][uniqId] {
						gf.freePartitions[topic][group][part] = 1
						logln("Метим РАЗДЕЛ", part, "для группы", group, "свободным")
						delete(gf.consumers[topic][group], uniqId)
						logln("Удаляем слушаетля", uniqId, "из группы", group, "и раздела", part)
					}
				}

				partOneConsumer := float64(partition) / float64(len(consumerInGroup[group]))

				// todo нужен более продвинутый алгоритм для равномерно-плавного распределения
				// потребителей по разделам топика, что они слушают
				// 4 + 0.5 => 5
				// 3.9 + 0.5 => 4
				need := int(math.Round(partOneConsumer + 0.49))

				gf.UNSAFE_FlushConsumerPartitions(topic, group)
				gf.UNSAFE_CreateFreePartitions(topic, group)

				for consumer := range consumerInGroup[group] {
					for part := range gf.freePartitions[topic][group] {
						if len(gf.consumers[topic][group][consumer]) >= need {
							break
						}

						gf.UNSAFE_LinkConsumerToPartiton(topic, group, consumer, part)
						gf.UNSAFE_TakePartition(topic, group, part)
					}
				}

				if change.direction == consumerJoin {
					logln("Подписчик", uniqId, "слушает селудющие РАЗДЕЛЫ", gf.consumers[topic][group][uniqId])
				}

				logln("Новый порядок таков", gf.consumers[topic][group])

				gf.mu.Unlock()
			}
		}
	}(topic, partitions)
}
